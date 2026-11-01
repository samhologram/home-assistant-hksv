"""Shared Home Assistant stream output for HKSV fragment inspection.

This adapter only transports fragments. Callers must verify codec and recording
parameters before advertising HKSV or delivering any data.
"""

import asyncio
from collections import deque
from collections.abc import AsyncIterator
from dataclasses import dataclass
from itertools import pairwise
from typing import TYPE_CHECKING, cast, override

from homeassistant.components.camera import get_camera_from_entity_id
from homeassistant.components.stream.core import (  # pylint: disable=home-assistant-component-root-import
    PROVIDERS,
    IdleTimer,
    Segment,
    StreamOutput,
    StreamSettings,
)

if TYPE_CHECKING:
    from av.container import InputContainer

    from homeassistant.components.camera import DynamicStreamSettings
    from homeassistant.components.stream import Stream
from homeassistant.core import HomeAssistant, callback

_PROVIDER = "homekit_secure_video"
_MAX_SEGMENTS = 8
_MAX_SNAPSHOT_BYTES = 16 * 1024 * 1024


@PROVIDERS.register(_PROVIDER)
class HKSVStreamOutput(StreamOutput):
    """Keep a bounded list of references to the shared stream's segments."""

    requires_aac_audio = False

    def __init__(
        self,
        hass: HomeAssistant,
        idle_timer: IdleTimer,
        stream_settings: StreamSettings,
        dynamic_stream_settings: DynamicStreamSettings,
    ) -> None:
        """Initialize the bounded provider."""
        super().__init__(
            hass,
            idle_timer,
            stream_settings,
            dynamic_stream_settings,
            deque_maxlen=_MAX_SEGMENTS,
        )

    @callback
    def _trim_bytes(self) -> None:
        """Bound retained segment references as their parts arrive."""
        while (
            self._segments
            and sum(s.data_size_with_init for s in self._segments) > _MAX_SNAPSHOT_BYTES
        ):
            self._segments.popleft()

    @callback
    @override
    def _async_put(self, segment: Segment) -> None:
        super()._async_put(segment)
        self._trim_bytes()

    @callback
    @override
    def part_put(self) -> None:
        """Trim references as segment parts arrive."""
        self._trim_bytes()
        super().part_put()

    @property
    @override
    def name(self) -> str:
        """Return the provider name."""
        return _PROVIDER

    @callback
    @override
    def cleanup(self) -> None:
        """Release references and wake waiting consumers."""
        super().cleanup()
        self._segments.clear()


@dataclass(frozen=True, slots=True)
class SharedFragment:
    """One completed segment from a shared stream generation."""

    stream_id: int
    sequence: int
    initialization: bytes
    data: bytes
    duration: float


class SharedCameraStream:
    """Own a dedicated output on a Camera's existing shared stream."""

    def __init__(self, stream: Stream, output: HKSVStreamOutput) -> None:
        """Store the stream and owned output."""
        self._stream = stream
        self._output = output
        self._closed = False

    @classmethod
    async def open(
        cls, hass: HomeAssistant, entity_id: str, *, with_audio: bool = False
    ) -> SharedCameraStream:
        """Attach an HKSV output without opening another camera connection."""
        camera = get_camera_from_entity_id(hass, entity_id)
        stream = await camera.async_create_stream()
        if stream is None:
            raise ValueError("Camera has no stream")
        if _PROVIDER in stream.outputs():
            raise RuntimeError("Camera already has an HKSV stream owner")
        had_outputs = bool(stream.outputs())
        output = stream.add_provider(_PROVIDER, timeout=300)
        assert isinstance(output, HKSVStreamOutput)
        output.requires_aac_audio = with_audio
        try:
            if had_outputs and with_audio:
                stream.update_source(stream.source)
            await stream.start()
        except BaseException:
            await stream.remove_provider(output)
            raise
        return cls(stream, output)

    def snapshot(
        self, cursor: tuple[int, int] | None = None
    ) -> tuple[SharedFragment, ...]:
        """Copy only complete new keyframe segments after checking their size."""
        self._output.idle_timer.awake()
        segments = tuple(self._output.get_segments())
        if not segments:
            return ()
        current_id = segments[-1].stream_id
        eligible = tuple(
            s
            for s in segments
            if s.stream_id == current_id
            and s.complete
            and s.parts
            and s.parts[0].has_keyframe
            and (cursor is None or s.stream_id != cursor[0] or s.sequence > cursor[1])
        )
        if (
            sum(s.data_size for s in eligible)
            + (len(eligible[0].init) if eligible else 0)
            > _MAX_SNAPSHOT_BYTES
        ):
            raise ValueError("HKSV prebuffer exceeds byte limit")
        return tuple(
            SharedFragment(s.stream_id, s.sequence, s.init, s.get_data(), s.duration)
            for s in eligible
        )

    async def fragments(self) -> AsyncIterator[SharedFragment]:
        """Yield each complete segment once, including after stream restarts."""
        cursor: tuple[int, int] | None = None
        while not self._closed:
            for fragment in self.snapshot(cursor):
                if (
                    cursor is not None
                    and fragment.stream_id == cursor[0]
                    and fragment.sequence != cursor[1] + 1
                ):
                    raise ValueError("HKSV consumer missed stream segments")
                cursor = (fragment.stream_id, fragment.sequence)
                yield fragment
            # Parts complete a segment after it was initially published.
            try:
                async with asyncio.timeout(1):
                    await self._output.part_recv()
            except TimeoutError:
                pass

    async def close(self) -> None:
        """Detach only this provider, preserving other shared stream outputs."""
        if self._closed:
            return
        self._closed = True
        if self._stream.outputs().get(_PROVIDER) is self._output:
            await self._stream.remove_provider(self._output)


@dataclass(frozen=True, slots=True)
class MediaObservation:
    """Measured media properties; these do not guarantee future behavior."""

    video_codec: str
    h264_profile_idc: int
    h264_level_idc: int
    width: int
    height: int
    audio_codec: str | None
    audio_profile: str | None
    audio_rate: int | None
    audio_channels: int | None
    fps: float
    max_keyframe_interval: float
    peak_fps: int
    peak_video_bitrate: int
    average_video_bitrate: float
    observation_seconds: float
    audio_peak_bitrate: int = 0
    audio_object_type: int = 0
    audio_max_packet_bits: int = 0
    audio_continuous: bool = False


def inspect_recording(
    initialization: bytes, fragments: tuple[bytes, ...]
) -> MediaObservation:
    """Inspect a bounded fMP4 sample using PyAV; call from an executor.

    Results only describe the supplied data and must not be used as a guarantee
    of future bitrate, keyframe cadence, timestamp continuity or codec stability.
    """
    from io import BytesIO  # noqa: PLC0415

    import av  # noqa: PLC0415

    size = len(initialization) + sum(map(len, fragments))
    if not fragments or size > _MAX_SNAPSHOT_BYTES:
        raise ValueError("Empty or oversized media sample")
    with av.open(
        BytesIO(initialization + b"".join(fragments)), format="mp4"
    ) as raw_container:
        container = cast("InputContainer", raw_container)
        videos = container.streams.video
        audios = container.streams.audio
        if len(videos) != 1 or len(audios) > 1:
            raise ValueError("Expected one video and at most one audio track")
        video = videos[0]
        codec = video.codec_context
        extra = codec.extradata or b""
        is_h264 = codec.name == "h264" and len(extra) >= 4 and extra[0] == 1
        if not codec.width or not codec.height:
            raise ValueError("Video dimensions unavailable")
        audio = audios[0].codec_context if audios else None
        audio_extra = audio.extradata or b"" if audio else b""
        audio_object_type = audio_extra[0] >> 3 if audio_extra else 0
        first = last = previous = None
        keyframes: list[float] = []
        video_bytes = 0
        video_packets = 0
        window: deque[tuple[float, int]] = deque()
        window_bytes = peak_bytes = peak_frames = 0
        audio_window: deque[tuple[float, int]] = deque()
        audio_window_bytes = audio_peak_bytes = audio_max_packet_bytes = 0
        audio_first = audio_last = None
        audio_valid = True
        streams = (video, audios[0]) if audios else (video,)
        for packet in container.demux(*streams):
            if packet.dts is None or packet.time_base is None:
                continue
            timestamp = float(packet.dts * packet.time_base)
            if audios and packet.stream.index == audios[0].index:
                if audio_last is not None and (
                    timestamp <= audio_last or timestamp - audio_last > 0.2
                ):
                    audio_valid = False
                if audio_first is None:
                    audio_first = timestamp
                audio_last = timestamp
                audio_window.append((timestamp, packet.size))
                audio_window_bytes += packet.size
                audio_max_packet_bytes = max(audio_max_packet_bytes, packet.size)
                while audio_window and audio_window[0][0] <= timestamp - 1 + 1e-6:
                    audio_window_bytes -= audio_window.popleft()[1]
                audio_peak_bytes = max(audio_peak_bytes, audio_window_bytes)
                continue
            if previous is not None and timestamp <= previous:
                raise ValueError("Nonmonotonic video timestamps")
            previous = last = timestamp
            if first is None:
                first = timestamp
            if packet.is_keyframe:
                keyframes.append(timestamp)
            video_bytes += packet.size
            video_packets += 1
            window.append((timestamp, packet.size))
            window_bytes += packet.size
            while window and window[0][0] <= timestamp - 1 + 1e-6:
                window_bytes -= window.popleft()[1]
            peak_bytes = max(peak_bytes, window_bytes)
            peak_frames = max(peak_frames, len(window))
        if first is None or last is None or last <= first or len(keyframes) < 2:
            raise ValueError("Insufficient media to measure keyframe interval")
        duration = last - first
        return MediaObservation(
            codec.name,
            extra[1] if is_h264 else 0,
            extra[3] if is_h264 else 0,
            codec.width,
            codec.height,
            audio.name if audio else None,
            audio.profile if audio else None,
            audio.sample_rate if audio else None,
            audio.channels if audio else None,
            (video_packets - 1) / duration,
            max(b - a for a, b in pairwise(keyframes)),
            peak_frames,
            peak_bytes * 8,
            video_bytes * 8 / duration,
            duration,
            audio_peak_bytes * 8,
            audio_object_type,
            audio_max_packet_bytes * 8,
            bool(
                audio_valid
                and audio_first is not None
                and audio_last is not None
                and audio_last - audio_first >= duration - 0.25
            ),
        )
