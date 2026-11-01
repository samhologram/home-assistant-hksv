"""Tests for the dedicated HKSV shared stream output."""

# Tests exercise internal stream muxing and timing behavior.
# pylint: disable=home-assistant-component-root-import

from fractions import Fraction
from io import BytesIO
from itertools import pairwise
from pathlib import Path
import shutil
import subprocess
from threading import Event
from unittest.mock import AsyncMock, Mock, patch

import av
import pytest

from homeassistant.components.homekit.hksv_stream import (
    SharedCameraStream,
    inspect_recording,
)
from homeassistant.components.stream.core import StreamSettings
from homeassistant.components.stream.diagnostics import Diagnostics
from homeassistant.components.stream.worker import (
    AudioTimestampNormalizer,
    StreamEndedError,
    StreamMuxer,
    StreamState,
    StreamWorkerError,
    stream_worker,
)


def segment(
    stream_id: int, sequence: int, *, complete: bool = True, keyframe: bool = True
):
    """Create a minimal segment."""
    return Mock(
        stream_id=stream_id,
        sequence=sequence,
        init=b"init",
        duration=1.0,
        data_size=8,
        complete=complete,
        parts=[Mock(has_keyframe=keyframe)],
        get_data=Mock(return_value=b"fragment"),
    )


async def test_snapshot_only_current_complete_keyframe_segments() -> None:
    """Do not mix generations or hand off incomplete/non-keyframe fragments."""
    output = Mock()
    output.get_segments.return_value = [
        segment(1, 1),
        segment(2, 2),
        segment(2, 3, complete=False),
        segment(2, 4, keyframe=False),
    ]
    shared = SharedCameraStream(Mock(), output)
    fragments = shared.snapshot()
    assert [(f.stream_id, f.sequence, f.data) for f in fragments] == [
        (2, 2, b"fragment")
    ]


async def test_close_only_own_output() -> None:
    """Closing the adapter detaches its own output once."""
    output = Mock()
    stream = Mock()
    stream.outputs.return_value = {"homekit_secure_video": output, "hls": Mock()}
    stream.remove_provider = AsyncMock()
    shared = SharedCameraStream(stream, output)
    await shared.close()
    await shared.close()
    stream.remove_provider.assert_awaited_once_with(output)


async def test_iterator_observes_generation_change() -> None:
    """A restart surfaces a new initialization generation."""
    output = Mock()
    output.get_segments.side_effect = [[segment(1, 1)], [segment(2, 1)]]
    output.part_recv = AsyncMock(return_value=True)
    shared = SharedCameraStream(Mock(), output)
    iterator = shared.fragments()
    assert (await anext(iterator)).stream_id == 1
    assert (await anext(iterator)).stream_id == 2
    await iterator.aclose()


def test_media_inspection_of_real_fragmented_mp4(tmp_path: Path) -> None:
    """Inspect real H.264/AAC media and reject a truncated sample."""
    if not (ffmpeg := shutil.which("ffmpeg")):
        pytest.skip("ffmpeg unavailable")
    path = tmp_path / "sample.mp4"
    process = subprocess.run(
        [
            ffmpeg,
            "-hide_banner",
            "-loglevel",
            "error",
            "-f",
            "lavfi",
            "-i",
            "testsrc=size=320x240:rate=15",
            "-f",
            "lavfi",
            "-i",
            "sine=frequency=1000:sample_rate=24000",
            "-t",
            "5",
            "-c:v",
            "libx264",
            "-profile:v",
            "baseline",
            "-pix_fmt",
            "yuv420p",
            "-g",
            "30",
            "-c:a",
            "aac",
            "-ac",
            "1",
            "-movflags",
            "frag_keyframe+empty_moov+default_base_moof",
            "-f",
            "mp4",
            "-y",
            str(path),
        ],
        capture_output=True,
        check=False,
    )
    if process.returncode:
        pytest.skip("ffmpeg does not support test encoders")
    media = path.read_bytes()
    observation = inspect_recording(b"", (media,))
    assert (observation.video_codec, observation.width, observation.height) == (
        "h264",
        320,
        240,
    )
    assert (
        observation.audio_codec,
        observation.audio_rate,
        observation.audio_channels,
    ) == ("aac", 24000, 1)
    assert observation.h264_profile_idc == 66
    assert observation.peak_fps == 15
    assert 1.5 < observation.max_keyframe_interval < 2.5
    with pytest.raises((ValueError, av.error.InvalidDataError)):  # pylint: disable=c-extension-no-member
        inspect_recording(b"", (media[:100],))


async def test_slow_consumer_rejects_missing_segment() -> None:
    """A consumer must not silently write discontinuous fMP4."""
    output = Mock()
    output.get_segments.side_effect = [[segment(1, 1)], [segment(1, 3)]]
    output.part_recv = AsyncMock(return_value=True)
    iterator = SharedCameraStream(Mock(), output).fragments()
    assert (await anext(iterator)).sequence == 1
    with pytest.raises(ValueError, match="missed"):
        await anext(iterator)


def test_oversized_segment_is_not_copied() -> None:
    """An oversized buffer fails before allocating segment copies."""
    large = segment(1, 1)
    large.data_size = 16 * 1024 * 1024
    output = Mock()
    output.get_segments.return_value = [large]
    with pytest.raises(ValueError, match="byte limit"):
        SharedCameraStream(Mock(), output).snapshot()
    large.get_data.assert_not_called()


def test_shared_worker_transcodes_alaw_only_when_requested(tmp_path: Path) -> None:
    """A real source retains audio with HKSV and retains old behavior otherwise."""
    if not (ffmpeg := shutil.which("ffmpeg")):
        pytest.skip("ffmpeg unavailable")
    path = tmp_path / "source.mkv"
    process = subprocess.run(
        [
            ffmpeg,
            "-hide_banner",
            "-loglevel",
            "error",
            "-f",
            "lavfi",
            "-i",
            "testsrc=size=320x240:rate=15",
            "-f",
            "lavfi",
            "-i",
            "sine=frequency=1000:sample_rate=8000",
            "-t",
            "20",
            "-c:v",
            "libx264",
            "-pix_fmt",
            "yuv420p",
            "-g",
            "30",
            "-c:a",
            "pcm_alaw",
            "-f",
            "matroska",
            "-y",
            str(path),
        ],
        capture_output=True,
        check=False,
    )
    if process.returncode:
        pytest.skip("ffmpeg does not support test encoders")

    for requested in (False, True):

        class Output:
            requires_aac_audio = requested

            def __init__(self) -> None:
                self.segments = []

            def put(self, segment):
                self.segments.append(segment)

            def part_put(self):
                pass

        output = Output()
        hass = Mock()
        hass.loop.call_soon_threadsafe.side_effect = lambda callback, *args: callback(
            *args
        )
        state = StreamState(hass, lambda output=output: {"test": output}, Diagnostics())
        settings = StreamSettings(False, 1.5, 2, 3, 2)
        with pytest.raises(StreamEndedError):
            stream_worker(str(path), {}, settings, state, Mock(), Event())
        segments = [segment for segment in output.segments if segment.complete]
        assert len(segments) >= 3
        for segment in segments:
            with av.open(
                BytesIO(segment.init + segment.get_data()), format="mp4"
            ) as container:
                assert container.streams.video[0].codec_context.name == "h264"
                assert [
                    stream.codec_context.name for stream in container.streams.audio
                ] == (["aac"] if requested else [])
                packets = [
                    packet for packet in container.demux() if packet.dts is not None
                ]
                if requested:
                    video_times = [
                        float(packet.dts * packet.time_base)
                        for packet in packets
                        if packet.stream.type == "video"
                    ]
                    audio_times = [
                        float(packet.dts * packet.time_base)
                        for packet in packets
                        if packet.stream.type == "audio"
                    ]
                    assert video_times and audio_times
                    assert abs(video_times[0] - audio_times[0]) < 0.3
                    assert abs(video_times[-1] - audio_times[-1]) < 0.3


@pytest.mark.parametrize(("requested", "expected_count"), [(False, 0), (True, 1)])
def test_mux_failure_diagnostic_is_scoped_and_redacted(
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
    requested: bool,
    expected_count: int,
) -> None:
    """Only the HKSV audio path logs packet metadata, without the source."""
    if not (ffmpeg := shutil.which("ffmpeg")):
        pytest.skip("ffmpeg unavailable")
    path = tmp_path / "private-camera-secret.mkv"
    result = subprocess.run(
        [
            ffmpeg,
            "-hide_banner",
            "-loglevel",
            "error",
            "-f",
            "lavfi",
            "-i",
            "testsrc=size=320x240:rate=15",
            "-f",
            "lavfi",
            "-i",
            "sine=frequency=1000:sample_rate=8000",
            "-t",
            "5",
            "-c:v",
            "libx264",
            "-pix_fmt",
            "yuv420p",
            "-g",
            "30",
            "-c:a",
            "pcm_alaw",
            "-f",
            "matroska",
            "-y",
            str(path),
        ],
        capture_output=True,
        check=False,
    )
    if result.returncode:
        pytest.skip("ffmpeg test encoders unavailable")
    output = Mock(requires_aac_audio=requested)
    hass = Mock()
    hass.loop.call_soon_threadsafe.side_effect = lambda callback, *args: callback(*args)
    state = StreamState(hass, lambda: {"test": output}, Diagnostics())
    settings = StreamSettings(False, 1.5, 2, 3, 2)
    original = StreamMuxer.mux_packet
    count = 0

    def fail_after_first(muxer: StreamMuxer, packet: av.Packet) -> None:
        nonlocal count
        count += 1
        if count == 2:
            raise av.FFmpegError(
                22, "invalid input", "rtsp://user:private-secret@camera/stream"
            )
        original(muxer, packet)

    with (
        patch.object(StreamMuxer, "mux_packet", fail_after_first),
        pytest.raises(StreamWorkerError),
    ):
        stream_worker(str(path), {}, settings, state, Mock(), Event())
    messages = [
        record.message
        for record in caplog.records
        if "HKSV shared mux failed" in record.message
    ]
    assert len(messages) == expected_count
    for message in messages:
        assert all(
            label in message
            for label in (
                "track=",
                "dts=",
                "pts=",
                "time_base=",
                "keyframe=",
                "video_codec=h264",
                "audio_transcode=true",
                "errno=22",
            )
        )
        assert "private" not in message and "rtsp" not in message


def test_audio_timestamps_clamp_overlap_and_preserve_gap() -> None:
    """Real camera overlap is repaired without discarding forward gaps."""
    clock = AudioTimestampNormalizer()
    actual = []
    for pts in (3130, 4154, 4170, 5194, 10000):
        frame = av.AudioFrame(format="s16", layout="mono", samples=1024)
        frame.sample_rate = 8000
        frame.time_base = Fraction(1, 8000)
        frame.pts = pts
        clock.normalize(frame)
        actual.append(frame.pts)
    assert actual == [3130, 4154, 5178, 6202, 10000]


def test_audio_timestamp_normalization_resamples_and_muxes_aac() -> None:
    """Real overlapping source PTS become ordered AAC frames in an MP4."""
    clock = AudioTimestampNormalizer()
    resampler = av.audio.resampler.AudioResampler(  # pylint: disable=c-extension-no-member
        format="fltp", layout="mono", rate=24000
    )
    output = BytesIO()
    container = av.open(output, mode="w", format="mp4")
    stream = container.add_stream("aac", rate=24000)
    stream.layout = "mono"
    converted_frames = []
    for pts in (3130, 4154, 4170, 5194):
        frame = av.AudioFrame(format="s16", layout="mono", samples=1024)
        frame.sample_rate = 8000
        frame.time_base = Fraction(1, 8000)
        frame.pts = pts
        clock.normalize(frame)
        converted_frames.extend(resampler.resample(frame))
    converted_frames.extend(resampler.resample(None))
    for previous, current in pairwise(converted_frames):
        assert previous.pts is not None and current.pts is not None
        assert current.pts >= previous.pts + previous.samples
    for frame in converted_frames:
        for packet in stream.encode(frame):
            container.mux(packet)
    for packet in stream.encode(None):
        container.mux(packet)
    container.close()
    output.seek(0)
    with av.open(output, format="mp4") as result:
        assert result.streams.audio[0].codec_context.name == "aac"
        assert sum(1 for packet in result.demux(audio=0) if packet.dts is not None) > 0
