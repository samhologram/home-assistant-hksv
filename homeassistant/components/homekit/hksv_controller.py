"""Experimental per-camera HomeKit Secure Video lifecycle.

The owner must explicitly attach services and stop this controller on teardown.
"""

import asyncio
import base64
from collections.abc import AsyncIterator, Awaitable, Callable
from dataclasses import replace
import logging
import time
from typing import Any

from pyhap.datastream import HDSReject, HDSServer
from pyhap.secure_video import HKSVServices, RecordingCapabilities, b64, parse_selected

from homeassistant.exceptions import HomeAssistantError

from .hksv_config import RecordingConfiguration
from .hksv_media import (
    RecordingProcess,
    can_copy_video,
    ffmpeg_command,
    shared_mp4_input,
)
from .hksv_stream import SharedCameraStream, SharedFragment, inspect_recording

_LOGGER = logging.getLogger(__name__)


class HKSVController:
    """Own one camera's HDS listener and independently bounded recording process."""

    def __init__(
        self,
        source: Callable[[bool], Awaitable[Any]],
        ffmpeg: str,
        store: Any,
        on_camera_disabled: Callable[[], None] | None = None,
        on_state_loaded: Callable[[], None] | None = None,
    ) -> None:
        """Initialize detached services and the recording lifecycle."""
        self.source = source
        self.ffmpeg = ffmpeg
        self.store = store
        self.on_camera_disabled = on_camera_disabled
        self.on_state_loaded = on_state_loaded
        self._stream_active_values: dict[int, bool] = {}
        self._ready = False
        self._privacy_ready = False
        self._selected_value: str | None = None
        self._native_resolution: tuple[int, int, int] | None = None
        self.capabilities = RecordingCapabilities(
            1280,
            720,
            15,
            0,
            2,
            0,
            2,
            resolutions=(
                (1280, 720, 15),
                (1280, 720, 30),
                (1920, 1080, 15),
                (1920, 1080, 30),
            ),
            profiles=(0, 1, 2),
            levels=(2,),
        )
        self.configuration: RecordingConfiguration | None = None
        self.hds = HDSServer(self._open_recording)
        self.services = HKSVServices(
            self.hds, self.capabilities, self._set_configuration
        )
        for char, handler in (
            (self.services.active, self._change_active),
            (self.services.camera_active, self._change_camera_active),
            (self.services.audio_active, self._change_audio_active),
            (self.services.event_snapshots, self._change_snapshots),
            (self.services.periodic_snapshots, self._change_snapshots),
        ):
            char.session_setter_callback = (
                lambda value, session, char=char, handler=handler: self._session_write(
                    char, handler, value
                )
            )
        selected_setter = self.services.selected.session_setter_callback
        self.services.selected.session_setter_callback = lambda value, session: (
            self._selected_write(selected_setter, value, session)
        )
        self.active = False
        self.camera_active = True
        self.audio_active = False
        self._process: RecordingProcess | None = None
        self._shared: Any = None
        self._process_key: tuple[RecordingConfiguration, bool] | None = None
        self._reconcile_task: asyncio.Task | None = None
        self._monitor_task: asyncio.Task | None = None
        self._restart_attempts = 0
        self._copy_disabled = False
        self._lock = asyncio.Lock()
        self._closed = False
        self._stream_active = False
        self._motion_until = 0.0
        self._motion_detected = False

    def _session_write(self, char: Any, handler: Callable, value: Any) -> None:
        if not self._ready or self._closed:
            raise RuntimeError("HKSV settings are still loading")
        handler(value)
        char.set_value(value)

    def _selected_write(self, handler: Callable, value: str, session: Any) -> None:
        if not self._ready or self._closed:
            raise RuntimeError("HKSV settings are still loading")
        handler(value, session)

    def _set_configuration(self, raw: bytes) -> None:
        selected = parse_selected(raw, self.capabilities)
        self.configuration = RecordingConfiguration.from_mapping(selected)
        self._copy_disabled = False
        self._restart_attempts = 0
        self._selected_value = b64(raw)
        self._schedule_save()
        self._schedule_reconcile()

    def _change_active(self, value: Any) -> None:
        if value not in (0, 1):
            raise ValueError("invalid recording active value")
        self.active = bool(value)
        self._restart_attempts = 0
        self._schedule_save()
        self._schedule_reconcile()

    def _change_camera_active(self, value: Any) -> None:
        if value not in (0, 1):
            raise ValueError("invalid camera active value")
        self.camera_active = bool(value)
        if self.on_state_loaded:
            self.on_state_loaded()
        if not self.camera_active and self.on_camera_disabled:
            self.on_camera_disabled()
        self._schedule_save()
        self._schedule_reconcile()

    def _change_audio_active(self, value: Any) -> None:
        if value not in (0, 1):
            raise ValueError("invalid audio active value")
        self.audio_active = bool(value)
        self._restart_attempts = 0
        self._schedule_save()
        self._schedule_reconcile()

    def _change_snapshots(self, value: Any) -> None:
        if value not in (0, 1):
            raise ValueError("invalid snapshots active value")
        # Characteristic is updated after callback; delay collecting the value.
        asyncio.get_running_loop().call_soon(self._schedule_save)

    def _state(self) -> dict[str, Any]:
        return {
            "active": self.active,
            "camera_active": self.camera_active,
            "audio_active": self.audio_active,
            "selected": self._selected_value,
            "event_snapshots": self.services.event_snapshots.value,
            "periodic_snapshots": self.services.periodic_snapshots.value,
            "stream_active": self._stream_active_values,
            "native_resolution": self._native_resolution,
        }

    def _schedule_save(self) -> None:
        if self._ready and not self._closed:
            self.store.async_delay_save(self._state, 1)

    def stream_active(self, index: int) -> bool:
        """Return the persisted streaming permission for a stream slot."""
        return self._stream_active_values.get(index, True)

    def set_stream_active(self, index: int, value: int) -> None:
        """Store a HomeKit stream slot permission."""
        if not self._ready or index < 0 or value not in (0, 1):
            raise ValueError("Invalid stream active value")
        self._stream_active_values[index] = bool(value)
        if self.on_state_loaded:
            self.on_state_loaded()
        if not value and self.on_camera_disabled:
            self.on_camera_disabled()
        self._schedule_save()

    @property
    def ready(self) -> bool:
        """Return whether persisted settings and the listener are ready."""
        return self._ready

    @property
    def privacy_ready(self) -> bool:
        """Return whether persisted privacy state has been restored."""
        return self._privacy_ready

    def snapshot_allowed(self, reason: int | None) -> bool:
        """Apply HomeKit snapshot permissions; unknown reasons fail closed."""
        if not self._privacy_ready or not self.camera_active:
            return False
        event = bool(self.services.event_snapshots.value)
        periodic = bool(self.services.periodic_snapshots.value)
        if reason is None:
            return event and periodic
        if reason == 0:
            return periodic
        if reason == 1:
            return event
        return False

    def motion(self, detected: bool) -> None:
        """Keep the recording open briefly after the latest motion detection."""
        self._motion_detected = detected
        if detected:
            self._motion_until = time.monotonic() + 8

    def _is_closed(self) -> bool:
        return self._closed

    async def start(self) -> None:
        """Start the listener before accepting HDS setup."""
        if self._closed:
            return
        state = await self.store.async_load() or {}
        if self._is_closed():
            return
        native_resolution = state.get("native_resolution")
        if (
            isinstance(native_resolution, list)
            and len(native_resolution) == 3
            and all(isinstance(item, int) for item in native_resolution)
        ):
            self._set_native_resolution(tuple(native_resolution))
        if selected := state.get("selected"):
            try:
                raw = base64.b64decode(selected, validate=True)
                self._set_configuration(raw)
                self.services.selected.set_value(selected, should_notify=False)
            except ValueError, TypeError:
                _LOGGER.warning("Ignoring invalid stored HKSV configuration")
        for key, char in (
            ("active", self.services.active),
            ("camera_active", self.services.camera_active),
            ("audio_active", self.services.audio_active),
            ("event_snapshots", self.services.event_snapshots),
            ("periodic_snapshots", self.services.periodic_snapshots),
        ):
            if key in state and state[key] in (0, 1, False, True):
                char.set_value(int(state[key]), should_notify=False)
        self.active = bool(self.services.active.value)
        self.camera_active = bool(self.services.camera_active.value)
        self.audio_active = bool(self.services.audio_active.value)
        for key, value in state.get("stream_active", {}).items():
            if str(key).isdigit() and value in (0, 1, False, True):
                self._stream_active_values[int(key)] = bool(value)
        if self.on_state_loaded:
            self.on_state_loaded()
        self._privacy_ready = True
        if self.camera_active and self._native_resolution is None:
            await self._discover_native()
        await self.hds.start()
        if self._is_closed():
            await self.hds.close()
            return
        self._ready = True
        self._schedule_reconcile()

    def _set_native_resolution(self, resolution: tuple[int, int, int]) -> None:
        """Add one bounded observed resolution alongside required fallback formats."""
        width, height, fps = resolution
        if not (0 < width <= 1920 and 0 < height <= 1080 and fps in (15, 30)):
            return
        self._native_resolution = resolution
        resolutions = tuple(dict.fromkeys((*self.capabilities.resolutions, resolution)))
        self.capabilities = replace(self.capabilities, resolutions=resolutions)
        _, video, _ = self.capabilities.supported()
        self.services.recording.get_characteristic(
            "Supported Video Recording Configuration"
        ).set_value(video)

    async def _discover_native(self) -> None:
        """Probe a shared source without changing its existing HLS provider."""
        shared = None
        try:
            async with asyncio.timeout(12):
                shared = await self.source(False)
                if shared is None or not hasattr(shared, "snapshot"):
                    return
                while not self._closed:
                    fragments = shared.snapshot()
                    if len(fragments) >= 2:
                        observed = await asyncio.to_thread(
                            inspect_recording,
                            fragments[-2].initialization,
                            (fragments[-2].data, fragments[-1].data),
                        )
                        if (
                            observed.h264_profile_idc in (66, 77, 100)
                            and observed.h264_level_idc <= 40
                            and 0 < observed.peak_fps <= 30
                        ):
                            self._set_native_resolution(
                                (
                                    observed.width,
                                    observed.height,
                                    15 if observed.peak_fps <= 15 else 30,
                                )
                            )
                        return
                    await asyncio.sleep(0.5)
        except TimeoutError, ValueError, OSError, HomeAssistantError:
            _LOGGER.debug("Could not probe native HKSV video format")
        finally:
            if shared:
                await shared.close()

    def _schedule_reconcile(self) -> None:
        if self._closed:
            return
        if self._reconcile_task is None or self._reconcile_task.done():
            self._reconcile_task = asyncio.create_task(self._reconcile())

    async def _reconcile(self) -> None:
        async with self._lock:
            while not self._closed:
                config = self.configuration
                desired = (
                    (config, self.audio_active)
                    if self.active and self.camera_active and config
                    else None
                )
                if desired == self._process_key:
                    return
                previous, self._process = self._process, None
                old_shared, self._shared = self._shared, None
                self._process_key = None
                if previous:
                    await previous.stop()
                if old_shared:
                    await old_shared.close()
                if desired is None:
                    return
                shared = None
                process = None
                try:
                    shared = await self.source(desired[1])
                    if shared is None:
                        raise RuntimeError("camera has no shared stream")  # noqa: TRY301
                    copy_video = await self._probe_copy(shared, desired[0])
                    command = ffmpeg_command(
                        self.ffmpeg,
                        ["-i", "pipe:0"],
                        desired[0],
                        desired[1],
                        copy_video=copy_video,
                    )
                    process = RecordingProcess(
                        command, desired[0].prebuffer_ms / 1000, 32 * 1024 * 1024
                    )
                    source = (
                        self._monitored_copy(shared, desired[0])
                        if copy_video
                        else shared.fragments()
                    )
                    await process.start(shared_mp4_input(source))
                except asyncio.CancelledError:
                    if process:
                        await process.stop()
                    if shared:
                        await shared.close()
                    raise
                except Exception:
                    if process:
                        await process.stop()
                    if shared:
                        await shared.close()
                    _LOGGER.exception("Could not start HKSV recording buffer")
                    return
                # State may have changed while acquiring source or launching FFmpeg.
                if (
                    self._closed
                    or not self.active
                    or not self.camera_active
                    or self.configuration != desired[0]
                    or self.audio_active != desired[1]
                ):
                    await process.stop()
                    await shared.close()
                    continue
                self._process, self._process_key = process, desired
                self._shared = shared
                self._monitor_task = asyncio.create_task(self._monitor(process))
                return

    async def _probe_copy(
        self, shared: SharedCameraStream, config: RecordingConfiguration
    ) -> bool:
        """Select copy only after observing a complete compatible window."""
        if self._copy_disabled or not hasattr(shared, "snapshot"):
            return False
        deadline = time.monotonic() + 10
        while not self._closed and time.monotonic() < deadline:
            fragments = shared.snapshot()
            if len(fragments) >= 2:
                try:
                    observed = await asyncio.to_thread(
                        inspect_recording,
                        fragments[-2].initialization,
                        (fragments[-2].data, fragments[-1].data),
                    )
                except ValueError, OSError:
                    return False
                return can_copy_video(observed, config)
            await asyncio.sleep(0.5)
        return False

    async def _monitored_copy(
        self, shared: SharedCameraStream, config: RecordingConfiguration
    ) -> AsyncIterator[SharedFragment]:
        """Validate each overlapping window before releasing copied media."""
        previous: SharedFragment | None = None
        async for fragment in shared.fragments():
            if previous is None:
                previous = fragment
                continue
            if fragment.stream_id != previous.stream_id:
                self._copy_disabled = True
                return
            try:
                observed = await asyncio.to_thread(
                    inspect_recording,
                    previous.initialization,
                    (previous.data, fragment.data),
                )
            except ValueError, OSError:
                self._copy_disabled = True
                return
            if not can_copy_video(observed, config):
                self._copy_disabled = True
                return
            yield previous
            previous = fragment

    async def _monitor(self, process: RecordingProcess) -> None:
        """Restart unexpectedly terminated encoders at most three times."""
        started = time.monotonic()
        try:
            await process.wait()
        except asyncio.CancelledError:
            raise
        except Exception:
            _LOGGER.exception("HKSV fragment reader failed")
        if self._closed or self._process is not process:
            return
        self._process = None
        self._process_key = None
        shared, self._shared = self._shared, None
        await process.stop()
        if shared:
            await shared.close()
        if time.monotonic() - started >= 60:
            self._restart_attempts = 0
        if self._restart_attempts >= 3:
            _LOGGER.error(
                "HKSV recording buffer stopped after repeated FFmpeg failures"
            )
            return
        self._restart_attempts += 1
        await asyncio.sleep(2 ** (self._restart_attempts - 1))
        if not self._closed:
            self._schedule_reconcile()

    def _open_recording(
        self, owner: object, stream_id: int
    ) -> AsyncIterator[tuple[bytes, bool]]:
        if not self._ready or not self.active or not self.camera_active:
            raise HDSReject(1)
        if self.configuration is None:
            raise HDSReject(9)
        if self._stream_active:
            raise HDSReject(2)
        process = self._process
        if process is None or not process.running:
            raise HDSReject(5)
        return self._recording(process)

    async def _recording(
        self, process: RecordingProcess
    ) -> AsyncIterator[tuple[bytes, bool]]:
        if self._stream_active:
            raise HDSReject(2)
        self._stream_active = True
        try:
            start = time.monotonic()
            first = True
            async for fragment in process.fragments():
                if first:
                    first = False
                    yield fragment, False
                    continue
                # A HomeKit request is itself an event; allow a full fragment even
                # if motion was only a short event pulse. Cap individual clips.
                last = (
                    not self.active
                    or not self.camera_active
                    or self._closed
                    or (
                        not self._motion_detected
                        and time.monotonic() >= self._motion_until
                        and time.monotonic() - start >= 4
                    )
                    or time.monotonic() - start >= 60
                )
                yield fragment, last
                if last:
                    return
        finally:
            self._stream_active = False

    async def close(self) -> None:
        """Stop sessions, background reconciliation and the FFmpeg child."""
        self._closed = True
        if self._ready:
            try:
                await self.store.async_save(self._state())
            except Exception:
                _LOGGER.exception("Could not persist HKSV state during shutdown")
        await self.hds.close()
        if self._monitor_task:
            self._monitor_task.cancel()
            await asyncio.gather(self._monitor_task, return_exceptions=True)
        if self._reconcile_task:
            self._reconcile_task.cancel()
            await asyncio.gather(self._reconcile_task, return_exceptions=True)
        async with self._lock:
            if self._process:
                await self._process.stop()
                self._process = None
            if self._shared:
                await self._shared.close()
                self._shared = None
            self._process_key = None
