"""Integration checks for opt-in Secure Video camera services."""

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, call, patch
from uuid import uuid4

from pyhap.hap_handler import HAPStatusError
from pyhap.hap_session import HAPSession
import pytest

from homeassistant.components import camera, ffmpeg
from homeassistant.components.homekit.accessories import HomeBridge
from homeassistant.components.homekit.const import (
    CONF_LINKED_MOTION_SENSOR,
    CONF_MAX_WIDTH,
    CONF_STREAM_SOURCE,
    SERV_MOTION_SENSOR,
)
from homeassistant.components.homekit.hksv_hardware import HardwareEncoder
from homeassistant.components.homekit.hksv_stream import MediaObservation
from homeassistant.components.homekit.type_cameras import FFMPEG_PID, Camera
from homeassistant.core import HomeAssistant
from homeassistant.setup import async_setup_component


@pytest.mark.usefixtures("socket_enabled")
async def test_camera_motion_missing_at_start(hass: HomeAssistant, run_driver) -> None:
    """A configured motion entity may appear after the accessory starts."""
    await async_setup_component(hass, ffmpeg.DOMAIN, {ffmpeg.DOMAIN: {}})
    await async_setup_component(
        hass, camera.DOMAIN, {camera.DOMAIN: {"platform": "demo"}}
    )
    await hass.async_block_till_done()
    hass.states.async_set("camera.demo_camera", None)
    accessory = Camera(
        hass,
        run_driver,
        "Camera",
        "camera.demo_camera",
        2,
        {CONF_LINKED_MOTION_SENSOR: "binary_sensor.motion", "secure_video": True},
    )
    bridge = HomeBridge("hass", run_driver, "Test Bridge")
    bridge.add_accessory(accessory)
    assert accessory._hksv is not None
    assert accessory.get_service(SERV_MOTION_SENSOR)
    assert not accessory._hksv.snapshot_allowed(None)
    accessory.run()
    await hass.async_block_till_done()
    async with asyncio.timeout(2):
        while not accessory._hksv.ready:
            await asyncio.sleep(0.01)
    hass.states.async_set("binary_sensor.motion", "on")
    await hass.async_block_till_done()
    assert accessory._char_motion_detected.value
    assert (
        accessory._hksv.services.transport
        in accessory._hksv.services.recording.linked_services
    )
    assert (
        accessory.get_service(SERV_MOTION_SENSOR)
        in accessory._hksv.services.recording.linked_services
    )
    assert accessory._hksv_motion_active.value is True
    assert len(accessory._hksv_stream_chars) == len(accessory._management)
    for char in accessory._hksv_stream_chars:
        assert char.value == 1
        assert accessory.iid_manager.get_iid(char) is not None
    session = HAPSession(uuid4(), b"x" * 32)
    first = accessory._hksv_stream_chars[0]
    first.client_update_value(0, session=session)
    assert not accessory._hksv.stream_active(0)
    with pytest.raises(RuntimeError, match="stream is disabled"):
        await accessory.start_stream({"id": "disabled", "stream_idx": 0}, {})
    first.client_update_value(1, session=session)
    assert accessory._hksv.stream_active(0)
    slot_zero = {"id": "slot_zero", "stream_idx": 0}
    slot_one = {"id": "slot_one", "stream_idx": 1}
    accessory.sessions.update({"slot_zero": slot_zero, "slot_one": slot_one})
    with patch.object(accessory, "stop_stream", new_callable=AsyncMock) as stop_stream:
        first.client_update_value(0, session=session)
        await hass.async_block_till_done()
        stop_stream.assert_awaited_once_with(slot_zero)
    first.client_update_value(1, session=session)
    accessory.sessions.clear()
    session_info = {"id": "existing"}
    accessory.sessions["existing"] = session_info
    session = HAPSession(uuid4(), b"x" * 32)
    with patch.object(accessory, "stop_stream", new_callable=AsyncMock) as stop_stream:
        accessory._hksv.services.camera_active.client_update_value(0, session=session)
        await hass.async_block_till_done()
        stop_stream.assert_awaited_once_with(session_info)
        assert accessory._hksv_motion_active.value is False
    accessory.sessions.clear()
    await accessory.stop()


async def test_insufficient_camera_limits_preserve_live_camera(
    hass: HomeAssistant, run_driver
) -> None:
    """Do not advertise recording when configured limits exclude its formats."""
    await async_setup_component(hass, ffmpeg.DOMAIN, {ffmpeg.DOMAIN: {}})
    hass.states.async_set("camera.demo_camera", "idle")
    accessory = Camera(
        hass,
        run_driver,
        "Camera",
        "camera.demo_camera",
        2,
        {
            CONF_LINKED_MOTION_SENSOR: "binary_sensor.motion",
            "secure_video": True,
            CONF_MAX_WIDTH: 1280,
        },
    )
    assert accessory._hksv is None
    assert accessory._management
    assert accessory.get_service(SERV_MOTION_SENSOR)


async def test_live_and_snapshot_available_while_hksv_probes(
    hass: HomeAssistant, run_driver
) -> None:
    """Allow existing camera access after privacy restore while probe is pending."""
    await async_setup_component(hass, ffmpeg.DOMAIN, {ffmpeg.DOMAIN: {}})
    hass.states.async_set("camera.demo_camera", "idle")
    accessory = Camera(
        hass,
        run_driver,
        "Camera",
        "camera.demo_camera",
        2,
        {CONF_LINKED_MOTION_SENSOR: "binary_sensor.motion", "secure_video": True},
    )
    controller = accessory._hksv
    assert controller is not None
    request = {"id": "test", "stream_idx": 0}
    with pytest.raises(RuntimeError, match="stream is disabled"):
        await accessory.start_stream(request, {})
    with pytest.raises(HAPStatusError):
        await accessory.async_get_snapshot({"image-width": 640, "image-height": 360})
    entered, release = asyncio.Event(), asyncio.Event()

    async def probe():
        entered.set()
        await release.wait()

    with (
        patch.object(controller.store, "async_load", new=AsyncMock(return_value={})),
        patch.object(controller, "_discover_native", new=probe),
        patch.object(controller.hds, "start", new=AsyncMock()),
        patch.object(
            accessory,
            "_async_get_hksv_source",
            new=AsyncMock(
                return_value=SimpleNamespace(
                    image=AsyncMock(return_value=b"jpeg"), close=AsyncMock()
                )
            ),
        ),
    ):
        task = asyncio.create_task(controller.start())
        try:
            await asyncio.wait_for(entered.wait(), 2)
            assert controller.privacy_ready and not controller.ready
            assert (
                await accessory.async_get_snapshot(
                    {"image-width": 640, "image-height": 360}
                )
                == b"jpeg"
            )
            controller.camera_active = False
            with pytest.raises(RuntimeError, match="stream is disabled"):
                await accessory.start_stream(request, {})
            with pytest.raises(HAPStatusError):
                await accessory.async_get_snapshot(
                    {"image-width": 640, "image-height": 360}
                )
        finally:
            release.set()
            await task
            await controller.close()


async def test_explicit_stream_source_disables_hksv_only(
    hass: HomeAssistant, run_driver, caplog: pytest.LogCaptureFixture
) -> None:
    """Reject an override that cannot share the camera entity's stream."""
    await async_setup_component(hass, ffmpeg.DOMAIN, {ffmpeg.DOMAIN: {}})
    hass.states.async_set("camera.demo_camera", "idle")
    source = "rtsp://example.invalid/live"
    accessory = Camera(
        hass,
        run_driver,
        "Camera",
        "camera.demo_camera",
        2,
        {
            CONF_LINKED_MOTION_SENSOR: "binary_sensor.motion",
            "secure_video": True,
            CONF_STREAM_SOURCE: source,
        },
    )
    assert accessory._hksv is None
    assert accessory._management
    with pytest.raises(ValueError, match="Explicit stream_source"):
        await accessory._async_get_hksv_source()
    assert "Secure Video is unavailable with an explicit stream source" in caplog.text


@pytest.mark.parametrize(
    ("error", "expected"),
    [
        (b"Connection refused", "connection_refused"),
        (b"Address already in use", "address_or_protocol"),
        (b"Bind failed", "address_or_protocol"),
        (b"Network is unreachable", "address_or_protocol"),
        (b"No route to host", "address_or_protocol"),
        (b"getaddrinfo", "address_or_protocol"),
    ],
)
async def test_live_failure_reports_only_safe_categories(
    hass: HomeAssistant,
    run_driver,
    caplog: pytest.LogCaptureFixture,
    error: bytes,
    expected: str,
) -> None:
    """Retain bounded failure categories without logging URLs or SRTP secrets."""
    await async_setup_component(hass, ffmpeg.DOMAIN, {ffmpeg.DOMAIN: {}})
    hass.states.async_set("camera.demo_camera", "idle")
    accessory = Camera(hass, run_driver, "Camera", "camera.demo_camera", 2, {})
    session = {
        "id": "diagnostic",
        "stream_idx": 0,
        FFMPEG_PID: 999999,
        "stream": SimpleNamespace(process=SimpleNamespace(returncode=1)),
    }
    reader = asyncio.StreamReader()
    reader.feed_data(
        b"x" * 4090 + error + b" rtsp://user:secret@host/stream srtp-key-secret"
    )
    reader.feed_eof()
    await accessory._async_log_stderr_stream(reader, session)
    assert session["ffmpeg_diagnostics"] == {expected}
    accessory.sessions["diagnostic"] = session
    with (
        patch(
            "homeassistant.components.homekit.type_cameras.pid_is_alive",
            return_value=False,
        ),
        patch.object(accessory, "set_streaming_available"),
    ):
        assert not await accessory._async_ffmpeg_watch("diagnostic")
    assert expected in caplog.text
    assert "secret" not in caplog.text
    assert "rtsp://" not in caplog.text
    accessory.sessions.clear()


@pytest.mark.parametrize(
    (
        "source_width",
        "source_height",
        "source_profile",
        "source_level",
        "expected_codec",
        "hardware",
    ),
    [
        (1280, 720, 100, 31, "copy", None),
        (2560, 1920, 100, 50, "libx264", None),
        (
            2560,
            1920,
            100,
            50,
            "h264_vaapi",
            HardwareEncoder(
                "h264_vaapi",
                ("-vaapi_device", "/dev/dri/render" + "D128"),
                "format=nv12,hwupload",
                ("-profile:v", "high", "-level:v", "4.0"),
            ),
        ),
    ],
)
async def test_secure_video_live_uses_codec_matching_negotiation(
    hass: HomeAssistant,
    run_driver,
    source_width: int,
    source_height: int,
    source_profile: int,
    source_level: int,
    expected_codec: str,
    hardware: HardwareEncoder | None,
) -> None:
    """Keep compatible video copied and scale incompatible video to the selection."""
    await async_setup_component(hass, ffmpeg.DOMAIN, {ffmpeg.DOMAIN: {}})
    hass.states.async_set("camera.demo_camera", "idle")
    accessory = Camera(
        hass, run_driver, "Camera", "camera.demo_camera", 2, {"video_codec": "copy"}
    )
    observed = MediaObservation(
        "h264",
        source_profile,
        source_level,
        source_width,
        source_height,
        None,
        None,
        None,
        None,
        15,
        2,
        15,
        900_000,
        800_000,
        4,
    )
    accessory._hksv = SimpleNamespace(
        privacy_ready=True,
        camera_active=True,
        stream_active=lambda _index: True,
        observe_live_source=AsyncMock(return_value=observed),
    )
    session = {"id": "live-test", "stream_idx": 0}
    accessory.sessions["live-test"] = session
    selection = {
        "v_profile_id": b"\x02",
        "v_level": b"\x02",
        "width": 1280,
        "height": 720,
        "fps": 15,
        "v_max_bitrate": 1500,
        "a_max_bitrate": 32,
        "address": "127.0.0.1",
        "v_port": 12345,
        "v_srtp_key": "synthetic",
        "v_ssrc": 12345,
    }
    reader = asyncio.StreamReader()
    reader.feed_eof()
    process = SimpleNamespace(
        pid=12345,
        returncode=None,
        stdin=SimpleNamespace(write=MagicMock(), drain=AsyncMock(), close=MagicMock()),
    )

    async def empty_parts():
        await asyncio.Event().wait()
        yield b""

    shared = SimpleNamespace(live_parts=empty_parts, close=AsyncMock())
    ffmpeg_process = SimpleNamespace(
        process=process,
        open=AsyncMock(return_value=True),
        get_reader=AsyncMock(return_value=reader),
    )
    with (
        patch.object(
            accessory, "_async_get_hksv_source", AsyncMock(return_value=shared)
        ),
        patch(
            "homeassistant.components.homekit.type_cameras.HAFFmpeg",
            return_value=ffmpeg_process,
        ),
        patch(
            "homeassistant.components.homekit.type_cameras._HARDWARE_PROBE.select",
            AsyncMock(return_value=hardware),
        ),
        patch.object(accessory, "_async_ffmpeg_watch", AsyncMock(return_value=True)),
    ):
        assert await accessory.start_stream(session, selection)
        output = ffmpeg_process.open.call_args.kwargs["output"]
        assert f"-c:v {expected_codec}" in output
        if expected_codec != "copy":
            assert "scale=1280:720:force_original_aspect_ratio=decrease" in output
            assert "pad=1280:720:(ow-iw)/2:(oh-ih)/2" in output
            if hardware:
                assert "hwupload" in output
                assert (
                    "-vaapi_device"
                    in ffmpeg_process.open.call_args.kwargs["input_source"]
                )
        else:
            assert "scale=" not in output
        accessory._async_stop_ffmpeg_watch("live-test")
        await accessory._release_shared_live(session)
    accessory.sessions.clear()


async def test_live_permission_rechecked_after_media_observation(
    hass: HomeAssistant, run_driver
) -> None:
    """A HomeKit privacy change during inspection prevents FFmpeg startup."""
    await async_setup_component(hass, ffmpeg.DOMAIN, {ffmpeg.DOMAIN: {}})
    hass.states.async_set("camera.demo_camera", "idle")
    accessory = Camera(
        hass, run_driver, "Camera", "camera.demo_camera", 2, {"video_codec": "copy"}
    )
    hksv = SimpleNamespace(
        privacy_ready=True, camera_active=True, stream_active=lambda _index: True
    )

    async def observe() -> None:
        hksv.camera_active = False

    hksv.observe_live_source = observe
    accessory._hksv = hksv
    session = {"id": "privacy-test", "stream_idx": 0}
    selection = {
        "v_profile_id": b"\x02",
        "v_level": b"\x02",
        "width": 1280,
        "height": 720,
        "fps": 15,
        "v_max_bitrate": 1500,
        "a_max_bitrate": 32,
        "address": "127.0.0.1",
        "v_port": 12345,
        "v_srtp_key": "synthetic",
        "v_ssrc": 12345,
    }
    with (
        patch(
            "homeassistant.components.homekit.type_cameras.HAFFmpeg"
        ) as ffmpeg_factory,
    ):
        assert not await accessory.start_stream(session, selection)
        ffmpeg_factory.assert_not_called()


async def test_shared_live_and_snapshot_privacy(
    hass: HomeAssistant, run_driver
) -> None:
    """Live and snapshots use the shared source and release their own leases."""
    await async_setup_component(hass, ffmpeg.DOMAIN, {ffmpeg.DOMAIN: {}})
    hass.states.async_set("camera.test", "idle")
    accessory = Camera(hass, run_driver, "Camera", "camera.test", 2, {})
    accessory._hksv = SimpleNamespace(
        privacy_ready=True,
        camera_active=True,
        stream_active=lambda _index: True,
        snapshot_allowed=lambda _reason: accessory._hksv.camera_active,
    )
    writer = SimpleNamespace(write=MagicMock(), drain=AsyncMock(), close=MagicMock())
    process = SimpleNamespace(pid=12345, returncode=None, stdin=writer)
    reader = asyncio.StreamReader()
    reader.feed_eof()
    ffmpeg_process = SimpleNamespace(
        process=process,
        open=AsyncMock(return_value=True),
        get_reader=AsyncMock(return_value=reader),
    )
    delivered = asyncio.Event()
    hold = asyncio.Event()

    async def parts():
        yield b"initialization"
        yield b"media"
        delivered.set()
        await hold.wait()

    shared = SimpleNamespace(
        live_parts=parts, image=AsyncMock(return_value=b"jpeg"), close=AsyncMock()
    )
    selection = {
        "v_profile_id": b"\x02",
        "v_level": b"\x02",
        "width": 1280,
        "height": 720,
        "fps": 15,
        "v_max_bitrate": 1500,
        "a_max_bitrate": 32,
        "address": "127.0.0.1",
        "v_port": 12345,
        "v_srtp_key": "synthetic",
        "v_ssrc": 12345,
    }
    session = {"id": "shared", "stream_idx": 0}
    accessory.sessions["shared"] = session
    with (
        patch.object(
            accessory, "_async_get_hksv_source", AsyncMock(return_value=shared)
        ),
        patch(
            "homeassistant.components.homekit.type_cameras.HAFFmpeg",
            return_value=ffmpeg_process,
        ),
        patch.object(accessory, "_async_ffmpeg_watch", AsyncMock(return_value=True)),
    ):
        assert await accessory.start_stream(session, selection)
        await asyncio.wait_for(delivered.wait(), 2)
        assert ffmpeg_process.open.call_args.kwargs["input_source"] == "-i pipe:0"
        assert writer.write.call_args_list == [call(b"initialization"), call(b"media")]
        assert (
            await accessory.async_get_snapshot(
                {"image-width": 640, "image-height": 360}
            )
            == b"jpeg"
        )
        shared.close.assert_awaited_once()
        await accessory._release_shared_live(session)
        assert shared.close.await_count == 2
        accessory._async_stop_ffmpeg_watch("shared")
    accessory.sessions.clear()


async def test_shared_live_privacy_change_releases_reference(
    hass: HomeAssistant, run_driver
) -> None:
    """A privacy change while acquiring the shared stream prevents FFmpeg startup."""
    await async_setup_component(hass, ffmpeg.DOMAIN, {ffmpeg.DOMAIN: {}})
    hass.states.async_set("camera.test", "idle")
    accessory = Camera(hass, run_driver, "Camera", "camera.test", 2, {})
    status = SimpleNamespace(
        privacy_ready=True,
        camera_active=True,
        stream_active=lambda _index: True,
    )
    accessory._hksv = status
    shared = SimpleNamespace(close=AsyncMock())

    async def acquire():
        status.camera_active = False
        return shared

    selection = {
        "v_profile_id": b"\x02",
        "v_level": b"\x02",
        "width": 1280,
        "height": 720,
        "fps": 15,
        "v_max_bitrate": 1500,
        "a_max_bitrate": 32,
        "address": "127.0.0.1",
        "v_port": 12345,
        "v_srtp_key": "synthetic",
        "v_ssrc": 12345,
    }
    with (
        patch.object(accessory, "_async_get_hksv_source", side_effect=acquire),
        patch("homeassistant.components.homekit.type_cameras.HAFFmpeg") as factory,
    ):
        assert not await accessory.start_stream(
            {"id": "privacy", "stream_idx": 0}, selection
        )
        shared.close.assert_awaited_once()
        factory.assert_not_called()


async def test_shared_snapshot_checks_privacy_after_decode(
    hass: HomeAssistant, run_driver
) -> None:
    """Do not return a frame if privacy changes while a keyframe is decoded."""
    await async_setup_component(hass, ffmpeg.DOMAIN, {ffmpeg.DOMAIN: {}})
    hass.states.async_set("camera.test", "idle")
    accessory = Camera(hass, run_driver, "Camera", "camera.test", 2, {})
    status = SimpleNamespace(camera_active=True)
    status.snapshot_allowed = lambda _reason: status.camera_active
    accessory._hksv = status

    async def image(_width, _height):
        status.camera_active = False
        return b"frame"

    shared = SimpleNamespace(image=image, close=AsyncMock())
    with (
        patch.object(
            accessory, "_async_get_hksv_source", AsyncMock(return_value=shared)
        ),
        pytest.raises(HAPStatusError),
    ):
        await accessory.async_get_snapshot({"image-width": 640, "image-height": 360})
    shared.close.assert_awaited_once()


async def test_live_backpressure_stops_only_live_pipe(
    hass: HomeAssistant, run_driver
) -> None:
    """A stalled live consumer releases its pipe without closing another lease."""
    await async_setup_component(hass, ffmpeg.DOMAIN, {ffmpeg.DOMAIN: {}})
    hass.states.async_set("camera.test", "idle")
    accessory = Camera(hass, run_driver, "Camera", "camera.test", 2, {})
    waiting = asyncio.Event()
    writer = SimpleNamespace(
        write=MagicMock(),
        drain=AsyncMock(side_effect=waiting.wait),
        close=MagicMock(),
    )
    process = SimpleNamespace(stdin=writer)

    async def parts():
        yield b"bounded part"

    shared = SimpleNamespace(live_parts=parts, close=AsyncMock())
    async with asyncio.timeout(7):
        await accessory._feed_shared_live(SimpleNamespace(process=process), shared)
    writer.write.assert_called_once_with(b"bounded part")
    writer.close.assert_called_once()
    shared.close.assert_not_awaited()


async def test_live_stop_closes_pipe_and_waits_for_process(
    hass: HomeAssistant, run_driver
) -> None:
    """A real child reading stdin exits when its shared input lease is released."""
    import sys  # noqa: PLC0415

    from homeassistant.components.homekit.type_cameras import (  # noqa: PLC0415
        FFMPEG_INPUT_TASK,
        FFMPEG_SHARED,
    )

    await async_setup_component(hass, ffmpeg.DOMAIN, {ffmpeg.DOMAIN: {}})
    hass.states.async_set("camera.test", "idle")
    accessory = Camera(hass, run_driver, "Camera", "camera.test", 2, {})
    process = await asyncio.create_subprocess_exec(
        sys.executable,
        "-c",
        "import sys; sys.stdin.buffer.read()",
        stdin=asyncio.subprocess.PIPE,
        stdout=asyncio.subprocess.DEVNULL,
        stderr=asyncio.subprocess.DEVNULL,
    )
    waiting = asyncio.Event()

    async def parts():
        yield b"media"
        await waiting.wait()

    shared = SimpleNamespace(live_parts=parts, close=AsyncMock())
    stream = SimpleNamespace(process=process)
    feeder = asyncio.create_task(accessory._feed_shared_live(stream, shared))
    session = {
        "id": "stopping",
        "stream": stream,
        FFMPEG_INPUT_TASK: feeder,
        FFMPEG_SHARED: shared,
    }
    accessory.sessions["stopping"] = session
    try:
        await asyncio.sleep(0.1)
        async with asyncio.timeout(5):
            await accessory.stop_stream(session)
        assert process.returncode == 0
        shared.close.assert_awaited_once()
    finally:
        if process.returncode is None:
            process.kill()
            await process.wait()
        accessory.sessions.clear()


async def test_live_exit_releases_shared_source_and_audio_proxy(
    hass: HomeAssistant, run_driver
) -> None:
    """An unexpectedly exited encoder releases its shared lease and audio proxy."""
    from homeassistant.components.homekit.type_cameras import (  # noqa: PLC0415
        AUDIO_PROXY,
        FFMPEG_SHARED,
    )

    await async_setup_component(hass, ffmpeg.DOMAIN, {ffmpeg.DOMAIN: {}})
    hass.states.async_set("camera.test", "idle")
    accessory = Camera(hass, run_driver, "Camera", "camera.test", 2, {})
    shared = SimpleNamespace(close=AsyncMock())
    proxy = SimpleNamespace(async_stop=AsyncMock())
    session = {
        "id": "ended",
        "stream_idx": 0,
        FFMPEG_PID: 12345,
        "stream": SimpleNamespace(process=SimpleNamespace(returncode=1)),
        FFMPEG_SHARED: shared,
        AUDIO_PROXY: proxy,
    }
    accessory.sessions["ended"] = session
    with (
        patch(
            "homeassistant.components.homekit.type_cameras.pid_is_alive",
            return_value=False,
        ),
        patch.object(accessory, "set_streaming_available"),
    ):
        assert not await accessory._async_ffmpeg_watch("ended")
    shared.close.assert_awaited_once()
    proxy.async_stop.assert_awaited_once()
    accessory.sessions.clear()


async def test_snapshot_privacy_change_during_release(
    hass: HomeAssistant, run_driver
) -> None:
    """No frame is returned if privacy is disabled during provider cleanup."""
    await async_setup_component(hass, ffmpeg.DOMAIN, {ffmpeg.DOMAIN: {}})
    hass.states.async_set("camera.test", "idle")
    accessory = Camera(hass, run_driver, "Camera", "camera.test", 2, {})
    status = SimpleNamespace(camera_active=True)
    status.snapshot_allowed = lambda _reason: status.camera_active
    accessory._hksv = status

    async def close():
        await asyncio.sleep(0)
        status.camera_active = False

    shared = SimpleNamespace(image=AsyncMock(return_value=b"frame"), close=close)
    with (
        patch.object(
            accessory, "_async_get_hksv_source", AsyncMock(return_value=shared)
        ),
        pytest.raises(HAPStatusError),
    ):
        await accessory.async_get_snapshot({"image-width": 640, "image-height": 360})


async def test_snapshot_without_keyframe_times_out_and_releases_source(
    hass: HomeAssistant, run_driver
) -> None:
    """An unavailable keyframe is bounded and its stream lease is released."""
    await async_setup_component(hass, ffmpeg.DOMAIN, {ffmpeg.DOMAIN: {}})
    hass.states.async_set("camera.test", "idle")
    accessory = Camera(hass, run_driver, "Camera", "camera.test", 2, {})
    accessory._hksv = SimpleNamespace(snapshot_allowed=lambda _reason: True)

    async def never(*_args):
        await asyncio.Event().wait()

    shared = SimpleNamespace(image=AsyncMock(side_effect=never), close=AsyncMock())
    with (
        patch("homeassistant.components.homekit.type_cameras.SNAPSHOT_TIMEOUT", 0.01),
        patch.object(
            accessory, "_async_get_hksv_source", AsyncMock(return_value=shared)
        ),
        pytest.raises(TimeoutError),
    ):
        await accessory.async_get_snapshot({"image-width": 640, "image-height": 360})
    shared.close.assert_awaited_once()


async def test_snapshot_source_acquisition_is_bounded(
    hass: HomeAssistant, run_driver
) -> None:
    """A source that never opens cannot hold a snapshot request indefinitely."""
    await async_setup_component(hass, ffmpeg.DOMAIN, {ffmpeg.DOMAIN: {}})
    hass.states.async_set("camera.test", "idle")
    accessory = Camera(hass, run_driver, "Camera", "camera.test", 2, {})
    accessory._hksv = SimpleNamespace(snapshot_allowed=lambda _reason: True)

    async def never(*_args):
        await asyncio.Event().wait()

    with (
        patch("homeassistant.components.homekit.type_cameras.SNAPSHOT_TIMEOUT", 0.01),
        patch.object(accessory, "_async_get_hksv_source", AsyncMock(side_effect=never)),
        pytest.raises(TimeoutError),
    ):
        await accessory.async_get_snapshot({"image-width": 640, "image-height": 360})


async def test_default_encoder_outputs_negotiated_size_profile_and_level(
    hass: HomeAssistant, run_driver
) -> None:
    """A real 4 MP input is encoded to HomeKit's negotiated 720p format."""
    from io import BytesIO  # noqa: PLC0415
    import shlex  # noqa: PLC0415
    import shutil  # noqa: PLC0415

    import av  # noqa: PLC0415

    binary = shutil.which("ffmpeg")
    if not binary:
        pytest.skip("ffmpeg unavailable")
    await async_setup_component(hass, ffmpeg.DOMAIN, {ffmpeg.DOMAIN: {}})
    hass.states.async_set("camera.test", "idle")
    accessory = Camera(hass, run_driver, "Camera", "camera.test", 2, {})
    selection = {
        "v_profile_id": b"\x02",
        "v_level": b"\x02",
        "width": 1280,
        "height": 720,
        "fps": 15,
    }
    _, codec, options, hardware = await accessory._live_video_parameters(selection)
    assert codec == "libx264"
    assert hardware is None
    process = await asyncio.create_subprocess_exec(
        binary,
        "-hide_banner",
        "-loglevel",
        "error",
        "-f",
        "lavfi",
        "-i",
        "color=c=blue:s=2560x1920:r=15",
        "-frames:v",
        "2",
        "-c:v",
        codec,
        *shlex.split(options),
        "-r",
        "15",
        "-movflags",
        "frag_keyframe+empty_moov",
        "-f",
        "mp4",
        "pipe:1",
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    async with asyncio.timeout(15):
        output, stderr = await process.communicate()
    assert process.returncode == 0, stderr.decode(errors="replace")
    with av.open(BytesIO(output), format="mp4") as container:
        video = container.streams.video[0]
        assert (video.width, video.height) == (1280, 720)
        assert video.codec_context.profile == "High"
        assert video.codec_context.extradata[3] == 40
        assert video.average_rate == 15
        assert len(list(container.decode(video=0))) == 2
