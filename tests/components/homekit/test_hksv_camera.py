"""Integration checks for opt-in Secure Video camera services."""

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch
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
            accessory, "_async_get_stream_source", new=AsyncMock(return_value=None)
        ),
        patch(
            "homeassistant.components.homekit.type_cameras.camera.async_get_image",
            new=AsyncMock(return_value=SimpleNamespace(content=b"jpeg")),
        ),
    ):
        task = asyncio.create_task(controller.start())
        try:
            await asyncio.wait_for(entered.wait(), 2)
            assert controller.privacy_ready and not controller.ready
            assert await accessory.start_stream(request, {}) is False
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
    """Keep the live source when an explicit override prevents shared HKSV."""
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
    assert await accessory._async_get_stream_source() == source
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
    process = SimpleNamespace(pid=12345, returncode=None)
    ffmpeg_process = SimpleNamespace(
        process=process,
        open=AsyncMock(return_value=True),
        get_reader=AsyncMock(return_value=reader),
    )
    with (
        patch.object(
            accessory, "_async_get_stream_source", AsyncMock(return_value="/dev/null")
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
        patch.object(
            accessory, "_async_get_stream_source", AsyncMock(return_value="/dev/null")
        ),
        patch(
            "homeassistant.components.homekit.type_cameras.HAFFmpeg"
        ) as ffmpeg_factory,
    ):
        assert not await accessory.start_stream(session, selection)
        ffmpeg_factory.assert_not_called()
