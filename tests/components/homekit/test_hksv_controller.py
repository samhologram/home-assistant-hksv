"""Isolated lifecycle checks for the experimental HKSV controller."""

import asyncio
from types import SimpleNamespace
from unittest.mock import patch
from uuid import uuid4

from pyhap.hap_session import HAPSession
import pyhap.secure_video as s

from homeassistant.components.homekit import hksv_controller as c


class Store:
    """Minimal in-memory state store."""

    def __init__(self) -> None:
        """Initialize state."""
        self.data = {"camera_active": 0, "event_snapshots": 0}

    async def async_load(self):
        """Load state."""
        return self.data

    async def async_save(self, data):
        """Save state."""
        self.data = data

    def async_delay_save(self, callback, delay):
        """Save a snapshot."""
        self.data = callback()


class HDS:
    """Fake transport server."""

    def __init__(self) -> None:
        """Initialize transport state."""
        self.started = False
        self.closed = False
        self.owner_closed = None

    async def start(self) -> None:
        """Start the fake transport."""
        self.started = True

    async def close(self) -> None:
        """Close the fake transport."""
        self.closed = True

    def prepare_session(self, secret, salt, owner):
        """Prepare an authenticated session."""
        assert self.started and secret == b"x" * 32 and salt == b"y" * 32
        return 4321, b"z" * 32

    def close_owner(self, owner: object) -> None:
        """Record the closed owner."""
        self.owner_closed = owner


def test_session_and_lifecycle() -> None:
    """Session setup and cleanup preserve state."""

    async def run():
        async def source(_with_audio: bool):
            return None

        store = Store()
        controller = c.HKSVController(source, "ffmpeg", store)
        hds = controller.hds = HDS()
        controller.services.hds = hds
        assert controller.active is False
        await controller.start()
        assert hds.started and not controller.camera_active
        assert controller.services.event_snapshots.value == 0
        session = HAPSession(uuid4(), b"x" * 32)
        request = s.b64(s.tlv((1, b"\0"), (2, b"\0"), (3, b"y" * 32)))
        response = controller.services.setup.client_update_value(
            request, session=session
        )
        assert s.parse_tlv(s.unb64(response))[3] == b"z" * 32
        session.close()
        assert hds.owner_closed is session
        await controller.close()
        assert hds.closed and store.data["camera_active"] is False

    asyncio.run(run())


def test_encoder_failure_is_bounded_and_unstarted_stream_not_reserved() -> None:
    """Failures retry only within the configured bound."""

    async def run():
        async def source(_with_audio: bool):
            return None

        ctl = c.HKSVController(source, "ffmpeg", Store())
        ctl._ready = True
        ctl.active = True
        ctl.configuration = object()

        class Process:
            def __init__(self) -> None:
                self._task = asyncio.create_task(asyncio.sleep(0))
                self.stopped = False
                self.running = True

            async def wait(self):
                await self._task

            async def stop(self):
                self.stopped = True

        original_sleep = c.asyncio.sleep

        async def no_delay(_):
            await original_sleep(0)

        c.asyncio.sleep = no_delay
        scheduled = []
        ctl._schedule_reconcile = lambda: scheduled.append(True)
        try:
            for _ in range(4):
                proc = Process()
                ctl._process = proc
                ctl._process_key = ("key", False)
                stream = ctl._open_recording(object(), 1)
                assert not ctl._stream_active
                await stream.aclose()
                await ctl._monitor(proc)
                assert proc.stopped and ctl._process is None
            assert len(scheduled) == 3
            assert ctl._restart_attempts == 3
        finally:
            c.asyncio.sleep = original_sleep

    asyncio.run(run())


def test_snapshot_permissions() -> None:
    """Snapshot requests follow operating permissions."""

    async def source(_with_audio: bool):
        return None

    controller = c.HKSVController(source, "ffmpeg", Store())
    controller._ready = True
    controller._privacy_ready = True
    controller.services.event_snapshots.set_value(0, should_notify=False)
    assert controller.snapshot_allowed(0)
    assert not controller.snapshot_allowed(1)
    assert not controller.snapshot_allowed(None)
    controller.services.event_snapshots.set_value(1, should_notify=False)
    controller.services.periodic_snapshots.set_value(0, should_notify=False)
    assert not controller.snapshot_allowed(0)
    assert controller.snapshot_allowed(1)
    assert not controller.snapshot_allowed(None)
    controller.camera_active = False
    assert not controller.snapshot_allowed(0)
    assert not controller.snapshot_allowed(1)


def test_writes_fail_closed_before_state_load() -> None:
    """Reject writes before restoring privacy state."""

    async def source(_with_audio: bool):
        return None

    controller = c.HKSVController(source, "ffmpeg", Store())
    session = HAPSession(uuid4(), b"x" * 32)
    try:
        controller.services.camera_active.client_update_value(0, session=session)
    except RuntimeError:
        pass
    else:
        raise AssertionError("accepted write before state load")
    assert controller.services.camera_active.value == 1
    assert not controller.snapshot_allowed(None)


def test_shared_source_attached_and_detached() -> None:
    """Detach the shared provider during shutdown."""

    async def run():
        class Shared:
            closed = False

            async def fragments(self):
                yield object()

            async def close(self) -> None:
                self.closed = True

        shared = Shared()

        async def source(_with_audio: bool):
            return shared

        class Process:
            running = True
            stopped = False

            def __init__(self, command, seconds, limit) -> None:
                self.command = command
                self.input_data = None

            async def start(self, input_data):
                self.input_data = input_data

            async def wait(self):
                await asyncio.Event().wait()

            async def stop(self):
                self.stopped = True
                assert self.input_data is not None
                await self.input_data.aclose()

        ctl = c.HKSVController(source, "ffmpeg", Store())
        ctl.hds = HDS()
        ctl.active = True
        ctl.configuration = SimpleNamespace(prebuffer_ms=4000)
        with (
            patch.object(c, "RecordingProcess", Process),
            patch.object(
                c, "ffmpeg_command", return_value=["ffmpeg", "-i", "pipe:0"]
            ) as command,
        ):
            await ctl._reconcile()
            assert ctl._process is not None
            assert command.call_args.args[1] == ["-i", "pipe:0"]
            proc = ctl._process
            await ctl.close()
            assert proc.stopped and shared.closed

    asyncio.run(run())
