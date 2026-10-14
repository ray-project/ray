import asyncio
import pickle
import struct
import sys
from types import SimpleNamespace

import pytest

from ray.experimental.sandbox.http.host_channel import (
    HostChannel,
    HostChannelError,
    HostChannelServer,
)
from ray.experimental.sandbox.http.node_host import CHANNEL_METHODS
from ray.experimental.sandbox.http.resolver import (
    HostedSandboxHandle,
    RayActorHandleResolver,
)
from ray.experimental.sandbox.http.schemas import SandboxAPISettings


class _HostError(Exception):
    pass


class _Exploit:
    """Unpickling it creates ``marker`` (see test_hello_is_never_decoded)."""

    def __init__(self, marker: str) -> None:
        self.marker = marker

    def __reduce__(self):
        return (exec, (f"open({self.marker!r}, 'w').close()",))


class _Host:
    """Stands in for a SandboxNodeHost: async and plain methods."""

    def __init__(self) -> None:
        self.release = asyncio.Event()

    async def describe(self, sandbox_id, wait_seconds=0.0):
        return {"sandbox_id": sandbox_id, "status": "running"}

    async def get_exec_by_key(self, sandbox_id, exec_key, wait_seconds=0.0):
        await self.release.wait()
        return {"exec_key": exec_key, "status": "completed"}

    async def start_exec(self, sandbox_id, command, exec_key=None):
        raise ValueError(f"bad command {command!r}")

    def stdin_status(self, sandbox_id, exec_key):
        return {"sandbox_id": sandbox_id, "exec_key": exec_key}

    async def read_file(self, sandbox_id, path):
        if path == "/set":
            return {"content": {1, 2}}  # msgpack can't carry a set
        if path == "/custom":
            raise _HostError("custom failure")
        return {"ok": True, "content": b"\x00bytes"}

    async def write_file(self, sandbox_id, path, content, append=False):
        return {"ok": True, "content": content}

    async def warm_status(self):
        return {}


async def _connected(server: HostChannelServer) -> HostChannel:
    channel = HostChannel(await server.start("127.0.0.1"))
    await channel.connect()
    return channel


def test_calls_round_trip_with_results_and_errors():
    async def scenario():
        server = HostChannelServer(_Host(), CHANNEL_METHODS)
        channel = await _connected(server)
        assert await channel.call("describe", ("sb-1",), {}) == {
            "sandbox_id": "sb-1",
            "status": "running",
        }
        # Plain methods are served too.
        assert await channel.call("stdin_status", ("sb-1", "k"), {}) == {
            "sandbox_id": "sb-1",
            "exec_key": "k",
        }
        # The host's own exceptions come back as themselves.
        with pytest.raises(ValueError, match="bad command"):
            await channel.call("start_exec", ("sb-1", ["x"]), {"exec_key": "k"})
        # Only the listed methods are served.
        with pytest.raises(AttributeError, match="not served"):
            await channel.call("warm_status", (), {})
        # The connection survives errors.
        assert (await channel.call("describe", ("sb-2",), {}))["sandbox_id"] == "sb-2"
        channel.close()
        server.close()

    asyncio.run(scenario())


def test_frames_far_above_the_stream_limit_round_trip():
    """Frames many times asyncio's 64 KiB stream limit go both ways: a read
    for more than the buffer holds resumes the paused transport."""

    async def scenario():
        server = HostChannelServer(_Host(), CHANNEL_METHODS)
        channel = await _connected(server)
        try:
            data = bytes(range(256)) * (8 * 4096)  # 8 MiB
            result = await asyncio.wait_for(
                channel.call("write_file", ("sb-1", "/f", data), {}), 30
            )
        finally:
            channel.close()
            server.close()
        return result["content"] == data

    assert asyncio.run(scenario())


def test_calls_wait_for_the_connection_to_drain_under_backpressure():
    async def scenario():
        server = HostChannelServer(_Host(), CHANNEL_METHODS)
        channel = await _connected(server)
        try:
            # As if far more than the drain threshold were still unsent.
            channel._writer.transport.get_write_buffer_size = lambda: 1 << 30
            pending = channel.call("describe", ("sb-1",), {})
            assert isinstance(pending, asyncio.Task)  # drains before the reply
            return await asyncio.wait_for(pending, 10)
        finally:
            channel.close()
            server.close()

    assert asyncio.run(scenario())["sandbox_id"] == "sb-1"


def test_calls_run_concurrently():
    async def scenario():
        host = _Host()
        server = HostChannelServer(host, CHANNEL_METHODS)
        channel = await _connected(server)
        waiting = channel.call("get_exec_by_key", ("sb-1", "k"), {})
        # A long poll does not hold up the calls behind it.
        assert (await channel.call("describe", ("sb-1",), {}))["status"] == "running"
        assert not waiting.done()
        host.release.set()
        assert (await waiting)["status"] == "completed"
        channel.close()
        server.close()

    asyncio.run(scenario())


def test_hello_is_never_decoded(tmp_path):
    """A peer without the token can't get its input deserialized."""
    marker = tmp_path / "exploited"

    async def scenario():
        server = HostChannelServer(_Host(), CHANNEL_METHODS)
        endpoint = await server.start("127.0.0.1")
        payload = pickle.dumps(_Exploit(str(marker)))
        reader, writer = await asyncio.open_connection("127.0.0.1", endpoint["port"])
        writer.write(struct.pack("!I", len(payload)) + payload)
        await writer.drain()
        # The server hangs up without a reply.
        assert await reader.read() == b""
        writer.close()
        server.close()

    asyncio.run(asyncio.wait_for(scenario(), 5))
    assert not marker.exists()


def test_oversized_hello_is_refused():
    async def scenario():
        server = HostChannelServer(_Host(), CHANNEL_METHODS)
        endpoint = await server.start("127.0.0.1")
        reader, writer = await asyncio.open_connection("127.0.0.1", endpoint["port"])
        # Announces a 512 MiB first frame: refused before any of it is read.
        writer.write(struct.pack("!I", 1 << 29))
        await writer.drain()
        assert await reader.read() == b""
        writer.close()
        server.close()

    asyncio.run(asyncio.wait_for(scenario(), 5))


def test_values_and_errors_are_plain_data():
    async def scenario():
        server = HostChannelServer(_Host(), CHANNEL_METHODS)
        channel = await _connected(server)
        # Bytes survive the trip.
        assert (await channel.call("read_file", ("sb-1", "/f"), {}))[
            "content"
        ] == b"\x00bytes"
        # A host exception that isn't a builtin arrives as RuntimeError.
        with pytest.raises(RuntimeError, match="_HostError: custom failure"):
            await channel.call("read_file", ("sb-1", "/custom"), {})
        # A result msgpack can't carry fails over to Ray (HostChannelError),
        # and so do arguments it can't carry.
        with pytest.raises(HostChannelError):
            await channel.call("read_file", ("sb-1", "/set"), {})
        with pytest.raises(HostChannelError):
            channel.call("describe", ({1, 2},), {})
        # The connection is still usable.
        assert (await channel.call("describe", ("sb-1",), {}))["status"] == "running"
        channel.close()
        server.close()

    asyncio.run(scenario())


def test_concurrent_first_starts_share_one_server():
    async def scenario():
        server = HostChannelServer(object(), frozenset())
        try:
            endpoints = await asyncio.gather(
                *(server.start("127.0.0.1") for _ in range(4))
            )
        finally:
            server.close()
        return {endpoint["port"] for endpoint in endpoints}

    assert len(asyncio.run(scenario())) == 1


def test_wrong_token_is_refused():
    async def scenario():
        server = HostChannelServer(_Host(), CHANNEL_METHODS)
        endpoint = dict(await server.start("127.0.0.1"), token="not-the-token")
        channel = HostChannel(endpoint)
        await channel.connect()
        with pytest.raises(HostChannelError):
            await channel.call("describe", ("sb-1",), {})
        assert not channel.ready
        server.close()

    asyncio.run(scenario())


def test_lost_connection_fails_pending_calls():
    async def scenario():
        server = HostChannelServer(_Host(), CHANNEL_METHODS)
        channel = await _connected(server)
        waiting = channel.call("get_exec_by_key", ("sb-1", "k"), {})
        await asyncio.sleep(0.05)
        server.close()
        with pytest.raises(HostChannelError):
            await waiting
        assert not channel.ready
        with pytest.raises(HostChannelError):
            channel.call("describe", ("sb-1",), {})

    asyncio.run(scenario())


class _Remote:
    def __init__(self, fn):
        self._fn = fn

    def remote(self, *args, **kwargs):
        return self._fn(*args, **kwargs)


class _HostHandle:
    """A node host actor handle: Ray-path calls run on ``target`` and are logged."""

    def __init__(self, target, server: HostChannelServer) -> None:
        self._actor_id = SimpleNamespace(hex=lambda: "a1")
        self.ray_calls = []
        self._target = target
        self._server = server

    def __getattr__(self, name):
        if name == "channel_endpoint":
            return _Remote(lambda: self._server.start("127.0.0.1"))

        async def via_ray(*args, **kwargs):
            self.ray_calls.append(name)
            return await getattr(self._target, name)(*args, **kwargs)

        return _Remote(via_ray)


def test_hosted_calls_use_the_channel_and_fall_back_to_ray():
    async def scenario():
        target = _Host()
        server = HostChannelServer(target, CHANNEL_METHODS)
        host = _HostHandle(target, server)
        resolver = RayActorHandleResolver(
            SandboxAPISettings(host_mode="node", host_channel=True)
        )
        handle = HostedSandboxHandle(host, "host", "sb-1", resolver)

        # The first call goes through Ray while the channel connects.
        assert (await handle.describe.remote())["sandbox_id"] == "sb-1"
        assert host.ray_calls == ["describe"]
        for _ in range(200):
            if resolver.host_channel(host) is not None:
                break
            await asyncio.sleep(0.01)
        assert (await handle.describe.remote())["status"] == "running"
        assert host.ray_calls == ["describe"]

        # A call in flight when the connection breaks is repeated through Ray.
        waiting = asyncio.ensure_future(handle.get_exec_by_key.remote("k"))
        await asyncio.sleep(0.05)
        server.close()
        await asyncio.sleep(0.05)
        target.release.set()
        assert (await waiting)["status"] == "completed"
        assert host.ray_calls == ["describe", "get_exec_by_key"]
        # Later calls use Ray until the retry interval passes.
        assert (await handle.describe.remote())["status"] == "running"
        assert host.ray_calls == ["describe", "get_exec_by_key", "describe"]

    asyncio.run(scenario())


def test_hosted_calls_skip_the_channel_unless_enabled():
    async def scenario():
        target = _Host()
        host = _HostHandle(target, HostChannelServer(target, CHANNEL_METHODS))
        resolver = RayActorHandleResolver(SandboxAPISettings(host_mode="node"))
        handle = HostedSandboxHandle(host, "host", "sb-1", resolver)
        for _ in range(3):
            await handle.describe.remote()
            await asyncio.sleep(0.02)
        assert host.ray_calls == ["describe"] * 3
        assert resolver.host_channel(host) is None

    asyncio.run(scenario())


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
