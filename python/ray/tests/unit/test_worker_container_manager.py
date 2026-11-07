import asyncio
import json
import os
import sys
import time
from contextlib import asynccontextmanager
from unittest import mock

import aiohttp
import pytest
from aiohttp import web
from aiohttp.test_utils import TestClient, TestServer

from ray._private.runtime_env.agent.runtime_env_agent import RuntimeEnvAgent
from ray._private.runtime_env.container_launch import split_container_options
from ray._private.runtime_env.worker_container_manager import WorkerContainerManager
from ray.core.generated import runtime_env_agent_pb2 as protocol

pytestmark = pytest.mark.skipif(
    sys.platform == "win32", reason="POSIX container controller"
)
SESSION = "1" * 56


def launch(letter="a", ttl=10):
    return dict(
        launch_id=letter * 56,
        session_id=SESSION,
        job_id="01000000",
        expires_at_ms=int((time.time() + ttl) * 1000),
    )


class Engine:
    def __init__(self):
        self.calls = []
        self.containers = {}
        self.gate = asyncio.Event()
        self.gate.set()
        self.started = asyncio.Event()
        self.fail_create = False
        self.fail_remove = False

    async def run(self, args, environment=None, cwd=None, timeout=None):
        self.calls.append(args)
        if args[0] == "create":
            self.started.set()
            await self.gate.wait()
            name = next(
                arg.split("=", 1)[1] for arg in args if arg.startswith("--name=")
            )
            self.containers[name] = "a" * 64
            if self.fail_create:
                raise RuntimeError("Podman create was SIGKILLed")
            return "a" * 64
        if args[0] == "rm":
            if self.fail_remove:
                raise RuntimeError("Podman storage unavailable")
            for key in list(self.containers):
                if key in args or self.containers[key] in args:
                    self.containers.pop(key)
            return ""
        if args[0] == "ps":
            query = args[-1]
            if query.startswith("name=^"):
                return self.containers.get(query[6:-1], "")
            return "\n".join(self.containers.values())
        raise AssertionError(args)


@asynccontextmanager
async def managed(tmp_path, **kwargs):
    manager = WorkerContainerManager(tmp_path, **kwargs)
    engine = Engine()
    manager._run = engine.run
    await manager.start(SESSION, os.getpid())
    try:
        yield manager, engine
    finally:
        await manager.close()


async def create(manager, identity, options=None):
    return await manager.create_registered(identity, options or [], ["image"], {}, None)


async def settled(manager, identity):
    await manager.records[identity["launch_id"]].cleanup


def test_create_retries_share_one_creator_and_reject_conflicting_inputs(tmp_path):
    async def run():
        async with managed(tmp_path) as (manager, engine):
            identity = launch()
            manager.register(identity)
            assert await create(manager, identity) == "a" * 64
            assert await create(manager, identity) == "a" * 64
            with pytest.raises(RuntimeError, match="different inputs"):
                await create(manager, identity, ["--privileged"])
            assert sum(args[0] == "create" for args in engine.calls) == 1

    asyncio.run(run())


def test_stop_before_grant_fences_create_and_late_registration(tmp_path):
    async def run():
        async with managed(tmp_path) as (manager, engine):
            identity = launch()
            assert manager.stop(identity)
            with pytest.raises(RuntimeError, match="cancelled"):
                manager.register(identity)
            with pytest.raises(RuntimeError, match="cancelled"):
                await create(manager, identity)
            await settled(manager, identity)
            assert not any(args[0] == "create" for args in engine.calls)

    asyncio.run(run())


def test_create_before_grant_waits_only_until_raylet_deadline(tmp_path):
    async def run():
        async with managed(tmp_path) as (manager, engine):
            identity = launch(ttl=0.03)
            with pytest.raises(asyncio.TimeoutError):
                await create(manager, identity)
            assert not engine.calls

    asyncio.run(run())


def test_early_create_is_released_by_grant(tmp_path):
    async def run():
        async with managed(tmp_path) as (manager, engine):
            identity = launch()
            task = asyncio.create_task(create(manager, identity))
            await asyncio.sleep(0)
            manager.register(identity)
            assert await task == "a" * 64

    asyncio.run(run())


def test_stop_returns_immediately_but_waits_for_creator_before_removal(tmp_path):
    async def run():
        async with managed(tmp_path) as (manager, engine):
            identity = launch()
            engine.gate.clear()
            manager.register(identity)
            caller = asyncio.create_task(create(manager, identity))
            await engine.started.wait()
            caller.cancel()
            await asyncio.gather(caller, return_exceptions=True)
            assert manager.stop(identity)
            await asyncio.sleep(0)
            assert not any(args[0] == "rm" for args in engine.calls)
            engine.gate.set()
            await settled(manager, identity)
            assert not engine.containers
            assert sum(args[0] == "create" for args in engine.calls) == 1

    asyncio.run(run())


def test_signalled_creator_can_finish_cleanup_instead_of_unknown_forever(tmp_path):
    async def run():
        async with managed(tmp_path) as (manager, engine):
            identity = launch()
            engine.fail_create = True
            manager.register(identity)
            with pytest.raises(RuntimeError):
                await create(manager, identity)
            await settled(manager, identity)
            assert not engine.containers
            assert not manager._failed

    asyncio.run(run())


def test_cleanup_retries_are_finite_and_disable_only_container_admission(tmp_path):
    async def run():
        async with managed(tmp_path, cleanup_attempts=2) as (manager, engine):
            identity = launch()
            manager.register(identity)
            await create(manager, identity)
            engine.fail_remove = True
            manager.stop(identity)
            await settled(manager, identity)
            assert sum(args[0] == "rm" for args in engine.calls) == 2
            assert manager.records[identity["launch_id"]].failed
            with pytest.raises(RuntimeError, match="disabled"):
                manager.check_admission()

    asyncio.run(run())


def test_fences_expire_and_no_worker_files_or_fsyncs_are_created(tmp_path):
    async def run():
        async with managed(tmp_path) as (manager, engine):
            identities = [launch(letter, ttl=0.08) for letter in "abcdef"]
            with mock.patch("os.fsync", side_effect=AssertionError("per-worker fsync")):
                for identity in identities:
                    manager.register(identity)
                    await create(manager, identity)
                    manager.stop(identity)
                    await settled(manager, identity)
                await asyncio.sleep(0.1)
            assert not manager.records
            assert sorted(p.suffix for p in tmp_path.iterdir()) == [".json", ".lock"]
            for identity in identities:
                with pytest.raises(RuntimeError, match="expired"):
                    manager.register(identity)
                with pytest.raises(RuntimeError, match="expired"):
                    await create(manager, identity)

    asyncio.run(run())


def test_grants_are_bounded_and_overflow_cancellation_cannot_resurrect(tmp_path):
    async def run():
        async with managed(tmp_path, max_pending=1) as (manager, engine):
            first, other = launch("a"), launch("b")
            manager.register(first)
            with pytest.raises(RuntimeError, match="Too many"):
                manager.register(other)
            with pytest.raises(RuntimeError):
                manager.stop(other)
            assert len(manager.records) == 1
            with pytest.raises(RuntimeError):
                await create(manager, other)
            assert not engine.calls

    asyncio.run(run())


def test_corrupt_session_marker_does_not_crash_agent_for_ordinary_jobs(tmp_path):
    async def run():
        (tmp_path / ("2" * 56 + ".json")).write_text("broken")
        manager = WorkerContainerManager(tmp_path)
        await manager.start(SESSION, os.getpid())
        try:
            with pytest.raises(RuntimeError, match="disabled"):
                manager.check_admission()
        finally:
            await manager.close()

    asyncio.run(run())


def test_dead_session_sweep_removes_marker_but_preserves_live_session(tmp_path):
    async def run():
        dead = "2" * 56
        live = "3" * 56
        from ray._private.runtime_env.worker_container_manager import _birth

        (tmp_path / (dead + ".json")).write_text(
            json.dumps({"pid": 99999999, "birth": "1"})
        )
        (tmp_path / (live + ".json")).write_text(
            json.dumps({"pid": os.getpid(), "birth": _birth(os.getpid())})
        )
        manager = WorkerContainerManager(tmp_path)
        engine = Engine()
        engine.containers["old"] = "a" * 64
        manager._run = engine.run
        await manager.start(SESSION, os.getpid())
        try:
            await asyncio.gather(*manager._recoveries)
            assert not (tmp_path / (dead + ".json")).exists()
            assert (tmp_path / (live + ".json")).exists()
            assert not engine.containers
            assert all(live not in " ".join(args) for args in engine.calls)
            assert not manager._recoveries
        finally:
            await manager.close()

    asyncio.run(run())


def test_dead_session_with_inflight_creator_is_not_swept(tmp_path):
    import fcntl

    async def run():
        dead = "2" * 56
        (tmp_path / (dead + ".json")).write_text(
            json.dumps({"pid": 99999999, "birth": "1"})
        )
        with open(tmp_path / (dead + ".lock"), "a") as creator_lock:
            fcntl.flock(creator_lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            manager = WorkerContainerManager(tmp_path, cleanup_attempts=1)
            engine = Engine()
            manager._run = engine.run
            await manager.start(SESSION, os.getpid())
            try:
                await asyncio.gather(*manager._recoveries)
                assert not engine.calls
                assert manager._failed
                assert (tmp_path / (dead + ".json")).exists()
            finally:
                await manager.close()

    asyncio.run(run())


def test_hung_command_is_killed_and_reaped_within_deadline(tmp_path):
    async def run():
        manager = WorkerContainerManager(
            tmp_path,
            operation_timeout=0.1,
            podman_command=[sys.executable, "-c", "import time; time.sleep(30)"],
        )
        await manager.start(SESSION, os.getpid())
        try:
            with pytest.raises(asyncio.TimeoutError):
                await asyncio.wait_for(manager._run(["create"]), 2)
            assert not manager._commands
        finally:
            await manager.close()

    asyncio.run(run())


def test_creator_inherits_session_lock_across_agent_exit(tmp_path):
    import fcntl

    async def run():
        manager = WorkerContainerManager(
            tmp_path,
            operation_timeout=0.2,
            podman_command=[
                sys.executable,
                "-c",
                "print('ready',flush=True); import time; time.sleep(30)",
            ],
        )
        await manager.start(SESSION, os.getpid())
        caller = asyncio.create_task(manager._run(["create"]))
        try:
            await asyncio.sleep(0.08)
            manager._owner_lock.close()
            manager._owner_lock = None
            with open(tmp_path / (SESSION + ".lock"), "a") as lock:
                with pytest.raises(BlockingIOError):
                    fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            with pytest.raises(asyncio.TimeoutError):
                await caller
            with open(tmp_path / (SESSION + ".lock"), "a") as lock:
                fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        finally:
            caller.cancel()
            await asyncio.gather(caller, return_exceptions=True)
            await manager.close()

    asyncio.run(run())


@pytest.mark.parametrize(
    "flags", [["-d"], ["-dit"], ["--detach"], ["--cidfile=x"], ["--name=x"]]
)
def test_lifetime_options_are_rejected(flags):
    with pytest.raises(ValueError):
        split_container_options(flags)


@pytest.mark.parametrize(
    "flags,expected_create,expected_attach",
    [
        (["-it"], ["-it"], ["--interactive"]),
        (["--label", "--detach"], ["--label", "--detach"], []),
        (["-l", "--sig-proxy=false"], ["-l", "--sig-proxy=false"], []),
        (
            ["--sig-proxy=false", "--detach-keys", "ctrl-q"],
            [],
            ["--sig-proxy=false", "--detach-keys", "ctrl-q"],
        ),
    ],
)
def test_option_values_and_combined_short_flags(
    flags, expected_create, expected_attach
):
    assert split_container_options(flags) == (expected_create, expected_attach)


async def server_for(manager):
    agent = RuntimeEnvAgent.__new__(RuntimeEnvAgent)
    agent._worker_container_manager = manager
    app = web.Application()
    agent.add_container_routes(app)
    client = TestClient(TestServer(app))
    await client.start_server()
    return client


@pytest.mark.parametrize("lost_response", ["register", "create", "stop"])
def test_http_retries_do_not_duplicate_or_resurrect_launch(tmp_path, lost_response):
    async def run():
        async with managed(tmp_path) as (manager, engine):
            client = await server_for(manager)
            owner = launch()
            identity = protocol.WorkerContainerLaunch(**owner)
            try:
                requests = [
                    (
                        "register",
                        protocol.RegisterWorkerContainerRequest(launch=identity),
                        protocol.RegisterWorkerContainerReply,
                    ),
                    (
                        "create",
                        protocol.CreateWorkerContainerRequest(
                            launch=identity, entrypoint=["image"]
                        ),
                        protocol.CreateWorkerContainerReply,
                    ),
                    (
                        "stop",
                        protocol.StopWorkerContainerRequest(launch=identity),
                        protocol.StopWorkerContainerReply,
                    ),
                ]
                for operation, request, reply_cls in requests:
                    for _ in range(2 if operation == lost_response else 1):
                        response = await client.post(
                            "/" + operation + "_worker_container",
                            data=request.SerializeToString(),
                        )
                        reply = reply_cls.FromString(await response.read())
                        assert reply.status == protocol.AGENT_RPC_STATUS_OK
                await settled(manager, owner)
                assert not engine.containers
                assert sum(args[0] == "create" for args in engine.calls) == 1
                response = await client.post(
                    "/create_worker_container", data=requests[1][1].SerializeToString()
                )
                assert (
                    protocol.CreateWorkerContainerReply.FromString(
                        await response.read()
                    ).status
                    == protocol.AGENT_RPC_STATUS_FAILED
                )
            finally:
                await client.close()

    asyncio.run(run())


def test_hung_create_does_not_hold_stop_http_response(tmp_path):
    async def run():
        async with managed(tmp_path) as (manager, engine):
            engine.gate.clear()
            owner = launch()
            manager.register(owner)
            client = await server_for(manager)
            create_call = asyncio.create_task(
                client.post(
                    "/create_worker_container",
                    data=protocol.CreateWorkerContainerRequest(
                        launch=protocol.WorkerContainerLaunch(**owner),
                        entrypoint=["image"],
                    ).SerializeToString(),
                )
            )
            try:
                await engine.started.wait()
                response = await asyncio.wait_for(
                    client.post(
                        "/stop_worker_container",
                        data=protocol.StopWorkerContainerRequest(
                            launch=protocol.WorkerContainerLaunch(**owner)
                        ).SerializeToString(),
                    ),
                    0.5,
                )
                assert protocol.StopWorkerContainerReply.FromString(
                    await response.read()
                ).accepted
                assert engine.gate.is_set() is False
                engine.gate.set()
                await create_call
                await settled(manager, owner)
            finally:
                engine.gate.set()
                await client.close()

    asyncio.run(run())


@pytest.mark.parametrize("token_auth", [False, True])
def test_control_listener_is_separate_loopback_and_keeps_auth(
    tmp_path, monkeypatch, token_auth
):
    from types import SimpleNamespace

    from ray._private.authentication_test_utils import reset_auth_token_state
    from ray._private.runtime_env.agent import main as agent_main

    monkeypatch.setenv("RAY_RAYLET_PID", str(os.getpid()))
    monkeypatch.setenv("RAY_AUTH_MODE", "token" if token_auth else "disabled")
    monkeypatch.setenv("RAY_AUTH_TOKEN", "control-listener-test-token")
    reset_auth_token_state()

    async def run():
        manager = WorkerContainerManager(tmp_path / "owners")
        manager._run = Engine().run
        agent = RuntimeEnvAgent.__new__(RuntimeEnvAgent)
        agent._worker_container_manager = manager
        agent._container_session = SESSION
        agent._container_plugin = SimpleNamespace(container_manager_address=None)
        agent._image_uri_plugin = SimpleNamespace(container_manager_address=None)
        args = SimpleNamespace(session_dir=str(tmp_path), node_id=SESSION)
        public = web.Application()
        public.cleanup_ctx.append(agent_main.container_cleanup_context(agent, args))
        with mock.patch.object(agent_main, "persist_port") as publish:
            async with TestClient(TestServer(public)) as client:
                address = agent._container_plugin.container_manager_address
                assert address.startswith("127.0.0.1:")
                assert address == agent._image_uri_plugin.container_manager_address
                publish.assert_called_once_with(
                    str(tmp_path),
                    SESSION,
                    "worker_container_agent_port",
                    int(address.rsplit(":", 1)[1]),
                )
                owner = launch()
                request = protocol.RegisterWorkerContainerRequest(
                    launch=protocol.WorkerContainerLaunch(**owner)
                ).SerializeToString()
                assert (
                    await client.post("/register_worker_container", data=request)
                ).status == 404
                async with aiohttp.ClientSession() as local:
                    url = "http://" + address + "/register_worker_container"
                    if token_auth:
                        assert (await local.post(url, data=request)).status in (
                            401,
                            403,
                        )
                    headers = (
                        {"Authorization": "Bearer control-listener-test-token"}
                        if token_auth
                        else {}
                    )
                    response = await local.post(url, data=request, headers=headers)
                    assert response.status == 200
                    assert (
                        protocol.RegisterWorkerContainerReply.FromString(
                            await response.read()
                        ).status
                        == protocol.AGENT_RPC_STATUS_OK
                    )

    try:
        asyncio.run(run())
    finally:
        reset_auth_token_state()


def test_failed_container_cleanup_does_not_fail_ordinary_runtime_env_setup(tmp_path):
    import logging

    from ray._private.runtime_env.agent import runtime_env_agent as agent_module
    from ray.runtime_env import RuntimeEnv

    async def run():
        params = dict(
            logging_level=logging.INFO,
            logging_format="%(message)s",
            log_dir=str(tmp_path),
            max_bytes=0,
            backup_count=0,
        )
        with mock.patch.object(
            agent_module,
            "setup_component_logger",
            return_value=logging.getLogger("container-admission-test"),
        ), mock.patch("ray.util.get_node_ip_address", return_value="127.0.0.1"):
            agent = RuntimeEnvAgent(
                str(tmp_path / "resources"),
                params,
                None,
                str(tmp_path),
                "127.0.0.1",
                0,
                SESSION,
            )
        manager = WorkerContainerManager(tmp_path / "owners")
        manager._failed = True
        agent._worker_container_manager = manager
        ordinary = protocol.GetOrCreateRuntimeEnvRequest(
            serialized_runtime_env=RuntimeEnv(env_vars={"A": "1"}).serialize(),
            job_id=b"01000000",
            source_process="raylet",
        )
        reply = await agent.GetOrCreateRuntimeEnv(ordinary)
        assert reply.status == protocol.AGENT_RPC_STATUS_OK
        container = protocol.GetOrCreateRuntimeEnvRequest(
            serialized_runtime_env=RuntimeEnv(image_uri="image").serialize(),
            job_id=b"01000000",
            source_process="raylet",
        )
        reply = await agent.GetOrCreateRuntimeEnv(container)
        assert reply.status == protocol.AGENT_RPC_STATUS_FAILED
        assert "new containers are disabled" in reply.error_message

    asyncio.run(run())


def test_launcher_http_preserves_exact_deadline_and_host_environment(
    tmp_path, monkeypatch
):
    from ray._private.authentication_test_utils import reset_auth_token_state
    from ray._private.runtime_env.container_launch import run_worker_container

    monkeypatch.setenv("RAY_AUTH_MODE", "disabled")
    monkeypatch.setenv("HTTP_PROXY", "http://127.0.0.1:1")
    reset_auth_token_state()

    async def run():
        async with managed(tmp_path) as (manager, engine):
            owner = launch()
            manager.register(owner)
            client = await server_for(manager)
            address = str(client.make_url("/")).removeprefix("http://").rstrip("/")
            environment = dict(
                RAY_WORKER_CONTAINER_LAUNCH_ID=owner["launch_id"],
                RAY_WORKER_CONTAINER_SESSION_ID=owner["session_id"],
                RAY_WORKER_CONTAINER_JOB_ID=owner["job_id"],
                RAY_WORKER_CONTAINER_DEADLINE_MS=str(owner["expires_at_ms"]),
                RAY_AUTH_TOKEN="do-not-log-token",
            )
            try:
                with mock.patch(
                    "os.execvpe", side_effect=RuntimeError("exec attach")
                ) as execute:
                    with pytest.raises(RuntimeError, match="exec attach"):
                        await asyncio.to_thread(
                            run_worker_container, address, [], ["image"], environment
                        )
                command = execute.call_args.args[1]
                assert command == ["podman", "start", "--attach", "a" * 64]
                assert execute.call_args.args[2] == environment
                assert "do-not-log-token" not in " ".join(command)
                assert manager.records[owner["launch_id"]].identity == owner
                manager.stop(owner)
                await settled(manager, owner)
            finally:
                await client.close()

    try:
        asyncio.run(run())
    finally:
        reset_auth_token_state()


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
