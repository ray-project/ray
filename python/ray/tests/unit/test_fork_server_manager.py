"""Unit tests for ForkServerManager lifecycle, keying, backoff, and clamping."""

import asyncio
import logging
import os
import sys
import tempfile
import time
from pathlib import Path
from types import SimpleNamespace
from typing import List, Optional, Set, Tuple

import pytest

from ray._private.runtime_env.agent.fork_server_manager import (
    ForkServerManager,
    _expand_env_vars_snapshot,
    is_exec_bound_env_var,
)
from ray._private.runtime_env.agent.runtime_env_agent import RuntimeEnvAgent
from ray._private.runtime_env.context import RuntimeEnvContext
from ray.core.generated import runtime_env_agent_pb2
from ray.runtime_env import RuntimeEnv

pytestmark = pytest.mark.skipif(
    sys.platform != "linux", reason="ForkServer is Linux-only."
)


@pytest.fixture
def short_tmp_dir():
    """Provides a short /tmp directory so AF_UNIX sun_path stays under 107 bytes in CI."""
    with tempfile.TemporaryDirectory(dir="/tmp", prefix="rfs_") as tmp_dir:
        yield Path(tmp_dir)


class _FakePopen:
    def __init__(self, pid: int, returncode: Optional[int] = None):
        self.pid = pid
        self.returncode = returncode

    def poll(self) -> Optional[int]:
        return self.returncode


class _FakeForkServer:
    """In-memory stand-in for ForkServerProcess to test manager state transitions."""

    _next_pid = 90000

    def __init__(
        self,
        socket_path: str,
        preload_modules: Optional[List[str]] = None,
        context: Optional[RuntimeEnvContext] = None,
        cgroup_path: Optional[str] = None,
        wait_ready: bool = True,
        timeout_s: float = 120.0,
        logs_dir: Optional[str] = None,
        setup_log_path: Optional[str] = None,
        worker_stdout_fd: Optional[int] = None,
        worker_stderr_fd: Optional[int] = None,
        *,
        ready_delay_s: float = 0.0,
        fail_ready: Optional[Exception] = None,
        startup_warnings: Optional[List[str]] = None,
    ):
        _FakeForkServer._next_pid += 1
        self.socket_path = socket_path
        self.preload_modules = preload_modules
        self.context = context
        self.cgroup_path = cgroup_path
        self.logs_dir = logs_dir
        self.setup_log_path = setup_log_path
        self.worker_stdout_fd = worker_stdout_fd
        self.worker_stderr_fd = worker_stderr_fd
        self.process = _FakePopen(_FakeForkServer._next_pid)
        self.started_at = time.monotonic()
        self.ready_at: Optional[float] = None
        self._stopped = False
        self._valid_cwd = True
        self._ready_delay_s = ready_delay_s
        self._fail_ready = fail_ready
        self._startup_warnings = list(startup_warnings or [])
        self.stop_calls = 0

    @property
    def stopped(self) -> bool:
        return self._stopped

    def is_alive(self) -> bool:
        return not self._stopped and self.process.poll() is None

    def has_valid_cwd(self) -> bool:
        return self.is_alive() and self._valid_cwd

    def read_startup_warnings(self) -> List[str]:
        warnings = list(self._startup_warnings)
        self._startup_warnings.clear()
        return warnings

    async def wait_ready_async(self, timeout_s: Optional[float] = None) -> None:
        if self._ready_delay_s > 0:
            if timeout_s is not None and self._ready_delay_s > timeout_s:
                await asyncio.sleep(max(0.0, timeout_s))
                raise TimeoutError(f"Timed out waiting for {self.socket_path}")
            await asyncio.sleep(self._ready_delay_s)
        if self._fail_ready is not None:
            self.process.returncode = 1
            raise self._fail_ready
        os.makedirs(os.path.dirname(self.socket_path), exist_ok=True)
        with open(self.socket_path, "wb"):
            pass
        if self.ready_at is None:
            self.ready_at = time.monotonic()

    def stop(self) -> None:
        if self._stopped:
            return
        self._stopped = True
        self.stop_calls += 1
        self.process.returncode = 0
        if os.path.exists(self.socket_path):
            os.unlink(self.socket_path)


class _FakeUriCache:
    def __init__(self) -> None:
        self.deleted: List[str] = []
        self._delete_fn = self._delete

    def _delete(self, uri: str, logger: logging.Logger) -> float:
        self.deleted.append(uri)
        return 0.0


def _make_manager(
    monkeypatch,
    short_tmp_dir,
    *,
    spawn_factory=None,
    uris_parser=None,
    custom_plugin_names=frozenset(),
    env_cache: Optional[Set[str]] = None,
    max_active: int = 32,
    max_idle: int = 0,
    idle_ttl_s: float = 60.0,
    max_restarts: int = 5,
    backoff_base_s: float = 0.01,
    monitor_interval_s: float = 0.02,
    healthy_uptime_s: float = 10.0,
    node_id: Optional[str] = "",
) -> Tuple[ForkServerManager, List[_FakeForkServer]]:
    created: List[_FakeForkServer] = []

    def _default_factory(**kwargs):
        srv = _FakeForkServer(**kwargs)
        created.append(srv)
        return srv

    factory = spawn_factory or _default_factory

    def _ctor(**kwargs):
        srv = factory(**kwargs)
        if srv not in created:
            created.append(srv)
        return srv

    monkeypatch.setenv("RAY_enable_worker_fork_server", "1")
    monkeypatch.setenv("RAY_max_active_fork_servers", str(max_active))
    monkeypatch.setenv("RAY_runtime_env_max_idle_fork_servers", str(max_idle))
    monkeypatch.setenv("RAY_runtime_env_fork_server_idle_ttl_s", str(idle_ttl_s))
    monkeypatch.setenv("RAY_fork_server_max_restarts", str(max_restarts))
    monkeypatch.setenv("RAY_fork_server_respawn_backoff_base_s", str(backoff_base_s))
    monkeypatch.setenv("RAY_fork_server_monitor_interval_s", str(monitor_interval_s))
    monkeypatch.setenv("RAY_fork_server_healthy_uptime_s", str(healthy_uptime_s))
    monkeypatch.setattr(
        "ray._private.runtime_env.agent.fork_server_manager.ForkServerProcess", _ctor
    )

    cache_set = env_cache if env_cache is not None else set()
    mgr = ForkServerManager(
        sockets_dir=str(short_tmp_dir / "sockets"),
        logs_dir=str(short_tmp_dir / "logs"),
        logger=logging.getLogger("test_fork_server_manager"),
        uris_parser=uris_parser or (lambda _env: []),
        pypath_plugins=(),
        custom_plugin_names=custom_plugin_names,
        env_cache_contains=lambda env: env in cache_set,
        workers_cgroup_path="/sys/fs/cgroup/ray/user/workers",
        node_id=node_id,
    )
    return mgr, created


def test_exec_bound_env_var_classification_and_expansion(monkeypatch):
    """Exec-bound vars go into the server key; non-exec-bound vars stay post-fork."""
    assert is_exec_bound_env_var("LD_PRELOAD") is True
    assert is_exec_bound_env_var("PYTHONHASHSEED") is True
    assert is_exec_bound_env_var("GLIBC_TUNABLES") is True
    assert is_exec_bound_env_var("_RJEM_MALLOC_CONF") is True
    assert is_exec_bound_env_var("GRPC_TRACE") is True

    assert is_exec_bound_env_var("PYTHONPATH") is False
    assert is_exec_bound_env_var("OMP_NUM_THREADS") is False
    assert is_exec_bound_env_var("OPENBLAS_NUM_THREADS") is False
    assert is_exec_bound_env_var("MKL_NUM_THREADS") is False
    assert is_exec_bound_env_var("RANK") is False

    monkeypatch.setenv("BASE_VAL", "hello")
    monkeypatch.setenv("MALLOC_CONF", "scrubbed")
    expanded = _expand_env_vars_snapshot(
        {
            "A": "${BASE_VAL}/world",
            "B": "$A/sub",
            "C": "${UNSET_UPPER_VAR}x",
            "D": "${MALLOC_CONF}y",
        }
    )
    assert expanded == {"A": "hello/world", "B": "hello/world/sub", "C": "x", "D": "y"}


def test_eligibility_and_multi_arg_py_executable_logging(
    monkeypatch, short_tmp_dir, caplog
):
    """Allowed keys, custom plugins, profilers, and multi-arg wrappers behave properly."""
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        mgr, _ = _make_manager(
            monkeypatch,
            short_tmp_dir,
            custom_plugin_names=frozenset({"my_custom_plugin"}),
        )
        try:
            ctx = RuntimeEnvContext()
            log = logging.getLogger("test_eligibility")

            # Eligible keys including excludes and worker_process_setup_hook.
            env_ok = RuntimeEnv(
                _validate=False,
                working_dir="gcs://pkg.zip",
                excludes=["*.tmp"],
                worker_process_setup_hook="my_mod.hook",
                env_vars={"RANK": "0"},
                config={"setup_timeout_seconds": 60},
            )
            assert mgr._should_use_fork_server(env_ok, ctx, log) is True

            # Custom plugin or profiler/container/override are bypassed.
            assert (
                mgr._should_use_fork_server(
                    RuntimeEnv(_validate=False, my_custom_plugin={"a": 1}), ctx, log
                )
                is False
            )
            assert (
                mgr._should_use_fork_server(
                    RuntimeEnv(_validate=False, nsight="default"), ctx, log
                )
                is False
            )
            assert (
                mgr._should_use_fork_server(
                    RuntimeEnv(_validate=False),
                    RuntimeEnvContext(override_worker_entrypoint="/worker.py"),
                    log,
                )
                is False
            )

            # Multi-arg py_executable logs once per serialized env and bypasses.
            wrapper_env = RuntimeEnv(_validate=False, py_executable="uv run python")
            serialized = wrapper_env.serialize()
            with caplog.at_level(logging.INFO, logger=log.name):
                sock1 = loop.run_until_complete(
                    mgr.resolve_socket(serialized, wrapper_env, ctx, log)
                )
                sock2 = loop.run_until_complete(
                    mgr.resolve_socket(serialized, wrapper_env, ctx, log)
                )
            assert sock1 is None and sock2 is None
            bypass_msgs = [
                r.message
                for r in caplog.records
                if "multi-argument wrapper command" in r.message
            ]
            assert len(bypass_msgs) == 1
        finally:
            mgr.shutdown()
    finally:
        asyncio.set_event_loop(None)
        loop.close()


def test_cache_key_excludes_normal_env_vars_and_includes_exec_bound(
    monkeypatch, short_tmp_dir
):
    """Non-exec-bound env_vars/config share fs_0 or one server; exec-bound vars split."""
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        mgr, created = _make_manager(monkeypatch, short_tmp_dir)
        try:
            log = logging.getLogger("test_keying")
            # Let default server finish readiness.
            loop.run_until_complete(asyncio.sleep(0.05))
            assert len(created) == 1
            default_sock = created[0].socket_path
            assert os.path.basename(default_sock) == "fs_0"

            # env_vars + config + worker_process_setup_hook only -> reuses fs_0!
            env_rank0 = RuntimeEnv(
                _validate=False,
                env_vars={"RANK": "0", "OMP_NUM_THREADS": "4"},
                config={"setup_timeout_seconds": 100},
                worker_process_setup_hook="mod.setup",
            )
            sock_rank0 = loop.run_until_complete(
                mgr.resolve_socket(
                    env_rank0.serialize(),
                    env_rank0,
                    RuntimeEnvContext(env_vars=dict(env_rank0.env_vars())),
                    log,
                )
            )
            assert sock_rank0 == default_sock
            assert len(created) == 1

            # preload_modules with differing non-exec env_vars share ONE per-env server.
            env_p0 = RuntimeEnv(
                _validate=False,
                preload_modules=["json"],
                env_vars={"RANK": "0"},
            )
            env_p1 = RuntimeEnv(
                _validate=False,
                preload_modules=["json"],
                env_vars={"RANK": "1"},
            )
            mgr.on_raylet_ref_added(env_p0.serialize())
            mgr.on_raylet_ref_added(env_p1.serialize())
            sock_p0 = loop.run_until_complete(
                mgr.resolve_socket(
                    env_p0.serialize(),
                    env_p0,
                    RuntimeEnvContext(env_vars={"RANK": "0"}),
                    log,
                )
            )
            sock_p1 = loop.run_until_complete(
                mgr.resolve_socket(
                    env_p1.serialize(),
                    env_p1,
                    RuntimeEnvContext(env_vars={"RANK": "1"}),
                    log,
                )
            )
            assert sock_p0 is not None
            assert sock_p0 == sock_p1
            assert sock_p0 != default_sock
            assert len(created) == 2

            # Exec-bound env var (LD_PRELOAD) gets its own distinct fork server.
            env_ld = RuntimeEnv(
                _validate=False,
                preload_modules=["json"],
                env_vars={"LD_PRELOAD": "/tmp/libfoo.so"},
            )
            mgr.on_raylet_ref_added(env_ld.serialize())
            sock_ld = loop.run_until_complete(
                mgr.resolve_socket(
                    env_ld.serialize(),
                    env_ld,
                    RuntimeEnvContext(env_vars={"LD_PRELOAD": "/tmp/libfoo.so"}),
                    log,
                )
            )
            assert sock_ld is not None
            assert sock_ld != sock_p0
            assert len(created) == 3
            assert created[2].context.env_vars == {"LD_PRELOAD": "/tmp/libfoo.so"}

            # Removing only p0 keeps shared server alive; removing p1 stops it.
            mgr.on_raylet_ref_removed(env_p0.serialize())
            assert created[1].is_alive() is True
            mgr.on_raylet_ref_removed(env_p1.serialize())
            assert created[1].is_alive() is False
        finally:
            mgr.shutdown()
    finally:
        asyncio.set_event_loop(None)
        loop.close()


def test_readiness_clamping_max_active_cap_and_lazy_respawn(monkeypatch, short_tmp_dir):
    """Covers setup_timeout clamping, max_active_fork_servers cap, cwd/crash respawn, and URICache hook."""
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        mgr, created = _make_manager(
            monkeypatch,
            short_tmp_dir,
            uris_parser=lambda env: (
                [(env["working_dir"], "working_dir")] if env.get("working_dir") else []
            ),
            max_active=2,  # 1 slot for fs_0 + 1 slot for a per-env server.
        )
        try:
            log = logging.getLogger("test_lifecycle")
            loop.run_until_complete(asyncio.sleep(0.05))
            assert len(created) == 1  # fs_0

            # 1. Tight setup_deadline skips cold start cleanly without crashing.
            env_slow = RuntimeEnv(_validate=False, preload_modules=["slow_mod"])
            mgr.on_raylet_ref_added(env_slow.serialize())
            expired_deadline = time.monotonic() - 0.1
            sock_none = loop.run_until_complete(
                mgr.resolve_socket(
                    env_slow.serialize(),
                    env_slow,
                    RuntimeEnvContext(),
                    log,
                    setup_deadline=expired_deadline,
                )
            )
            assert sock_none is None
            assert len(created) == 1
            mgr.on_raylet_ref_removed(env_slow.serialize())

            # 2. Per-env server fills the 2nd slot; a 3rd distinct env hits max_active cap.
            env_a = RuntimeEnv(_validate=False, working_dir="gcs://a.zip")
            env_b = RuntimeEnv(_validate=False, working_dir="gcs://b.zip")
            mgr.on_raylet_ref_added(env_a.serialize())
            mgr.on_raylet_ref_added(env_b.serialize())
            sock_a = loop.run_until_complete(
                mgr.resolve_socket(env_a.serialize(), env_a, RuntimeEnvContext(), log)
            )
            assert sock_a is not None
            assert len(created) == 2

            sock_b = loop.run_until_complete(
                mgr.resolve_socket(env_b.serialize(), env_b, RuntimeEnvContext(), log)
            )
            assert sock_b is None
            assert len(created) == 2

            # 3. Stale working_dir triggers atomic lazy respawn with the same socket path.
            created[1]._valid_cwd = False
            sock_a2 = loop.run_until_complete(
                mgr.resolve_socket(env_a.serialize(), env_a, RuntimeEnvContext(), log)
            )
            assert sock_a2 == sock_a
            assert created[1]._stopped is True
            assert len(created) == 3
            assert created[2].is_alive() is True

            # 4. URICache eviction stops servers holding that URI before deleting it.
            fake_cache = _FakeUriCache()
            mgr.wrap_uri_cache(fake_cache)
            fake_cache._delete_fn("gcs://a.zip", log)
            assert created[2]._stopped is True
            assert fake_cache.deleted == ["gcs://a.zip"]
        finally:
            mgr.shutdown()
    finally:
        asyncio.set_event_loop(None)
        loop.close()


def test_default_server_crash_backoff_and_max_restarts(monkeypatch, short_tmp_dir):
    """Default fork server respawns on unexpected exit and stops after max_restarts."""
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        attempts = 0

        def _factory(**kwargs):
            nonlocal attempts
            attempts += 1
            return _FakeForkServer(
                fail_ready=RuntimeError(f"boom {attempts}"), **kwargs
            )

        mgr, created = _make_manager(
            monkeypatch,
            short_tmp_dir,
            spawn_factory=_factory,
            max_restarts=3,
            backoff_base_s=0.01,
            monitor_interval_s=0.01,
        )
        try:
            assert mgr._default_monitor_task is not None
            loop.run_until_complete(
                asyncio.wait_for(mgr._default_monitor_task, timeout=2.0)
            )
            assert attempts == 3
            assert mgr._failures[None] == 3
            assert mgr._default_server is None
            assert all(s._stopped for s in created)
        finally:
            mgr.shutdown()
    finally:
        asyncio.set_event_loop(None)
        loop.close()


def test_idle_ttl_and_fifo_eviction(monkeypatch, short_tmp_dir):
    """When max_idle > 0, idle servers persist until TTL or FIFO eviction."""
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        mgr, created = _make_manager(
            monkeypatch, short_tmp_dir, max_idle=1, idle_ttl_s=0.15
        )
        try:
            log = logging.getLogger("test_idle")
            loop.run_until_complete(asyncio.sleep(0.05))

            env_a = RuntimeEnv(_validate=False, preload_modules=["mod_a"])
            env_b = RuntimeEnv(_validate=False, preload_modules=["mod_b"])
            mgr.on_raylet_ref_added(env_a.serialize())
            sock_a = loop.run_until_complete(
                mgr.resolve_socket(env_a.serialize(), env_a, RuntimeEnvContext(), log)
            )
            assert sock_a is not None
            srv_a = created[-1]

            # Dropping ref keeps srv_a alive in idle pool (max_idle=1).
            mgr.on_raylet_ref_removed(env_a.serialize())
            assert srv_a.is_alive() is True

            # A second idle env evicts srv_a (FIFO cap=1), then expires after TTL.
            mgr.on_raylet_ref_added(env_b.serialize())
            sock_b = loop.run_until_complete(
                mgr.resolve_socket(env_b.serialize(), env_b, RuntimeEnvContext(), log)
            )
            assert sock_b is not None
            srv_b = created[-1]
            mgr.on_raylet_ref_removed(env_b.serialize())
            assert srv_a.is_alive() is False
            assert srv_b.is_alive() is True

            loop.run_until_complete(asyncio.sleep(0.25))
            assert srv_b.is_alive() is False
        finally:
            mgr.shutdown()
    finally:
        asyncio.set_event_loop(None)
        loop.close()


def test_runtime_env_agent_source_process_gating_and_cache_hit(
    monkeypatch, short_tmp_dir
):
    """Only source_process='raylet' spawns/gets fork servers; eager_install caches base context."""
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    created: List[_FakeForkServer] = []

    def _ctor(**kwargs):
        srv = _FakeForkServer(**kwargs)
        created.append(srv)
        return srv

    monkeypatch.setenv("RAY_enable_worker_fork_server", "1")
    monkeypatch.setattr(
        "ray._private.runtime_env.agent.fork_server_manager.ForkServerProcess", _ctor
    )
    monkeypatch.setattr("ray.util.get_node_ip_address", lambda: "127.0.0.1")

    try:
        (short_tmp_dir / "logs").mkdir()
        agent = RuntimeEnvAgent(
            runtime_env_dir=str(short_tmp_dir / "runtime_resources"),
            logging_params={
                "logging_level": "INFO",
                "logging_format": "%(message)s",
                "log_dir": str(short_tmp_dir / "logs"),
                "filename": "runtime_env_agent.log",
                "max_bytes": 1024 * 1024,
                "backup_count": 1,
            },
            gcs_client=SimpleNamespace(),
            temp_dir=str(short_tmp_dir / "tmp"),
            address="127.0.0.1",
            runtime_env_agent_port=12345,
            session_dir=str(short_tmp_dir / "session"),
            node_id="deadbeef1234567890",
        )
        try:
            loop.run_until_complete(asyncio.sleep(0.05))
            assert len(created) == 1  # node-scoped default server only
            assert os.path.basename(created[0].socket_path) == "fs_deadbeef_0"

            env_str = RuntimeEnv(_validate=False, preload_modules=["json"]).serialize()

            # 1. raylet_eager_install populates _env_cache without starting a per-env server.
            eager_req = runtime_env_agent_pb2.GetOrCreateRuntimeEnvRequest(
                serialized_runtime_env=env_str,
                job_id=b"01000000",
                source_process="raylet_eager_install",
            )
            eager_reply = loop.run_until_complete(
                agent.GetOrCreateRuntimeEnv(eager_req)
            )
            assert eager_reply.status == runtime_env_agent_pb2.AGENT_RPC_STATUS_OK
            eager_ctx = RuntimeEnvContext.deserialize(
                eager_reply.serialized_runtime_env_context
            )
            assert eager_ctx.fork_server_socket_path is None
            assert len(created) == 1

            # 2. client_server also gets base context without spawning a fork server.
            client_req = runtime_env_agent_pb2.GetOrCreateRuntimeEnvRequest(
                serialized_runtime_env=env_str,
                job_id=b"01000000",
                source_process="client_server",
            )
            client_reply = loop.run_until_complete(
                agent.GetOrCreateRuntimeEnv(client_req)
            )
            assert client_reply.status == runtime_env_agent_pb2.AGENT_RPC_STATUS_OK
            client_ctx = RuntimeEnvContext.deserialize(
                client_reply.serialized_runtime_env_context
            )
            assert client_ctx.fork_server_socket_path is None
            assert len(created) == 1

            # 3. raylet worker request hits _env_cache and resolves a per-env fork server.
            worker_req = runtime_env_agent_pb2.GetOrCreateRuntimeEnvRequest(
                serialized_runtime_env=env_str,
                job_id=b"01000000",
                source_process="raylet",
            )
            worker_reply = loop.run_until_complete(
                agent.GetOrCreateRuntimeEnv(worker_req)
            )
            assert worker_reply.status == runtime_env_agent_pb2.AGENT_RPC_STATUS_OK
            worker_ctx = RuntimeEnvContext.deserialize(
                worker_reply.serialized_runtime_env_context
            )
            assert worker_ctx.fork_server_socket_path is not None
            assert len(created) == 2
            per_env_srv = created[1]
            assert per_env_srv.is_alive() is True

            # 4. DeleteRuntimeEnvIfPossible from raylet stops the per-env fork server
            # even while raylet_eager_install still holds a reference in ReferenceTable.
            del_req = runtime_env_agent_pb2.DeleteRuntimeEnvIfPossibleRequest(
                serialized_runtime_env=env_str,
                source_process="raylet",
            )
            del_reply = loop.run_until_complete(
                agent.DeleteRuntimeEnvIfPossible(del_req)
            )
            assert del_reply.status == runtime_env_agent_pb2.AGENT_RPC_STATUS_OK
            assert per_env_srv.is_alive() is False
        finally:
            agent._fork_server_manager.shutdown()
    finally:
        asyncio.set_event_loop(None)
        loop.close()


def test_resolve_socket_fallbacks_cleanly_on_plugin_error_and_phantom_idle(
    monkeypatch, short_tmp_dir
):
    """Plugin errors fall back to execvpe; skipped starts never register phantom idle timers."""
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        mgr, created = _make_manager(
            monkeypatch, short_tmp_dir, max_idle=1, idle_ttl_s=60.0
        )
        try:
            log = logging.getLogger("test_fallback")
            loop.run_until_complete(asyncio.sleep(0.05))

            # Expired deadline before server start must not register a phantom idle timer.
            env_slow = RuntimeEnv(_validate=False, preload_modules=["slow_mod"])
            sock = loop.run_until_complete(
                mgr.resolve_socket(
                    env_slow.serialize(),
                    env_slow,
                    RuntimeEnvContext(),
                    log,
                    setup_deadline=time.monotonic() - 1.0,
                )
            )
            assert sock is None
            assert mgr._idle_timers == {}

            # Unexpected error in uris_parser must be caught and return None, not crash.
            monkeypatch.setattr(
                mgr, "_uris_parser", lambda _env: (_ for _ in ()).throw(ValueError("x"))
            )
            sock_err = loop.run_until_complete(
                mgr.resolve_socket(
                    env_slow.serialize(), env_slow, RuntimeEnvContext(), log
                )
            )
            assert sock_err is None
            assert len(created) == 1
        finally:
            mgr.shutdown()
    finally:
        asyncio.set_event_loop(None)
        loop.close()


def test_per_node_socket_and_log_isolation(monkeypatch, short_tmp_dir):
    """Two nodes sharing a session sockets dir get distinct default and per-env paths."""
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        monkeypatch.setenv("RAY_NODE_ID", "11223344aabbccdd")
        mgr_env, created_env = _make_manager(monkeypatch, short_tmp_dir, node_id=None)
        mgr_arg, created_arg = _make_manager(
            monkeypatch, short_tmp_dir, node_id="55667788eeff0011"
        )
        try:
            log = logging.getLogger("test_node_isolation")
            loop.run_until_complete(asyncio.sleep(0.05))
            assert os.path.basename(created_env[0].socket_path) == "fs_11223344_0"
            assert os.path.basename(created_arg[0].socket_path) == "fs_55667788_0"
            assert (
                os.path.basename(created_env[0].setup_log_path)
                == "fork_server_default_11223344.log"
            )
            assert (
                os.path.basename(created_arg[0].setup_log_path)
                == "fork_server_default_55667788.log"
            )

            env = RuntimeEnv(_validate=False, preload_modules=["json"])
            serialized = env.serialize()
            mgr_env.on_raylet_ref_added(serialized)
            mgr_arg.on_raylet_ref_added(serialized)
            sock_a = loop.run_until_complete(
                mgr_env.resolve_socket(serialized, env, RuntimeEnvContext(), log)
            )
            sock_b = loop.run_until_complete(
                mgr_arg.resolve_socket(serialized, env, RuntimeEnvContext(), log)
            )
            assert sock_a is not None and sock_b is not None
            assert sock_a != sock_b
        finally:
            mgr_env.shutdown()
            mgr_arg.shutdown()
    finally:
        asyncio.set_event_loop(None)
        loop.close()


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
