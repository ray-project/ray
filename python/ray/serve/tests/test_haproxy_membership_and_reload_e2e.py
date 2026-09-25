"""End-to-end tests for HAProxy membership updates and reloads (real HAProxy).

Covers two independently gated modes:
- RAY_SERVE_HAPROXY_DYNAMIC_SERVERS_ENABLED: replica membership changes go
  through the Runtime API and must not reload HAProxy.
- RAY_SERVE_HAPROXY_MASTER_WORKER_ENABLED: reloads are performed by a
  long-lived HAProxy master instead of spawning a new process.

Membership tests run under both process models. HAProxy runs against fake
replicas, plus one full Serve app.
"""
import asyncio
import os
import subprocess
import sys
import tempfile
import threading
import time
from collections import Counter
from typing import Dict, List, Optional, Set

import pytest
import pytest_asyncio
import requests
import uvicorn
from fastapi import FastAPI, Request, Response

import ray
from ray import serve
from ray._common.network_utils import find_free_port
from ray._common.test_utils import async_wait_for_condition, wait_for_condition
from ray.serve._private.common import RequestProtocol
from ray.serve._private.constants import (
    RAY_SERVE_ENABLE_HA_PROXY,
    SERVE_INGRESS_ROUTER_HEADER_PREFIX,
)
from ray.serve._private.haproxy import (
    BackendConfig,
    HAProxyApi,
    HAProxyConfig,
    HAProxyManager,
    ServerConfig,
)
from ray.serve._private.haproxy_server_slots import SlotState, slot_name
from ray.serve.config import HTTPOptions

pytestmark = pytest.mark.skipif(
    not RAY_SERVE_ENABLE_HA_PROXY,
    reason="RAY_SERVE_ENABLE_HA_PROXY not set.",
)

_ROUTER_HEADER = SERVE_INGRESS_ROUTER_HEADER_PREFIX + "trace"


def _haproxy_pids() -> Set[int]:
    result = subprocess.run(
        ["pgrep", "-x", "haproxy"], capture_output=True, text=True, check=False
    )
    return {int(pid) for pid in result.stdout.split()}


@pytest.fixture(autouse=True)
def clean_up_haproxy_processes():
    subprocess.run(["pkill", "-x", "haproxy"], check=False)
    yield
    subprocess.run(["pkill", "-x", "haproxy"], check=False)


class _FakeHTTPServer:
    def _serve(self, app: FastAPI, ready_check) -> None:
        self._server = uvicorn.Server(
            uvicorn.Config(app, host="127.0.0.1", port=self.port, log_level="error")
        )
        self._thread = threading.Thread(
            target=lambda: asyncio.run(self._server.serve()), daemon=True
        )
        self._thread.start()
        wait_for_condition(ready_check)

    def stop(self) -> None:
        self._server.should_exit = True
        self._thread.join(timeout=5)


class FakeReplica(_FakeHTTPServer):
    """HTTP server standing in for a replica's direct-ingress port."""

    def __init__(self, label: str):
        self.label = label
        self.port = find_free_port()
        self.replica_id = f"SERVE_REPLICA::app#dep#{label}"
        app = FastAPI()

        @app.get("/-/healthz")
        async def healthz():
            return "success"

        @app.api_route("/{path:path}", methods=["GET", "POST"])
        async def handle(path: str, request: Request, response: Response):
            delay_s = float(request.query_params.get("sleep", "0"))
            if delay_s:
                await asyncio.sleep(delay_s)
            response.headers["x-replica"] = label
            router_header = request.headers.get(_ROUTER_HEADER)
            if router_header is not None:
                response.headers["x-router-header"] = router_header
            return {"replica": label}

        self._serve(
            app,
            lambda: requests.get(
                f"http://127.0.0.1:{self.port}/-/healthz", timeout=2
            ).ok,
        )

    def server(self) -> ServerConfig:
        return ServerConfig(
            name=HAProxyManager.get_safe_name(self.replica_id),
            host="127.0.0.1",
            port=self.port,
            replica_id=self.replica_id,
        )


class FakeRouter(_FakeHTTPServer):
    """Fake /internal/route that pins every request to `self.target`."""

    def __init__(self):
        self.port = find_free_port()
        self.target: Optional[str] = None
        app = FastAPI()

        @app.post("/internal/route")
        async def route():
            return {
                "replica_id": self.target,
                "request_headers": {_ROUTER_HEADER: "from-router"},
            }

        self._serve(
            app,
            lambda: requests.post(
                f"http://127.0.0.1:{self.port}/internal/route", timeout=2
            ).ok,
        )


@pytest.fixture
def fakes():
    started = []

    def make(kind, *args):
        fake = kind(*args)
        started.append(fake)
        return fake

    yield make
    for fake in started:
        fake.stop()


class RunningHAProxy:
    def __init__(self, temp_dir: str, **cfg_overrides):
        self.port = find_free_port()
        cfg = dict(
            http_options=HTTPOptions(
                host="127.0.0.1", port=self.port, keep_alive_timeout_s=58
            ),
            stats_port=find_free_port(),
            metrics_port=find_free_port(),
            socket_path=os.path.join(temp_dir, "admin.sock"),
            server_state_base=temp_dir,
            server_state_file=os.path.join(temp_dir, "server-state"),
            dynamic_servers_enabled=True,
            master_worker_enabled=False,
            has_received_routes=True,
            has_received_servers=True,
            health_check_inter="500ms",
            health_check_rise=1,
            health_check_fall=2,
        )
        cfg.update(cfg_overrides)
        self.api = HAProxyApi(
            cfg=HAProxyConfig(**cfg),
            config_file_path=os.path.join(temp_dir, "haproxy.cfg"),
        )

    @property
    def master_worker(self) -> bool:
        return self.api.cfg.master_worker_enabled

    def url(self, path: str = "/app") -> str:
        return f"http://127.0.0.1:{self.port}{path}"

    async def start(self, backends: List[BackendConfig]) -> None:
        self.api.set_backend_configs({b.name: b for b in backends})
        await self.api.start()

    async def apply(self, backends: List[BackendConfig]) -> None:
        self.api.set_backend_configs({b.name: b for b in backends})
        await self.api.apply()

    async def stop(self) -> None:
        await self.api.stop()

    @property
    def serving_pid(self) -> int:
        """Pid of the process serving the current config; changes on reload."""
        if self.master_worker:
            return self.api._worker_pid
        return self.api._proc.pid

    def steady_state_pids(self) -> Set[int]:
        """Processes expected once no old workers remain."""
        return {self.api._proc.pid, self.serving_pid}


def _backend(
    replicas: List[FakeReplica],
    *,
    name: str = "http-app",
    path_prefix: str = "/app",
    router: Optional[FakeRouter] = None,
    fallback: Optional[FakeReplica] = None,
) -> BackendConfig:
    return BackendConfig(
        name=name,
        path_prefix=path_prefix,
        app_name=name,
        servers=[replica.server() for replica in replicas],
        ingress_request_router_servers=(
            [ServerConfig(name="router", host="127.0.0.1", port=router.port)]
            if router
            else []
        ),
        fallback_server=(
            ServerConfig(name="fallback", host="127.0.0.1", port=fallback.port)
            if fallback
            else None
        ),
        protocol=RequestProtocol.HTTP,
    )


def _labels(url: str, n: int = 30, method: str = "GET") -> Counter:
    labels = Counter()
    for _ in range(n):
        response = requests.request(method, url, json={}, timeout=5)
        labels[
            response.headers.get("x-replica") if response.ok else response.status_code
        ] += 1
    return labels


async def _wait_for_labels(url: str, expected: set, timeout: float = 15, **kwargs):
    async def _check():
        return set(_labels(url, **kwargs)) == expected

    await async_wait_for_condition(_check, timeout=timeout, retry_interval_ms=200)


def _in_background(fn) -> Dict[str, object]:
    """Run `fn` in a thread; returns a dict filled with its result or error."""
    result: Dict[str, object] = {}

    def _run():
        try:
            result["value"] = fn()
        except Exception as e:  # noqa: BLE001
            result["error"] = repr(e)

    result["thread"] = threading.Thread(target=_run)
    result["thread"].start()
    return result


@pytest_asyncio.fixture
async def haproxy_factory():
    started: List[RunningHAProxy] = []
    with tempfile.TemporaryDirectory(dir="/tmp") as temp_dir:

        def make(**cfg_overrides) -> RunningHAProxy:
            proxy = RunningHAProxy(temp_dir, **cfg_overrides)
            started.append(proxy)
            return proxy

        yield make
        for proxy in started:
            await proxy.stop()


@pytest.fixture(params=[False, True], ids=["standalone", "master_worker"])
def haproxy(request, haproxy_factory):
    """Dynamic-servers HAProxy under each process model."""

    def make(**cfg_overrides) -> RunningHAProxy:
        cfg_overrides.setdefault("master_worker_enabled", request.param)
        return haproxy_factory(**cfg_overrides)

    return make


# ---------------------------------------------------------------------------
# Dynamic servers.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_scale_up_and_down_without_reload(haproxy, fakes):
    a, b = fakes(FakeReplica, "A"), fakes(FakeReplica, "B")
    proxy = haproxy()
    await proxy.start([_backend([a])])
    await _wait_for_labels(proxy.url(), {"A"})
    pid = proxy.serving_pid

    await proxy.apply([_backend([a, b])])
    await _wait_for_labels(proxy.url(), {"A", "B"})

    await proxy.apply([_backend([b])])
    # The removed replica is no longer reachable: no soft-stopping worker with a
    # frozen backend view exists to keep routing to it.
    await _wait_for_labels(proxy.url(), {"B"})
    assert set(_labels(proxy.url(), n=50)) == {"B"}

    assert proxy.serving_pid == pid
    assert await proxy.api._get_running_pid() == pid
    assert not proxy.api.has_alive_old_procs()
    assert _haproxy_pids() == proxy.steady_state_pids()
    assert proxy.api.count_haproxy_processes() == 1


@pytest.mark.asyncio
async def test_request_longer_than_hard_stop_survives_membership_changes(
    haproxy, fakes
):
    """The failure from the issue: a request on a reused keep-alive connection
    must not be cut at hard-stop-after when replicas change underneath it."""
    a, b, c = (fakes(FakeReplica, label) for label in "ABC")
    proxy = haproxy(hard_stop_after_s=2)
    await proxy.start([_backend([a])])
    await _wait_for_labels(proxy.url(), {"A"})

    session = requests.Session()
    assert session.get(proxy.url(), timeout=5).ok  # Establish keep-alive.

    await proxy.apply([_backend([a, b])])

    slow = _in_background(
        lambda: session.get(proxy.url() + "?sleep=5", timeout=30).status_code
    )
    time.sleep(0.5)
    # More churn while the request is in flight, past hard-stop-after.
    await proxy.apply([_backend([a, b, c])])
    await asyncio.sleep(2)
    await proxy.apply([_backend([a, c])])
    slow["thread"].join(timeout=30)

    assert slow.get("value") == 200, slow
    assert not proxy.api.has_alive_old_procs()
    assert _haproxy_pids() == proxy.steady_state_pids()


@pytest.mark.asyncio
async def test_draining_slot_waits_for_in_flight_request(haproxy, fakes):
    a, b, c, d = (fakes(FakeReplica, label) for label in "ABCD")
    proxy = haproxy(min_server_slots=4)
    await proxy.start([_backend([a])])
    await _wait_for_labels(proxy.url(), {"A"})
    pid = proxy.serving_pid

    slow = _in_background(
        lambda: requests.get(proxy.url() + "?sleep=4", timeout=30).headers.get(
            "x-replica"
        )
    )
    time.sleep(0.5)

    # Remove A while its request is in flight and add B, then C: neither may
    # take A's slot while it still carries the request.
    await proxy.apply([_backend([b])])
    table = proxy.api._slot_tables["http-app"]
    assert table.slot_by_name(slot_name(1)).state is SlotState.DRAINING
    await proxy.apply([_backend([b, c])])
    assert table.slot_by_name(slot_name(1)).state is SlotState.DRAINING
    assert table.slot_by_name(slot_name(3)).server.name == c.server().name
    await _wait_for_labels(proxy.url(), {"B", "C"})

    slow["thread"].join(timeout=30)
    # Maintenance does not cut the in-flight request.
    assert slow.get("value") == "A", slow

    # The slot is released only once HAProxy reports no sessions or pooled
    # connections left on it (a connection lingers briefly after the response).
    async def _slot_released():
        await proxy.apply([_backend([b, c])])
        return table.slot_by_name(slot_name(1)).state is SlotState.FREE

    await async_wait_for_condition(_slot_released, timeout=30, retry_interval_ms=1000)

    # The released slot is the lowest free one, so the next replica reuses it.
    await proxy.apply([_backend([b, c, d])])
    assert table.slot_by_name(slot_name(1)).server.name == d.server().name
    await _wait_for_labels(proxy.url(), {"B", "C", "D"})
    assert proxy.serving_pid == pid


@pytest.mark.asyncio
async def test_capacity_growth_reloads_once_and_keeps_assignments(haproxy, fakes):
    a, b = fakes(FakeReplica, "A"), fakes(FakeReplica, "B")
    proxy = haproxy(min_server_slots=1)
    await proxy.start([_backend([a])])
    await _wait_for_labels(proxy.url(), {"A"})
    pid = proxy.serving_pid
    master_pid = proxy.api._proc.pid

    await proxy.apply([_backend([a, b])])

    assert proxy.serving_pid != pid
    if proxy.master_worker:
        assert proxy.api._proc.pid == master_pid
    assert proxy.api._slot_tables["http-app"].capacity == 2
    await _wait_for_labels(proxy.url(), {"A", "B"})

    # Within the new capacity, membership changes no longer reload.
    grown_pid = proxy.serving_pid
    await proxy.apply([_backend([b])])
    await _wait_for_labels(proxy.url(), {"B"})
    assert proxy.serving_pid == grown_pid


@pytest.mark.asyncio
@pytest.mark.parametrize("state_file", [True, False])
async def test_config_reload_keeps_replicas_routable(haproxy, fakes, state_file):
    a = fakes(FakeReplica, "A")
    other = fakes(FakeReplica, "OTHER")
    proxy = haproxy(enable_hap_optimization=state_file)
    await proxy.start([_backend([a])])
    await _wait_for_labels(proxy.url(), {"A"})
    pid = proxy.serving_pid

    # A route change is a real config change and reloads.
    await proxy.apply(
        [_backend([a]), _backend([other], name="http-other", path_prefix="/other")]
    )

    assert proxy.serving_pid != pid
    # Assignments are in place as soon as apply() returns.
    assert set(_labels(proxy.url(), n=10)) == {"A"}
    assert set(_labels(proxy.url("/other"), n=10)) == {"OTHER"}


@pytest.mark.asyncio
async def test_ingress_router_pins_to_replicas_added_at_runtime(haproxy, fakes):
    a, b = fakes(FakeReplica, "A"), fakes(FakeReplica, "B")
    fallback = fakes(FakeReplica, "FALLBACK")
    router = fakes(FakeRouter)
    router.target = b.replica_id
    proxy = haproxy()

    await proxy.start([_backend([a], router=router, fallback=fallback)])
    await _wait_for_labels(proxy.url(), {"A"})
    pid = proxy.serving_pid

    # B is unknown to HAProxy: a pin-miss goes to the fallback proxy, without
    # the router's headers.
    await _wait_for_labels(proxy.url(), {"FALLBACK"}, method="POST")
    response = requests.post(proxy.url(), json={}, timeout=5)
    assert "x-router-header" not in response.headers

    # Adding B at runtime makes the pin resolve, with the router's headers.
    await proxy.apply([_backend([a, b], router=router, fallback=fallback)])
    await _wait_for_labels(proxy.url(), {"B"}, method="POST")
    response = requests.post(proxy.url(), json={}, timeout=5)
    assert response.headers.get("x-router-header") == "from-router"

    # Removing B turns it back into a pin-miss.
    await proxy.apply([_backend([a], router=router, fallback=fallback)])
    await _wait_for_labels(proxy.url(), {"FALLBACK"}, method="POST")

    assert proxy.serving_pid == pid


@pytest.mark.asyncio
async def test_router_app_without_replicas_uses_fallback(haproxy, fakes):
    fallback = fakes(FakeReplica, "FALLBACK")
    router = fakes(FakeRouter)
    proxy = haproxy()

    await proxy.start([_backend([], router=router, fallback=fallback)])

    # Like a static config, the router is not consulted until the app has
    # replicas; requests take the primary backend's fallback server.
    await _wait_for_labels(proxy.url(), {"FALLBACK"}, method="POST")


@pytest.mark.asyncio
async def test_router_app_without_replicas_keeps_its_longer_prefix(haproxy, fakes):
    """A nested router app with no replicas must not let its parent app's
    router claim and pin its requests."""
    parent_replica = fakes(FakeReplica, "PARENT")
    child_fallback = fakes(FakeReplica, "CHILD_FALLBACK")
    parent_router = fakes(FakeRouter)
    parent_router.target = parent_replica.replica_id
    child_router = fakes(FakeRouter)
    proxy = haproxy()

    await proxy.start(
        [
            _backend([parent_replica], name="http-parent", router=parent_router),
            _backend(
                [],
                name="http-child",
                path_prefix="/app/child",
                router=child_router,
                fallback=child_fallback,
            ),
        ]
    )
    await _wait_for_labels(proxy.url("/app"), {"PARENT"}, method="POST")

    await _wait_for_labels(proxy.url("/app/child/x"), {"CHILD_FALLBACK"}, method="POST")


# ---------------------------------------------------------------------------
# Master-worker process model (with a static config).
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_master_worker_reloads_keep_master_and_replace_worker(
    haproxy_factory, fakes
):
    a = fakes(FakeReplica, "A")
    proxy = haproxy_factory(master_worker_enabled=True, dynamic_servers_enabled=False)
    await proxy.start([_backend([a])])
    await _wait_for_labels(proxy.url(), {"A"})
    master_pid = proxy.api._proc.pid

    seen_workers = {proxy.serving_pid}
    for _ in range(3):
        await proxy.api.reload()
        assert proxy.api._proc.pid == master_pid
        assert proxy.serving_pid not in seen_workers
        seen_workers.add(proxy.serving_pid)
        assert await proxy.api._get_running_pid() == proxy.serving_pid
        assert set(_labels(proxy.url(), n=10)) == {"A"}

    # Idle old workers exit on their own; the master stays.
    wait_for_condition(lambda: not proxy.api.has_alive_old_procs(), timeout=20)
    assert _haproxy_pids() == proxy.steady_state_pids()
    assert proxy.api.count_haproxy_processes() == 1


@pytest.mark.asyncio
async def test_master_worker_old_worker_finishes_request_and_blocks_drain(
    haproxy_factory, fakes
):
    a = fakes(FakeReplica, "A")
    proxy = haproxy_factory(master_worker_enabled=True, dynamic_servers_enabled=False)
    await proxy.start([_backend([a])])
    await _wait_for_labels(proxy.url(), {"A"})
    old_worker = proxy.serving_pid

    slow = _in_background(
        lambda: requests.get(proxy.url() + "?sleep=4", timeout=30).status_code
    )
    time.sleep(0.5)
    await proxy.api.reload()

    # The old worker holds the in-flight request, so the proxy is not drained.
    assert proxy.api._old_worker_pids == [old_worker]
    assert proxy.api.has_alive_old_procs()

    slow["thread"].join(timeout=30)
    assert slow.get("value") == 200, slow
    wait_for_condition(lambda: not proxy.api.has_alive_old_procs(), timeout=20)


@pytest.mark.asyncio
async def test_master_worker_failed_reload_keeps_current_worker(haproxy_factory, fakes):
    a = fakes(FakeReplica, "A")
    proxy = haproxy_factory(master_worker_enabled=True, dynamic_servers_enabled=False)
    await proxy.start([_backend([a])])
    await _wait_for_labels(proxy.url(), {"A"})
    master_pid, worker_pid = proxy.api._proc.pid, proxy.serving_pid

    proxy.api.cfg.balance_algorithm = "not-an-algorithm"
    with pytest.raises(RuntimeError):
        await proxy.api.reload()

    # The master keeps the previous worker serving the previous config.
    assert proxy.api._proc.returncode is None
    assert proxy.api._proc.pid == master_pid
    assert proxy.serving_pid == worker_pid
    assert await proxy.api._get_running_pid() == worker_pid
    assert set(_labels(proxy.url(), n=10)) == {"A"}

    # A corrected config reloads normally.
    proxy.api.cfg.balance_algorithm = "leastconn"
    await proxy.api.reload()
    assert proxy.serving_pid != worker_pid
    assert set(_labels(proxy.url(), n=10)) == {"A"}


@pytest.mark.asyncio
async def test_master_worker_stop_leaves_no_processes(haproxy_factory, fakes):
    a = fakes(FakeReplica, "A")
    proxy = haproxy_factory(master_worker_enabled=True, dynamic_servers_enabled=False)
    await proxy.start([_backend([a])])
    await _wait_for_labels(proxy.url(), {"A"})
    await proxy.api.reload()

    await proxy.stop()

    wait_for_condition(lambda: _haproxy_pids() == set(), timeout=10)
    assert proxy.api._proc is None


# ---------------------------------------------------------------------------
# Full Serve app.
# ---------------------------------------------------------------------------


@pytest.fixture(params=["0", "1"], ids=["standalone", "master_worker"])
def dynamic_servers_cluster(request, monkeypatch):
    monkeypatch.setenv("RAY_SERVE_HAPROXY_DYNAMIC_SERVERS_ENABLED", "1")
    monkeypatch.setenv("RAY_SERVE_HAPROXY_MASTER_WORKER_ENABLED", request.param)
    ray.init(num_cpus=8)
    yield request.param == "1"
    serve.shutdown()
    ray.shutdown()


def test_serve_replica_scaling_does_not_reload_haproxy(dynamic_servers_cluster):
    master_worker = dynamic_servers_cluster

    @serve.deployment(num_replicas=1)
    class Echo:
        def __call__(self) -> str:
            return serve.get_replica_context().replica_id.unique_id

    def replica_ids(n: int = 60) -> set:
        return {
            requests.get("http://127.0.0.1:8000/echo", timeout=5).text for _ in range(n)
        }

    serve.run(Echo.bind(), route_prefix="/echo")
    wait_for_condition(lambda: len(replica_ids(10)) == 1, timeout=60)
    # One HAProxy process per node, plus the master in master-worker mode.
    wait_for_condition(
        lambda: len(_haproxy_pids()) == (2 if master_worker else 1), timeout=30
    )
    pids = _haproxy_pids()

    serve.run(Echo.options(num_replicas=3).bind(), route_prefix="/echo")
    wait_for_condition(lambda: len(replica_ids()) == 3, timeout=60)
    assert _haproxy_pids() == pids

    serve.run(Echo.options(num_replicas=1).bind(), route_prefix="/echo")
    wait_for_condition(lambda: len(replica_ids()) == 1, timeout=60)
    assert _haproxy_pids() == pids


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
