"""End-to-end tests for HAProxy master-worker mode (real HAProxy).

With RAY_SERVE_HAPROXY_MASTER_WORKER_ENABLED, a long-lived HAProxy master
performs reloads (SIGUSR2) instead of Serve spawning a new process with `-sf`.
"""
import asyncio
import os
import subprocess
import sys
import tempfile
import threading
import time
from collections import Counter
from typing import Dict, List, Set

import pytest
import pytest_asyncio
import requests
import uvicorn
from fastapi import FastAPI, Request, Response

import ray
from ray import serve
from ray._common.network_utils import find_free_port
from ray._common.test_utils import async_wait_for_condition, wait_for_condition
from ray.serve._private.constants import RAY_SERVE_ENABLE_HA_PROXY
from ray.serve._private.haproxy import (
    BackendConfig,
    HAProxyApi,
    HAProxyConfig,
    ServerConfig,
)
from ray.serve.config import HTTPOptions

pytestmark = pytest.mark.skipif(
    not RAY_SERVE_ENABLE_HA_PROXY,
    reason="RAY_SERVE_ENABLE_HA_PROXY not set.",
)


def _haproxy_pids() -> Set[int]:
    result = subprocess.run(
        ["pgrep", "-x", "haproxy"], capture_output=True, text=True, check=False
    )
    return {int(pid) for pid in result.stdout.split()}


def _haproxy_masters() -> Set[int]:
    """HAProxy processes whose parent is not another HAProxy process."""
    result = subprocess.run(
        ["ps", "-o", "pid=,ppid=", "-C", "haproxy"],
        capture_output=True,
        text=True,
        check=False,
    )
    rows = [tuple(map(int, line.split())) for line in result.stdout.splitlines()]
    pids = {pid for pid, _ in rows}
    return {pid for pid, ppid in rows if ppid not in pids}


@pytest.fixture(autouse=True)
def clean_up_haproxy_processes():
    subprocess.run(["pkill", "-x", "haproxy"], check=False)
    yield
    subprocess.run(["pkill", "-x", "haproxy"], check=False)


class FakeReplica:
    """HTTP server standing in for a replica's direct-ingress port."""

    def __init__(self, label: str):
        self.port = find_free_port()
        app = FastAPI()

        @app.get("/-/healthz")
        async def healthz():
            return "success"

        @app.get("/{path:path}")
        async def handle(path: str, request: Request, response: Response):
            delay_s = float(request.query_params.get("sleep", "0"))
            if delay_s:
                await asyncio.sleep(delay_s)
            response.headers["x-replica"] = label
            return {"replica": label}

        self._server = uvicorn.Server(
            uvicorn.Config(app, host="127.0.0.1", port=self.port, log_level="error")
        )
        self._thread = threading.Thread(
            target=lambda: asyncio.run(self._server.serve()), daemon=True
        )
        self._thread.start()
        wait_for_condition(
            lambda: requests.get(
                f"http://127.0.0.1:{self.port}/-/healthz", timeout=2
            ).ok
        )

    def stop(self) -> None:
        self._server.should_exit = True
        self._thread.join(timeout=5)


class MasterWorkerHAProxy:
    def __init__(self, temp_dir: str, replica: FakeReplica):
        self.port = find_free_port()
        self.api = HAProxyApi(
            cfg=HAProxyConfig(
                http_options=HTTPOptions(
                    host="127.0.0.1", port=self.port, keep_alive_timeout_s=58
                ),
                stats_port=find_free_port(),
                metrics_port=find_free_port(),
                socket_path=os.path.join(temp_dir, "admin.sock"),
                server_state_base=temp_dir,
                server_state_file=os.path.join(temp_dir, "server-state"),
                master_worker_enabled=True,
                has_received_routes=True,
                has_received_servers=True,
                health_check_inter="500ms",
                health_check_rise=1,
                health_check_fall=2,
            ),
            backend_configs={
                "http-app": BackendConfig(
                    name="http-app",
                    path_prefix="/app",
                    app_name="app",
                    servers=[
                        ServerConfig(
                            name="replica", host="127.0.0.1", port=replica.port
                        )
                    ],
                )
            },
            config_file_path=os.path.join(temp_dir, "haproxy.cfg"),
        )

    @property
    def url(self) -> str:
        return f"http://127.0.0.1:{self.port}/app"

    @property
    def master_pid(self) -> int:
        return self.api._proc.pid

    @property
    def worker_pid(self) -> int:
        return self.api._worker_pid


def _labels(url: str, n: int = 10) -> Counter:
    labels = Counter()
    for _ in range(n):
        response = requests.get(url, timeout=5)
        labels[
            response.headers.get("x-replica") if response.ok else response.status_code
        ] += 1
    return labels


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
async def running_haproxy():
    replica = FakeReplica("A")
    with tempfile.TemporaryDirectory(dir="/tmp") as temp_dir:
        proxy = MasterWorkerHAProxy(temp_dir, replica)
        await proxy.api.start()

        async def _serving():
            return set(_labels(proxy.url)) == {"A"}

        await async_wait_for_condition(_serving, timeout=15, retry_interval_ms=200)
        try:
            yield proxy
        finally:
            await proxy.api.stop()
            replica.stop()


@pytest.mark.asyncio
async def test_reloads_keep_master_and_replace_worker(running_haproxy):
    proxy = running_haproxy
    master_pid = proxy.master_pid

    seen_workers = {proxy.worker_pid}
    for _ in range(3):
        await proxy.api.reload()
        assert proxy.master_pid == master_pid
        assert proxy.worker_pid not in seen_workers
        seen_workers.add(proxy.worker_pid)
        assert await proxy.api._get_running_pid() == proxy.worker_pid
        assert set(_labels(proxy.url)) == {"A"}

    # Idle old workers exit on their own; the master stays.
    wait_for_condition(lambda: not proxy.api.has_alive_old_procs(), timeout=20)
    assert _haproxy_pids() == {master_pid, proxy.worker_pid}
    assert proxy.api.count_haproxy_processes() == 1


@pytest.mark.asyncio
async def test_old_worker_finishes_request_and_blocks_drain(running_haproxy):
    proxy = running_haproxy
    old_worker = proxy.worker_pid

    slow = _in_background(
        lambda: requests.get(proxy.url + "?sleep=4", timeout=30).status_code
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
async def test_failed_reload_keeps_current_worker(running_haproxy):
    proxy = running_haproxy
    master_pid, worker_pid = proxy.master_pid, proxy.worker_pid

    proxy.api.cfg.balance_algorithm = "not-an-algorithm"
    with pytest.raises(RuntimeError):
        await proxy.api.reload()

    # The master keeps the previous worker serving the previous config.
    assert proxy.api._proc.returncode is None
    assert proxy.master_pid == master_pid
    assert proxy.worker_pid == worker_pid
    assert await proxy.api._get_running_pid() == worker_pid
    assert await proxy.api.is_running()
    assert set(_labels(proxy.url)) == {"A"}

    # A corrected config reloads normally.
    proxy.api.cfg.balance_algorithm = "leastconn"
    await proxy.api.reload()
    assert proxy.worker_pid != worker_pid
    assert set(_labels(proxy.url)) == {"A"}


@pytest.mark.asyncio
async def test_stop_leaves_no_processes(running_haproxy):
    proxy = running_haproxy
    await proxy.api.reload()

    await proxy.api.stop()

    wait_for_condition(lambda: _haproxy_pids() == set(), timeout=10)
    assert proxy.api._proc is None


@pytest.fixture
def master_worker_cluster(monkeypatch):
    monkeypatch.setenv("RAY_SERVE_HAPROXY_MASTER_WORKER_ENABLED", "1")
    ray.init(num_cpus=8)
    yield
    serve.shutdown()
    ray.shutdown()


def test_serve_scaling_reloads_through_one_master(master_worker_cluster):
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
    # One master and one worker once startup reloads have settled.
    wait_for_condition(lambda: len(_haproxy_pids()) == 2, timeout=60)
    masters: List[int] = sorted(_haproxy_masters())
    assert len(masters) == 1

    serve.run(Echo.options(num_replicas=3).bind(), route_prefix="/echo")
    wait_for_condition(lambda: len(replica_ids()) == 3, timeout=60)

    # Membership changes reload through the same master, and displaced workers
    # exit, leaving one master and one worker.
    assert sorted(_haproxy_masters()) == masters
    wait_for_condition(lambda: len(_haproxy_pids()) == 2, timeout=60)


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
