"""HAProxy router applications, run against a real HAProxy and fake replicas."""
import asyncio
import contextlib
import json
import os
import subprocess
import sys
import tempfile
import threading
import time
from typing import Dict, List, Optional, Tuple

import pytest
import pytest_asyncio
import requests
import uvicorn
from fastapi import FastAPI, Request, Response

from ray._common.network_utils import find_free_port
from ray._common.test_utils import async_wait_for_condition, wait_for_condition
from ray.serve._private.common import RequestProtocol
from ray.serve._private.constants import RAY_SERVE_ENABLE_HA_PROXY
from ray.serve._private.haproxy import (
    BackendConfig,
    HAProxyApi,
    HAProxyConfig,
    HAProxyManager,
    ServerConfig,
    _router_application_pools,
    _router_target_apps,
    get_haproxy_binary,
)
from ray.serve.config import HTTPOptions
from ray.serve.schema import Target, TargetGroup

pytestmark = pytest.mark.skipif(
    not RAY_SERVE_ENABLE_HA_PROXY,
    reason="RAY_SERVE_ENABLE_HA_PROXY not set.",
)


def _replica_id(app: str, name: str) -> str:
    return f"SERVE_REPLICA::{app}#Ingress#{name}"


def _server(app: str, name: str, port: int, host: str = "127.0.0.1"):
    return ServerConfig(
        name=f"{app}-{name}", host=host, port=port, replica_id=_replica_id(app, name)
    )


def _backend(app: str, prefix: str, servers, **kwargs) -> BackendConfig:
    return BackendConfig(
        name=f"http-{app}",
        path_prefix=prefix,
        app_name=app,
        ingress_deployment_name="Ingress",
        http_health_check_path="/-/healthz",
        servers=servers,
        **kwargs,
    )


def _haproxy_config(temp_dir: str, port: int) -> HAProxyConfig:
    return HAProxyConfig(
        http_options=HTTPOptions(host="127.0.0.1", port=port, keep_alive_timeout_s=58),
        stats_port=find_free_port(),
        socket_path=os.path.join(temp_dir, "admin.sock"),
        has_received_routes=True,
        has_received_servers=True,
        http_health_check_path="/-/healthz",
        health_check_inter="500ms",
        health_check_rise=1,
        health_check_fall=2,
    )


def _render(temp_dir: str, backends: List[BackendConfig]) -> str:
    api = HAProxyApi(
        cfg=_haproxy_config(temp_dir, find_free_port()),
        backend_configs={b.name: b for b in backends},
        config_file_path=os.path.join(temp_dir, "haproxy.cfg"),
    )
    api._generate_config_file_internal()
    with open(api.config_file_path) as f:
        return f.read()


def _check_config(config_path: str) -> None:
    result = subprocess.run(
        [get_haproxy_binary(), "-c", "-f", config_path],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stdout + result.stderr


class TestRouterApplicationConfig:
    def test_no_router_application_renders_nothing(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            config = _render(
                temp_dir, [_backend("app", "/", [_server("app", "a", 9000)])]
            )
            assert "router_app" not in config
            assert not os.path.exists(os.path.join(temp_dir, "router_application.lua"))

    def test_router_application_renders_valid_config(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            config = _render(
                temp_dir,
                [
                    _backend(
                        "router",
                        "/",
                        [_server("router", "r1", 9000)],
                        is_router_application=True,
                    ),
                    _backend(
                        "model-a",
                        "/v1/model-a",
                        [_server("model-a", "a1", 9001)],
                        ingress_request_router_servers=[
                            ServerConfig(name="llm-router", host="127.0.0.1", port=9002)
                        ],
                    ),
                ],
            )
            lua_path = os.path.join(temp_dir, "router_application.lua")
            assert f"lua-load-per-thread {lua_path}" in config
            assert (
                "http-request set-var(txn.router_app) str(http-router) if "
                '{ var(txn.serve_backend) -m str "http-router" }'
            ) in config
            assert (
                'use_backend http-model-a if { var(txn.router_backend) -m str "http-model-a" }'
                in config
            )
            assert (
                'use-server model-a-a1 if { var(txn.router_server) -m str "model-a-a1" }'
                in config
            )
            # The router is never a dispatch target.
            assert "use_backend http-router if { var(txn.router_backend)" not in config
            # Router-selected backends win over path routing.
            assert config.index("var(txn.router_backend)") < config.index(
                "use_backend http-router if is_http-router"
            )
            # A routed request never also runs the target's ingress request router.
            assert (
                "http-request lua.route_via_ingress_request_router if METH_POST "
                "has_ingress_request_router_app !is_router_request"
            ) in config
            with open(lua_path) as f:
                assert '"SERVE_REPLICA::model-a#Ingress#a1"' in f.read()
            _check_config(os.path.join(temp_dir, "haproxy.cfg"))


def test_manager_carries_router_marker_to_backend_config():
    manager_cls = HAProxyManager.__ray_metadata__.modified_class
    manager = manager_cls.__new__(manager_cls)
    manager._node_ip_address = "10.0.0.1"
    target_group = TargetGroup(
        protocol=RequestProtocol.HTTP,
        route_prefix="/",
        app_name="router",
        targets=[Target(ip="10.0.0.1", port=9000, instance_id="", name="r1")],
        is_router_application=True,
    )

    backend = manager._create_backend_config(target_group, fallback_target=None)

    assert backend.is_router_application is True


class TestRouterApplicationMaps:
    def test_pool_prefers_colocated_replicas(self):
        backend = _backend(
            "router",
            "/",
            [
                _server("router", "remote", 9000, host="10.0.0.2"),
                _server("router", "local-2", 9002, host="10.0.0.1"),
                _server("router", "local-1", 9001, host="10.0.0.1"),
            ],
            is_router_application=True,
        )
        (_, pool) = _router_application_pools([backend], "10.0.0.1")["http-router"]
        assert [s.name for s in pool] == ["router-local-1", "router-local-2"]

        (_, pool) = _router_application_pools([backend], "10.0.0.9")["http-router"]
        assert [s.name for s in pool] == [
            "router-local-1",
            "router-local-2",
            "router-remote",
        ]

    def test_pool_uses_fallback_when_scaled_to_zero(self):
        fallback = ServerConfig(name="proxy", host="10.0.0.1", port=8000)
        backend = _backend(
            "router", "/", [], is_router_application=True, fallback_server=fallback
        )
        assert _router_application_pools([backend])["http-router"][1] == [fallback]

        no_fallback = _backend("router", "/", [], is_router_application=True)
        assert _router_application_pools([no_fallback])["http-router"][1] == []

    def test_target_apps(self):
        backends = [
            _backend(
                "router", "/", [_server("router", "r", 1)], is_router_application=True
            ),
            _backend("model-a", "/a", [_server("model-a", "a", 2)]),
            _backend("scaled-to-zero", "/z", []),
            # The proxies' target group has no application.
            BackendConfig(
                name="http-",
                path_prefix="/",
                servers=[ServerConfig(name="p", host="h", port=3, replica_id="p")],
            ),
        ]
        assert list(_router_target_apps(backends)) == ["model-a"]


def _serve(app: FastAPI, port: int) -> uvicorn.Server:
    server = uvicorn.Server(
        uvicorn.Config(
            app=app, host="127.0.0.1", port=port, log_level="error", access_log=False
        )
    )
    threading.Thread(target=lambda: asyncio.run(server.serve()), daemon=True).start()
    wait_for_condition(
        lambda: requests.get(f"http://127.0.0.1:{port}/-/healthz", timeout=2).ok
    )
    return server


def _fake_replica(port: int, name: str) -> uvicorn.Server:
    app = FastAPI()

    @app.get("/-/healthz")
    async def health():
        return "ok"

    @app.api_route("/{path:path}", methods=["GET", "POST"])
    async def handle(path: str, request: Request):
        return {
            "replica": name,
            "method": request.method,
            "path": request.url.path,
            "query": request.url.query,
            "body": (await request.body()).decode(),
            "request_id": request.headers.get("x-request-id"),
            "serve_headers": sorted(
                k for k in request.headers if k.startswith("x-serve-")
            ),
        }

    return _serve(app, port)


class FakeRouter:
    """Router ingress that answers with `self.response` and records calls."""

    def __init__(self, port: int):
        self.calls: List[Dict] = []
        self.response: Tuple[int, str] = (500, "unset")
        self.delay_s = 0.0
        app = FastAPI()

        @app.get("/-/healthz")
        async def health():
            return "ok"

        @app.api_route("/{path:path}", methods=["GET", "POST"])
        async def route(path: str, request: Request):
            self.calls.append(
                {
                    "method": request.method,
                    "path": request.url.path,
                    "query": request.url.query,
                    "body": (await request.body()).decode(),
                    "headers": dict(request.headers),
                }
            )
            await asyncio.sleep(self.delay_s)
            status, body = self.response
            return Response(body, status_code=status, media_type="application/json")

        self.server = _serve(app, port)

    def decide(self, app: str, name: str):
        self.response = (
            200,
            json.dumps({"application": app, "replica_id": _replica_id(app, name)}),
        )


@contextlib.asynccontextmanager
async def _router_cluster(router_prefix: str = "/"):
    """HAProxy with a router at `router_prefix` and model-a, model-b, control."""
    with tempfile.TemporaryDirectory() as temp_dir:
        haproxy_port = find_free_port()
        ports = {k: find_free_port() for k in ("r1", "a1", "b1", "b2", "c1")}
        router = FakeRouter(ports["r1"])
        replicas = {
            name: _fake_replica(ports[name], name) for name in ("a1", "b1", "b2", "c1")
        }
        backends = [
            _backend(
                "router",
                router_prefix,
                [_server("router", "r1", ports["r1"])],
                is_router_application=True,
            ),
            _backend("model-a", "/v1/model-a", [_server("model-a", "a1", ports["a1"])]),
            _backend(
                "model-b",
                "/v1/model-b",
                [
                    _server("model-b", "b1", ports["b1"]),
                    _server("model-b", "b2", ports["b2"]),
                ],
            ),
            _backend("control", "/v1/control", [_server("control", "c1", ports["c1"])]),
        ]
        api = HAProxyApi(
            cfg=_haproxy_config(temp_dir, haproxy_port),
            backend_configs={b.name: b for b in backends},
            config_file_path=os.path.join(temp_dir, "haproxy.cfg"),
        )
        url = f"http://127.0.0.1:{haproxy_port}"
        try:
            await api.start()
            await async_wait_for_condition(
                lambda: requests.get(f"{url}/-/healthz", timeout=2).ok, timeout=10
            )
            yield url, router, replicas
        finally:
            await api.stop()
            for server in [router.server, *replicas.values()]:
                server.should_exit = True


@pytest_asyncio.fixture
async def router_cluster(request):
    async with _router_cluster(getattr(request, "param", "/")) as cluster:
        yield cluster


def _chat(
    url: str, body: Dict, headers: Optional[Dict] = None, query: str = ""
) -> requests.Response:
    return requests.post(
        f"{url}/v1/chat/completions{query}", json=body, headers=headers, timeout=15
    )


@pytest.mark.asyncio
async def test_post_dispatches_to_selected_replica(router_cluster):
    url, router, _ = router_cluster

    for app, name in [("model-b", "b2"), ("model-a", "a1"), ("model-b", "b1")]:
        router.decide(app, name)
        body = {"model": app, "messages": []}
        resp = _chat(url, body, headers={"x-request-id": f"req-{name}"}, query="?x=1")

        assert resp.status_code == 200, resp.text
        served = resp.json()
        # The client sees the selected replica's response, not the decision.
        assert served["replica"] == name
        assert served["method"] == "POST"
        assert served["path"] == f"/v1/{app}/v1/chat/completions"
        assert served["query"] == "x=1"
        assert json.loads(served["body"]) == body
        assert served["request_id"] == f"req-{name}"
        assert served["serve_headers"] == []

    assert len(router.calls) == 3
    call = router.calls[-1]
    assert call["method"] == "POST"
    assert call["path"] == "/v1/chat/completions"
    assert call["query"] == "x=1"
    assert json.loads(call["body"])["model"] == "model-b"
    assert call["headers"]["x-request-id"] == "req-b1"
    assert call["headers"]["content-type"] == "application/json"


@pytest.mark.asyncio
async def test_get_dispatches_to_selected_application(router_cluster):
    url, router, _ = router_cluster
    router.decide("control", "c1")

    resp = requests.get(f"{url}/v1/models?limit=2", timeout=10)

    assert resp.status_code == 200, resp.text
    served = resp.json()
    assert served["replica"] == "c1"
    assert served["method"] == "GET"
    assert served["path"] == "/v1/control/v1/models"
    assert served["query"] == "limit=2"
    assert router.calls[-1]["method"] == "GET"
    assert router.calls[-1]["path"] == "/v1/models"


@pytest.mark.asyncio
@pytest.mark.parametrize("router_cluster", ["/llm"], indirect=True)
async def test_router_prefix_is_replaced(router_cluster):
    url, router, _ = router_cluster
    router.decide("model-a", "a1")

    resp = requests.post(f"{url}/llm/v1/chat/completions", json={}, timeout=10)

    assert resp.status_code == 200, resp.text
    assert resp.json()["path"] == "/v1/model-a/v1/chat/completions"
    assert router.calls[-1]["path"] == "/llm/v1/chat/completions"

    # Paths outside the router's prefix are not routed by it.
    assert requests.post(f"{url}/v1/chat/completions", timeout=10).status_code == 404
    assert len(router.calls) == 1


@pytest.mark.asyncio
async def test_forwards_session_and_generated_request_id(router_cluster):
    url, router, _ = router_cluster
    router.decide("model-a", "a1")

    resp = _chat(url, {}, headers={"x-session-id": "session-1"})

    assert resp.status_code == 200, resp.text
    headers = router.calls[-1]["headers"]
    assert headers["x-session-id"] == "session-1"
    # HAProxy generates one request ID and uses it on both hops.
    assert headers["x-request-id"]
    assert resp.json()["request_id"] == headers["x-request-id"]


@pytest.mark.asyncio
async def test_strips_client_serve_headers(router_cluster):
    url, router, _ = router_cluster
    router.decide("model-a", "a1")

    resp = _chat(url, {}, headers={"x-serve-router-forged": "1"})

    assert resp.status_code == 200, resp.text
    assert resp.json()["serve_headers"] == []
    assert "x-serve-router-forged" not in router.calls[-1]["headers"]


@pytest.mark.asyncio
@pytest.mark.parametrize("status", [400, 404, 413, 503])
async def test_returns_router_error_to_client(router_cluster, status):
    url, router, _ = router_cluster
    error = {"error": {"message": "nope", "code": status}}
    router.response = (status, json.dumps(error))

    resp = _chat(url, {})

    assert resp.status_code == status
    assert resp.headers["content-type"] == "application/json"
    assert resp.json() == error


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "status, decision, reason",
    [
        (200, "not json", "malformed_decision"),
        (200, {"application": "model-a"}, "malformed_decision"),
        (200, {"replica_id": _replica_id("model-a", "a1")}, "malformed_decision"),
        (200, {"application": 1, "replica_id": "x"}, "malformed_decision"),
        (201, {"application": "model-a", "replica_id": "x"}, "malformed_decision"),
        (
            200,
            {"application": "model-c", "replica_id": _replica_id("model-c", "c")},
            "unknown_application",
        ),
        # A router cannot route to itself or another router.
        (
            200,
            {"application": "router", "replica_id": _replica_id("router", "r1")},
            "unknown_application",
        ),
        (
            200,
            {"application": "model-a", "replica_id": _replica_id("model-a", "gone")},
            "unknown_replica_id",
        ),
        # A replica of another application does not resolve.
        (
            200,
            {"application": "model-a", "replica_id": _replica_id("model-b", "b1")},
            "unknown_replica_id",
        ),
    ],
)
async def test_fails_closed_on_bad_decision(router_cluster, status, decision, reason):
    url, router, replicas = router_cluster
    body = decision if isinstance(decision, str) else json.dumps(decision)
    router.response = (status, body)

    resp = _chat(url, {})

    assert resp.status_code == 503
    assert resp.headers["x-serve-reason"] == reason
    assert resp.text == f"Router application failed: {reason}"


@pytest.mark.asyncio
async def test_fails_closed_when_selected_replica_is_down(router_cluster):
    """A DOWN pinned replica is not swapped for another one."""
    url, router, replicas = router_cluster
    router.decide("model-b", "b1")
    replicas["b1"].should_exit = True

    await async_wait_for_condition(
        lambda: _chat(url, {}).headers.get("x-serve-reason") == "replica_unavailable",
        timeout=15,
    )


@pytest.mark.asyncio
async def test_fails_closed_when_router_unreachable(router_cluster):
    url, router, _ = router_cluster
    router.server.should_exit = True

    await async_wait_for_condition(
        lambda: _chat(url, {}).headers.get("x-serve-reason") == "router_unreachable",
        timeout=10,
    )


@pytest.mark.asyncio
async def test_fails_closed_when_router_times_out(monkeypatch):
    monkeypatch.setattr(
        "ray.serve._private.haproxy.RAY_SERVE_HAPROXY_INGRESS_REQUEST_ROUTER_TIMEOUT_S",
        1,
    )
    async with _router_cluster() as (url, router, _):
        router.decide("model-a", "a1")
        router.delay_s = 3
        start = time.monotonic()

        resp = _chat(url, {})

        assert resp.status_code == 503
        assert resp.headers["x-serve-reason"] == "router_timeout"
        assert time.monotonic() - start < 3


@pytest.mark.asyncio
async def test_direct_requests_are_not_routed(router_cluster):
    url, router, _ = router_cluster

    resp = requests.post(f"{url}/v1/model-a/v1/chat/completions", json={}, timeout=10)
    assert resp.json()["replica"] == "a1"
    resp = requests.get(f"{url}/v1/control/v1/models", timeout=10)
    assert resp.json()["replica"] == "c1"

    assert router.calls == []


@pytest.mark.asyncio
async def test_truncated_body_is_marked_and_forwarded_whole(monkeypatch):
    """Over tune.bufsize the router sees a marked prefix; the replica gets all."""
    monkeypatch.setattr(
        "ray.serve._private.haproxy.RAY_SERVE_HAPROXY_INGRESS_REQUEST_ROUTER_BUFSIZE",
        64 * 1024,
    )
    async with _router_cluster() as (url, router, _):
        router.decide("model-a", "a1")
        raw = json.dumps({"pad": "x" * 300_000})

        resp = requests.post(
            f"{url}/v1/chat/completions",
            data=raw,
            headers={"content-type": "application/json"},
            timeout=10,
        )

        assert resp.status_code == 200, resp.text
        assert resp.json()["body"] == raw
        call = router.calls[-1]
        received, total = call["headers"]["x-body-truncated"].split("/")
        assert int(total) == len(raw)
        assert int(received) == len(call["body"]) < len(raw)


@pytest.mark.asyncio
@pytest.mark.parametrize("with_fallback", [True, False])
async def test_scaled_to_zero_router(with_fallback):
    """With no router replicas HAProxy asks the fallback proxy, else 503s."""
    with tempfile.TemporaryDirectory() as temp_dir:
        haproxy_port = find_free_port()
        proxy_port, a1_port = find_free_port(), find_free_port()
        # The fallback proxy routes to the router by path; the fake plays both.
        proxy = FakeRouter(proxy_port)
        proxy.decide("model-a", "a1")
        replica = _fake_replica(a1_port, "a1")
        fallback = ServerConfig(name="proxy", host="127.0.0.1", port=proxy_port)
        backends = [
            _backend(
                "router",
                "/",
                [],
                is_router_application=True,
                fallback_server=fallback if with_fallback else None,
            ),
            _backend("model-a", "/v1/model-a", [_server("model-a", "a1", a1_port)]),
        ]
        api = HAProxyApi(
            cfg=_haproxy_config(temp_dir, haproxy_port),
            backend_configs={b.name: b for b in backends},
            config_file_path=os.path.join(temp_dir, "haproxy.cfg"),
        )
        url = f"http://127.0.0.1:{haproxy_port}"
        try:
            await api.start()
            await async_wait_for_condition(
                lambda: requests.get(f"{url}/-/healthz", timeout=2).ok, timeout=10
            )
            resp = _chat(url, {})
            if with_fallback:
                assert resp.status_code == 200, resp.text
                assert resp.json()["replica"] == "a1"
                assert len(proxy.calls) == 1
            else:
                assert resp.status_code == 503
                assert resp.headers["x-serve-reason"] == "router_unavailable"
                assert proxy.calls == []
        finally:
            await api.stop()
            proxy.server.should_exit = True
            replica.should_exit = True


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
