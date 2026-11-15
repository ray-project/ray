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
from starlette.responses import StreamingResponse

from ray._common.network_utils import find_free_port
from ray._common.test_utils import async_wait_for_condition, wait_for_condition
from ray.serve._private.common import RequestProtocol
from ray.serve._private.constants import (
    RAY_SERVE_ENABLE_HA_PROXY,
    SERVE_INGRESS_ROUTER_REQUEST_PATH_HEADER,
    SERVE_ROUTER_APPLICATION_DIRECT_RESPONSE_HEADER,
    SERVE_ROUTER_APPLICATION_ORIGINAL_METHOD_HEADER,
)
from ray.serve._private.haproxy import (
    BackendConfig,
    HAProxyApi,
    HAProxyConfig,
    HAProxyManager,
    ServerConfig,
    _format_router_application_pools_lua,
    _format_router_target_apps_lua,
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
            assert "force-persist if { var(txn.router_server) -m found }" in config
            # The router is never a dispatch target.
            assert "use_backend http-router if { var(txn.router_backend)" not in config
            # Router-selected backends win over path routing.
            assert config.index("var(txn.router_backend)") < config.index(
                "use_backend http-router if is_http-router"
            )
            # A routed request never also runs the target's ingress request router.
            assert (
                "http-request lua.route_via_ingress_request_router if "
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
    def test_pool_prefers_colocated_replicas_with_remote_fallbacks(self):
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
        local_lua = _format_router_application_pools_lua([backend], "10.0.0.1")
        assert "port = 9001" in local_lua
        assert "port = 9002" in local_lua
        assert "port = 9000" in local_lua
        assert local_lua.index("port = 9001") < local_lua.index("port = 9002")
        assert local_lua.index("port = 9002") < local_lua.index("port = 9000")
        assert local_lua.index("local_pool") < local_lua.index("port = 9001")
        assert local_lua.index("remote_pool") < local_lua.index("port = 9000")

        remote_lua = _format_router_application_pools_lua([backend], "10.0.0.9")
        assert "port = 9001" in remote_lua
        assert "port = 9002" in remote_lua
        assert "port = 9000" in remote_lua

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
        targets_lua = _format_router_target_apps_lua(backends)
        assert '["model-a"]' in targets_lua
        assert '["router"]' not in targets_lua
        assert '["scaled-to-zero"]' not in targets_lua


def _serve(app: FastAPI, port: int) -> uvicorn.Server:
    server = uvicorn.Server(
        uvicorn.Config(
            app=app, host="0.0.0.0", port=port, log_level="error", access_log=False
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

    @app.api_route("/{path:path}", methods=["GET", "HEAD", "POST"])
    async def handle(path: str, request: Request, response: Response):
        response.headers["x-replica"] = name
        response.headers["x-method"] = request.method
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

    def __init__(self, port: int, route_prefix: str = "/"):
        self.calls: List[Dict] = []
        self.response: Tuple[int, str] = (500, "unset")
        self.response_headers: Dict[str, str] = {}
        self.stream_response = False
        self.delay_s = 0.0
        app = FastAPI()

        @app.get("/-/healthz")
        async def health():
            return "ok"

        internal_route = (
            "/internal/route"
            if route_prefix == "/"
            else f"{route_prefix}/internal/route"
        )

        @app.post(internal_route)
        async def route(request: Request):
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
            if self.stream_response:
                midpoint = len(body) // 2
                return StreamingResponse(
                    iter([body[:midpoint], body[midpoint:]]),
                    status_code=status,
                    media_type="application/json",
                    headers=self.response_headers,
                )
            return Response(
                body,
                status_code=status,
                media_type="application/json",
                headers=self.response_headers,
            )

        self.server = _serve(app, port)

    def decide(self, app: str, name: str):
        self.response = (
            200,
            json.dumps({"application": app, "replica_id": _replica_id(app, name)}),
        )


@contextlib.asynccontextmanager
async def _router_cluster(
    router_prefix: str = "/",
    extra_app: Optional[str] = None,
    router_scaled_to_zero: bool = False,
    router_fallback: bool = False,
    remote_router: bool = False,
):
    """HAProxy with a router at `router_prefix` and model-a, model-b, control."""
    with tempfile.TemporaryDirectory() as temp_dir:
        haproxy_port = find_free_port()
        replica_names = ["a1", "b1", "b2", "c1"]
        if extra_app:
            replica_names.append("extra")
        ports = {k: find_free_port() for k in ["r1", "r2", *replica_names]}
        router = FakeRouter(ports["r1"], route_prefix=router_prefix)
        remote = (
            FakeRouter(ports["r2"], route_prefix=router_prefix)
            if remote_router
            else None
        )
        router.remote = remote
        replicas = {name: _fake_replica(ports[name], name) for name in replica_names}
        router_servers = (
            [] if router_scaled_to_zero else [_server("router", "r1", ports["r1"])]
        )
        if remote is not None:
            router_servers.append(
                _server("router", "r2", ports["r2"], host="127.0.0.2")
            )
        backends = [
            _backend(
                "router",
                router_prefix,
                router_servers,
                is_router_application=True,
                fallback_server=(
                    ServerConfig(name="proxy", host="127.0.0.1", port=ports["r1"])
                    if router_fallback
                    else None
                ),
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
        if extra_app:
            backends.append(
                BackendConfig(
                    name="http-extra",
                    path_prefix="/v1/extra",
                    app_name=extra_app,
                    ingress_deployment_name="Ingress",
                    http_health_check_path="/-/healthz",
                    servers=[
                        ServerConfig(
                            name="extra",
                            host="127.0.0.1",
                            port=ports["extra"],
                            replica_id=_replica_id(extra_app, "extra"),
                        )
                    ],
                )
            )
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
            for server in [
                router.server,
                *(remote.server for remote in [remote] if remote),
                *replicas.values(),
            ]:
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

    cases = [
        (
            "model-b",
            "b2",
            {"x-request-id": "req-b2", "authorization": "Bearer secret"},
        ),
        (
            "model-a",
            "a1",
            {"x-session-id": "session-1", "x-serve-router-forged": "1"},
        ),
        ("model-b", "b1", {"x-request-id": "req-b1"}),
    ]
    for app, name, headers in cases:
        router.decide(app, name)
        body = {"model": app, "messages": []}
        resp = _chat(url, body, headers=headers, query="?x=1")

        assert resp.status_code == 200, resp.text
        served = resp.json()
        assert served["replica"] == name
        assert served["method"] == "POST"
        assert served["path"] == f"/v1/{app}/v1/chat/completions"
        assert served["query"] == "x=1"
        assert json.loads(served["body"]) == body
        assert served["serve_headers"] == []
        call = router.calls[-1]
        assert call["method"] == "POST"
        assert call["path"] == "/internal/route"
        assert call["query"] == ""
        assert json.loads(call["body"]) == body
        assert call["headers"]["content-type"] == "application/json"
        assert (
            call["headers"][SERVE_ROUTER_APPLICATION_ORIGINAL_METHOD_HEADER] == "POST"
        )
        assert (
            call["headers"][SERVE_INGRESS_ROUTER_REQUEST_PATH_HEADER]
            == "/v1/chat/completions?x=1"
        )
        assert call["headers"]["x-request-id"] == served["request_id"]
        if "authorization" in headers:
            assert call["headers"]["authorization"] == headers["authorization"]
        if "x-request-id" in headers:
            assert served["request_id"] == headers["x-request-id"]
        else:
            assert served["request_id"]
            assert call["headers"]["x-session-id"] == "session-1"
            assert "x-serve-router-forged" not in call["headers"]

    assert len(router.calls) == len(cases)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "method,request_target", [("GET", "/v1/models?limit=2"), ("HEAD", "/v1/models")]
)
async def test_non_post_request_uses_post_for_decision(
    router_cluster, method, request_target
):
    url, router, _ = router_cluster
    router.decide("control", "c1")

    resp = requests.request(method, f"{url}{request_target}", timeout=10)

    assert resp.status_code == 200, resp.text
    assert resp.headers["x-replica"] == "c1"
    assert resp.headers["x-method"] == method
    if method == "GET":
        served = resp.json()
        assert served["path"] == "/v1/control/v1/models"
        assert served["query"] == "limit=2"
    else:
        assert resp.content == b""

    call = router.calls[-1]
    assert call["method"] == "POST"
    assert call["path"] == "/internal/route"
    assert call["headers"][SERVE_ROUTER_APPLICATION_ORIGINAL_METHOD_HEADER] == method
    assert call["headers"][SERVE_INGRESS_ROUTER_REQUEST_PATH_HEADER] == request_target


@pytest.mark.asyncio
async def test_dispatches_escaped_decision_values():
    app = 'mod\u00e8le "replica_id": "x\\\U0001f680'
    async with _router_cluster(extra_app=app) as (url, router, _):
        router.decide(app, "extra")

        resp = _chat(url, {})

        assert resp.status_code == 200, resp.text
        assert resp.json()["replica"] == "extra"


@pytest.mark.asyncio
@pytest.mark.parametrize("router_cluster", ["/llm"], indirect=True)
async def test_router_prefix_is_replaced(router_cluster):
    url, router, _ = router_cluster
    router.decide("model-a", "a1")

    resp = requests.post(f"{url}/llm/v1/chat/completions?x=1", json={}, timeout=10)

    assert resp.status_code == 200, resp.text
    assert resp.json()["path"] == "/v1/model-a/v1/chat/completions"
    assert resp.json()["query"] == "x=1"
    assert (
        router.calls[-1]["headers"][SERVE_INGRESS_ROUTER_REQUEST_PATH_HEADER]
        == "/v1/chat/completions?x=1"
    )
    assert router.calls[-1]["path"] == "/llm/internal/route"

    resp = requests.get(f"{url}/llm?x=1", timeout=10)

    assert resp.status_code == 200, resp.text
    assert (
        router.calls[-1]["headers"][SERVE_INGRESS_ROUTER_REQUEST_PATH_HEADER] == "/?x=1"
    )
    assert router.calls[-1]["path"] == "/llm/internal/route"

    # Paths outside the router's prefix are not routed by it.
    assert requests.post(f"{url}/v1/chat/completions", timeout=10).status_code == 404
    assert (
        requests.post(
            f"{url}/v1/model-a/v1/chat/completions", json={}, timeout=10
        ).json()["replica"]
        == "a1"
    )
    assert (
        requests.get(f"{url}/v1/control/v1/models", timeout=10).json()["replica"]
        == "c1"
    )
    assert len(router.calls) == 2


@pytest.mark.asyncio
async def test_forwards_cors_preflight_headers(router_cluster):
    url, router, _ = router_cluster
    router.response = (200, "OK")
    router.response_headers = {
        SERVE_ROUTER_APPLICATION_DIRECT_RESPONSE_HEADER: "1",
        "Access-Control-Allow-Origin": "https://example.com",
    }

    resp = requests.options(
        f"{url}/v1/chat/completions",
        headers={
            "Origin": "https://example.com",
            "Access-Control-Request-Method": "POST",
            "Access-Control-Request-Headers": "authorization,content-type",
        },
        timeout=10,
    )

    assert resp.status_code == 200, resp.text
    assert resp.headers["access-control-allow-origin"] == "https://example.com"
    call_headers = router.calls[-1]["headers"]
    assert call_headers["origin"] == "https://example.com"
    assert call_headers["access-control-request-method"] == "POST"
    assert (
        call_headers["access-control-request-headers"] == "authorization,content-type"
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("status", [400, 404, 413, 503])
async def test_returns_router_error_to_client(router_cluster, status):
    url, router, _ = router_cluster
    error = {"error": {"message": "nope", "code": status}}
    router.response = (status, json.dumps(error))
    if status == 503:
        router.response_headers = {"Retry-After": "7"}

    resp = _chat(url, {})

    assert resp.status_code == status
    assert resp.headers["content-type"] == "application/json"
    assert resp.json() == error
    assert resp.headers.get("retry-after") == ("7" if status == 503 else None)


@pytest.mark.asyncio
async def test_returns_marked_responses_directly(router_cluster):
    url, router, _ = router_cluster
    body = {"object": "list", "data": [{"id": "model-a"}]}
    router.response = (200, json.dumps(body))
    router.response_headers = {
        SERVE_ROUTER_APPLICATION_DIRECT_RESPONSE_HEADER: "1",
    }

    resp = requests.get(f"{url}/v1/models", timeout=10)

    assert resp.status_code == 200
    assert resp.headers["content-type"] == "application/json"
    assert SERVE_ROUTER_APPLICATION_DIRECT_RESPONSE_HEADER not in resp.headers
    assert resp.json() == body

    router.stream_response = True
    resp = requests.get(f"{url}/v1/models", timeout=10)

    assert resp.status_code == 200
    assert resp.headers["content-type"] == "application/json"
    assert SERVE_ROUTER_APPLICATION_DIRECT_RESPONSE_HEADER not in resp.headers
    assert resp.json() == body

    router.stream_response = False
    router.response = (307, "")
    router.response_headers = {
        SERVE_ROUTER_APPLICATION_DIRECT_RESPONSE_HEADER: "1",
        "Access-Control-Allow-Origin": "https://example.com",
        "Connection": "x-router-hop",
        "Location": "/v1/redirected",
        "Set-Cookie": "session=abc; HttpOnly",
        "X-Router-Hop": "secret",
    }

    resp = requests.get(f"{url}/v1/models", allow_redirects=False, timeout=10)

    assert resp.status_code == 307
    assert resp.headers["access-control-allow-origin"] == "https://example.com"
    assert resp.headers["location"] == "/v1/redirected"
    assert resp.headers["set-cookie"] == "session=abc; HttpOnly"
    assert SERVE_ROUTER_APPLICATION_DIRECT_RESPONSE_HEADER not in resp.headers
    assert "x-router-hop" not in resp.headers


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
    url, router, _ = router_cluster
    body = decision if isinstance(decision, str) else json.dumps(decision)
    router.response = (status, body)

    resp = _chat(url, {})

    assert resp.status_code == 503
    assert resp.headers["x-serve-reason"] == reason
    assert resp.headers["retry-after"] == "1"
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
async def test_remote_router_is_only_used_after_local_connection_failure():
    async with _router_cluster(remote_router=True) as (url, router, _):
        router.decide("model-a", "a1")
        router.remote.decide("model-b", "b1")

        for _ in range(3):
            assert _chat(url, {}).json()["replica"] == "a1"
        assert router.remote.calls == []

        router.server.should_exit = True
        await async_wait_for_condition(
            lambda: _chat(url, {}).json().get("replica") == "b1", timeout=10
        )
        assert router.remote.calls


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
    async with _router_cluster(
        router_scaled_to_zero=True, router_fallback=with_fallback
    ) as (url, router, _):
        router.decide("model-a", "a1")
        resp = _chat(url, {})

        if with_fallback:
            assert resp.status_code == 200, resp.text
            assert resp.json()["replica"] == "a1"
            assert len(router.calls) == 1
        else:
            assert resp.status_code == 503
            assert resp.headers["x-serve-reason"] == "router_unavailable"
            assert router.calls == []


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
