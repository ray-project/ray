import sys
from contextlib import asynccontextmanager
from types import SimpleNamespace
from typing import Optional
from unittest.mock import AsyncMock, MagicMock, patch

import openai
import pytest
from fastapi import HTTPException
from starlette.datastructures import Headers

from ray import serve
from ray.llm._internal.serve.core.configs.llm_config import (
    LLMConfig,
    ModelLoadingConfig,
)
from ray.llm._internal.serve.core.configs.openai_api_models import to_model_metadata
from ray.llm._internal.serve.core.ingress import router as router_module
from ray.llm._internal.serve.core.ingress.ingress import (
    OpenAiIngress,
    make_fastapi_ingress,
)
from ray.llm._internal.serve.core.ingress.router import (
    LLMRouter,
    _parse_routing_payload,
)
from ray.llm._internal.serve.core.server.llm_server import LLMServer
from ray.llm._internal.serve.routing_policies.kv_aware.constants import (
    KV_TOKEN_METADATA_KEY,
)
from ray.llm.tests.serve.mocks.mock_vllm_engine import MockVLLMEngine
from ray.serve._private.common import DeploymentID
from ray.serve._private.constants import SERVE_SESSION_ID
from ray.serve._private.thirdparty.get_asgi_route_name import (
    ASGIRoutePatternMatcher,
    RoutePattern,
)
from ray.serve.exceptions import DeploymentUnavailableError


class _DirectRouterReplicaId:
    def __init__(self, unique_id: str, full_id: Optional[str] = None):
        self.unique_id = unique_id
        self._full_id = full_id or unique_id

    def to_full_id_str(self) -> str:
        return self._full_id


class _FakeRequest:
    def __init__(self, body: bytes, headers: Optional[dict] = None):
        self._body = body
        self.headers = Headers(headers or {})

    async def body(self) -> bytes:
        return self._body


class _DirectRouterReplica:
    """RunningReplica stand-in for ``LLMRouter._pick_replica`` tests."""

    def __init__(
        self,
        unique_id: str,
        full_id: Optional[str] = None,
        endpoint: Optional[tuple] = ("127.0.0.1", 8000),
        routing_stats: Optional[dict] = None,
    ):
        self.replica_id = _DirectRouterReplicaId(unique_id, full_id)
        self.backend_http_endpoint = endpoint
        self.routing_stats = routing_stats or {}


def _fake_handle(deployment_name: str) -> MagicMock:
    """A handle stand-in whose deployment name is real: ``route`` reports it in
    the response body, so a bare MagicMock attribute would leak into it."""
    handle = MagicMock()
    handle.deployment_id.name = deployment_name
    return handle


def _new_direct_router(handle=None, servers=None):
    """Router with ``servers`` (model id -> handle), or a single-model router
    around ``handle`` (default: one fake handle for model ``x``)."""
    router = LLMRouter.__new__(LLMRouter)
    if servers is None:
        servers = {"x": handle if handle is not None else _fake_handle("LLMServer:x")}
    router._servers = dict(servers)
    # Routing tests don't exercise tokenization; that lives in test_tokenizer.py.
    router._tokenizer = None
    return router


def _selection_for(replica):
    """Build a ``ReplicaSelection``-shaped mock that ``_pick_replica`` reads."""
    return MagicMock(replica_id=replica.replica_id.unique_id, _replica=replica)


def _choose_replica_returning(*replicas):
    """Patch ``handle.choose_replica`` to yield the given replicas in order.

    Each call to ``choose_replica`` consumes one replica from the sequence and
    yields its ``_DirectRouterReplica`` wrapped as a selection.
    """
    selections = iter(_selection_for(r) for r in replicas)

    @asynccontextmanager
    async def fake_choose_replica(*args, **kwargs):
        yield next(selections)

    return fake_choose_replica


@pytest.fixture(name="llm_config")
def create_llm_config(stream_batching_interval_ms: Optional[int] = None):

    if stream_batching_interval_ms is not None:
        return LLMConfig(
            model_loading_config=ModelLoadingConfig(
                model_id="llm_model_id",
            ),
            experimental_configs={
                "stream_batching_interval_ms": stream_batching_interval_ms,
            },
        )
    else:
        return LLMConfig(
            model_loading_config=ModelLoadingConfig(
                model_id="llm_model_id",
            ),
        )


@pytest.fixture(name="client")
def create_oai_client(llm_config: LLMConfig):
    ServerDeployment = serve.deployment(LLMServer)

    ingress_options = OpenAiIngress.get_deployment_options(llm_configs=[llm_config])
    ingress_cls = make_fastapi_ingress(OpenAiIngress)
    RouterDeployment = serve.deployment(ingress_cls, **ingress_options)
    server = ServerDeployment.bind(llm_config, engine_cls=MockVLLMEngine)
    router = RouterDeployment.bind(
        llm_deployments={llm_config.model_id: server},
        model_cards={
            llm_config.model_id: to_model_metadata(llm_config.model_id, llm_config)
        },
    )
    serve.run(router)

    client = openai.Client(base_url="http://localhost:8000/v1", api_key="foo")
    yield client

    serve.shutdown()


class TestDirectStreamingLLMRouter:
    @pytest.mark.asyncio
    async def test_route_parses_body_into_routing_payload(self):
        """A parseable body becomes a routing payload passed positionally."""
        router = _new_direct_router()
        router._pick_replica = AsyncMock(
            return_value=("127.0.0.1", 9001, "DeploymentName#replica", None)
        )

        body = b'{"model":"x","messages":[{"role":"user","content":"hi"}]}'
        request = _FakeRequest(body)

        result = await router.route(request)

        assert result == {
            "host": "127.0.0.1",
            "port": 9001,
            "deployment": "LLMServer:x",
            "replica_id": "DeploymentName#replica",
        }
        _, kwargs = router._pick_replica.call_args
        assert kwargs["handle"] is router._servers["x"]
        payload = kwargs["routing_payload"]
        assert isinstance(payload, SimpleNamespace)
        assert payload.messages == [{"role": "user", "content": "hi"}]
        # The whole body is exposed, so a router can read any field.
        assert payload.model == "x"
        assert not hasattr(payload, "prompt")
        # A parseable body must not trip the "no routing key" warning.
        assert router._warned_no_routing_key is False

    @pytest.mark.asyncio
    async def test_route_truncated_body_yields_no_payload_and_warns_once(self):
        """A truncated body derives no key. ``route`` forwards ``None`` and
        warns once per replica."""
        router = _new_direct_router()
        router._pick_replica = AsyncMock(
            return_value=("127.0.0.1", 9001, "DeploymentName#replica", None)
        )

        # Truncated prefix is not valid JSON so json.loads fails.
        body = b'{"model":"x","prompt":"' + (b"x" * 1024)
        request = _FakeRequest(body, headers={"x-body-truncated": "1058/90000"})

        with patch.object(router_module.logger, "warning") as mock_warning:
            await router.route(request)
            await router.route(request)

        # routing_payload is None on both calls. Warning fires once.
        for call in router._pick_replica.call_args_list:
            assert call.kwargs["routing_payload"] is None
        assert mock_warning.call_count == 1
        assert router._warned_no_routing_key is True

    @pytest.mark.asyncio
    async def test_route_returns_503_on_pick_failure(self):
        router = _new_direct_router()
        router._pick_replica = AsyncMock(side_effect=RuntimeError("no replicas"))

        with pytest.raises(HTTPException) as exc_info:
            await router.route(_FakeRequest(b"{}"))
        assert exc_info.value.status_code == 503
        assert "no replicas" in exc_info.value.detail

    @pytest.mark.asyncio
    async def test_route_returns_400_on_bad_routing_request(self):
        router = _new_direct_router()
        router._pick_replica = AsyncMock(side_effect=ValueError("empty prompt"))

        with pytest.raises(HTTPException) as exc_info:
            await router.route(_FakeRequest(b"{}"))
        assert exc_info.value.status_code == 400
        assert "empty prompt" in exc_info.value.detail

    @pytest.mark.asyncio
    async def test_route_returns_503_on_deployment_unavailable(self):
        err = DeploymentUnavailableError(DeploymentID(name="LLMServer:test"))
        router = _new_direct_router()
        router._pick_replica = AsyncMock(side_effect=err)

        with pytest.raises(HTTPException) as exc_info:
            await router.route(_FakeRequest(b"{}"))
        assert exc_info.value.status_code == 503
        assert "LLMServer:test" in exc_info.value.detail

    @pytest.mark.asyncio
    async def test_pick_replica_returns_backend_endpoint_from_handle(self):
        """``_pick_replica`` reads the endpoint off the selection's replica."""
        replica = _DirectRouterReplica(
            "r1",
            full_id="DeploymentName#r1",
            endpoint=("10.0.0.1", 8123),
        )
        handle = MagicMock()
        handle.choose_replica = _choose_replica_returning(replica)
        router = _new_direct_router(handle)

        host, port, replica_id, token_endpoint = await router._pick_replica(
            handle=handle
        )

        assert (host, port, replica_id) == ("10.0.0.1", 8123, "DeploymentName#r1")
        assert token_endpoint is None

    @pytest.mark.asyncio
    async def test_pick_replica_returns_prompt_token_endpoint(self):
        replica = _DirectRouterReplica(
            "r1",
            full_id="DeploymentName#r1",
            routing_stats={KV_TOKEN_METADATA_KEY: {"endpoint": "tcp://10.0.0.1:7557"}},
        )
        handle = MagicMock()
        handle.choose_replica = _choose_replica_returning(replica)
        router = _new_direct_router(handle)

        *_, token_endpoint = await router._pick_replica(handle=handle)

        assert token_endpoint == "tcp://10.0.0.1:7557"

    @pytest.mark.asyncio
    async def test_pick_replica_forwards_payload_positionally(self):
        """A routing payload reaches ``choose_replica`` as the first positional
        arg, alongside the ``_reserve=False`` fast-path flag."""
        replica = _DirectRouterReplica("r1", full_id="d#r1")

        captured = {}

        @asynccontextmanager
        async def fake_choose_replica(*args, **kwargs):
            captured["args"] = args
            captured["kwargs"] = kwargs
            yield _selection_for(replica)

        handle = MagicMock()
        handle.choose_replica = fake_choose_replica
        router = _new_direct_router(handle)

        payload = SimpleNamespace(messages=[{"role": "user", "content": "hi"}])
        await router._pick_replica(handle=handle, routing_payload=payload)

        assert captured["args"] == (payload,)
        assert captured["kwargs"] == {"_reserve": False}

    @pytest.mark.asyncio
    async def test_pick_replica_omits_positional_arg_when_no_payload(self):
        """With no routing payload, nothing is forwarded positionally. The
        configured router then sees empty args and load-balances."""
        replica = _DirectRouterReplica("r1", full_id="d#r1")

        captured = {}

        @asynccontextmanager
        async def fake_choose_replica(*args, **kwargs):
            captured["args"] = args
            captured["kwargs"] = kwargs
            yield _selection_for(replica)

        handle = MagicMock()
        handle.choose_replica = fake_choose_replica
        router = _new_direct_router(handle)

        await router._pick_replica(handle=handle, routing_payload=None)

        assert captured["args"] == ()
        assert captured["kwargs"] == {"_reserve": False}

    @pytest.mark.asyncio
    async def test_pick_replica_raises_when_endpoint_missing(self):
        """If the picked replica has no backend HTTP endpoint, surface a 503
        via ``RuntimeError`` (same error contract as before)."""
        replica = _DirectRouterReplica("r1", endpoint=None)
        handle = MagicMock()
        handle.choose_replica = _choose_replica_returning(replica)
        router = _new_direct_router(handle)

        with pytest.raises(RuntimeError, match="no backend HTTP endpoint"):
            await router._pick_replica(handle=handle)


_PICK = ("127.0.0.1", 9001, "SERVE_REPLICA::app#dep#r", None)


def _fake_ingress_handle(routes=None, error=None):
    """Ingress handle whose ``__serve_route_patterns__.remote()`` is stubbed.

    Set through ``setattr``: ``MagicMock`` auto-creates ordinary attributes but
    raises for dunder names, so the route-fetch method has to be attached by
    hand.
    """
    handle = _fake_handle("DirectStreamingIngress")
    remote = AsyncMock(
        side_effect=error,
        return_value=_INGRESS_ROUTES if routes is None else routes,
    )
    handle.__serve_route_patterns__ = MagicMock(remote=remote)
    return handle


async def _init_router(servers, llm_config=None, ingress=None):
    """Run the real ``__init__`` on a bare instance.

    ``__new__`` skips the Serve actor setup the other helpers avoid, but these
    tests are about what ``__init__`` itself does, so it is invoked explicitly.
    """
    router = LLMRouter.__new__(LLMRouter)
    await LLMRouter.__init__(
        router, servers=servers, llm_config=llm_config, ingress=ingress
    )
    return router


def _two_model_router():
    return _new_direct_router(
        servers={
            "model-a": _fake_handle("LLMServer:model-a"),
            "model-b": _fake_handle("LLMServer:model-b"),
        }
    )


class TestDirectStreamingModelSelection:
    """``route`` picks the deployment by the body's ``model`` and reports it.

    The selection logic is exercised only through unit tests until the
    multi-model builder lands; every builder still binds a single server.
    """

    @pytest.mark.asyncio
    async def test_selects_named_model_and_reports_its_deployment(self):
        router = _two_model_router()
        router._pick_replica = AsyncMock(return_value=_PICK)

        result = await router.route(
            _FakeRequest(
                b'{"model":"model-b","messages":[{"role":"user","content":"hi"}]}'
            )
        )

        assert result["deployment"] == "LLMServer:model-b"
        assert (
            router._pick_replica.call_args.kwargs["handle"]
            is router._servers["model-b"]
        )

    @pytest.mark.asyncio
    async def test_sole_model_is_default_when_model_omitted(self):
        router = _new_direct_router()
        router._pick_replica = AsyncMock(return_value=_PICK)

        result = await router.route(_FakeRequest(b'{"prompt":"hi"}'))

        assert result["deployment"] == "LLMServer:x"

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "body",
        [
            b'{"prompt":"hi"}',  # well-formed, no model
            b'{"model":"model-a","prompt":"' + (b"x" * 64),  # truncated by HAProxy
            b"not json",
            b"",
        ],
    )
    async def test_multiple_models_require_readable_model_field(self, body):
        """With several models an unreadable or absent ``model`` is a 400. The
        router never guesses a deployment, and never reaches replica selection."""
        router = _two_model_router()
        router._pick_replica = AsyncMock(return_value=_PICK)

        with pytest.raises(HTTPException) as exc_info:
            await router.route(_FakeRequest(body))

        assert exc_info.value.status_code == 400
        assert "Model parameter is required" in exc_info.value.detail
        router._pick_replica.assert_not_called()

    @pytest.mark.asyncio
    async def test_unknown_model_is_404(self):
        router = _two_model_router()
        router._pick_replica = AsyncMock(return_value=_PICK)

        with pytest.raises(HTTPException) as exc_info:
            await router.route(_FakeRequest(b'{"model":"model-c","prompt":"hi"}'))

        assert exc_info.value.status_code == 404
        assert "model-c" in exc_info.value.detail
        router._pick_replica.assert_not_called()

    @pytest.mark.asyncio
    async def test_exact_model_id_wins_over_base_model_id(self):
        """A configured id that itself contains ':' must resolve exactly, not be
        stripped to a base id that maps to a different deployment."""
        router = _new_direct_router(
            servers={
                "org/model": _fake_handle("LLMServer:org--model"),
                "org/model:v2": _fake_handle("LLMServer:org--model_v2"),
            }
        )
        router._pick_replica = AsyncMock(return_value=_PICK)

        result = await router.route(
            _FakeRequest(b'{"model":"org/model:v2","prompt":"hi"}')
        )

        assert result["deployment"] == "LLMServer:org--model_v2"

    @pytest.mark.asyncio
    async def test_lora_style_id_resolves_to_base_model(self):
        router = _two_model_router()
        router._pick_replica = AsyncMock(return_value=_PICK)

        result = await router.route(
            _FakeRequest(b'{"model":"model-a:my-adapter","prompt":"hi"}')
        )

        assert result["deployment"] == "LLMServer:model-a"

    @pytest.mark.asyncio
    async def test_model_is_read_from_body_without_routing_key(self):
        """An embeddings-shaped body has no ``messages``/``prompt``, so it yields
        no routing payload -- but it still names a model and must select it."""
        router = _two_model_router()
        router._pick_replica = AsyncMock(return_value=_PICK)

        with patch.object(router_module.logger, "warning"):
            result = await router.route(
                _FakeRequest(b'{"model":"model-b","input":"hello"}')
            )

        assert result["deployment"] == "LLMServer:model-b"
        assert router._pick_replica.call_args.kwargs["routing_payload"] is None


_INGRESS_ROUTES = [
    RoutePattern(methods=["GET"], path="/v1/models"),
    RoutePattern(methods=["GET"], path="/v1/models/{model:path}"),
    RoutePattern(methods=["POST"], path="/admin/pause"),
]


def _ingress_router(routes=None):
    """Two-model router that also has an application ingress with its own routes."""
    router = _two_model_router()
    router._ingress = _fake_handle("DirectStreamingIngress")
    router._ingress_routes = ASGIRoutePatternMatcher(
        _INGRESS_ROUTES if routes is None else routes
    )
    return router


def _req(body=b'{"model":"model-b","prompt":"hi"}', *, method=None, path=None):
    """A forwarded request, optionally carrying HAProxy's method/path headers."""
    headers = {}
    if method is not None:
        headers["x-serve-request-method"] = method
    if path is not None:
        headers["x-serve-request-path"] = path
    return _FakeRequest(body, headers=headers)


class TestDirectStreamingIngressPriorityRouting:
    """A request on a route the application ingress declares goes to the ingress.

    HAProxy consults the router for every request now, not just model traffic,
    so the router is what keeps `GET /v1/models` on the control ingress instead
    of answering it from a model deployment. The ingress branch deliberately
    does none of the model-request work -- no body read, no session affinity, no
    tokenization -- because a control request has nothing to route on and its
    body may well have been truncated.
    """

    @pytest.mark.asyncio
    async def test_ingress_route_selects_the_ingress(self):
        router = _ingress_router()
        router._pick_replica = AsyncMock(return_value=_PICK)

        result = await router.route(_req(method="GET", path="/v1/models"))

        assert result["deployment"] == "DirectStreamingIngress"
        assert router._pick_replica.call_args.kwargs["handle"] is router._ingress

    @pytest.mark.asyncio
    async def test_path_converter_route_selects_the_ingress(self):
        """Model ids contain slashes, so `/v1/models/{model:path}` has to match
        across segments or per-model discovery would fall through to a model."""
        router = _ingress_router()
        router._pick_replica = AsyncMock(return_value=_PICK)

        result = await router.route(
            _req(method="GET", path="/v1/models/meta-llama/Llama-3")
        )

        assert result["deployment"] == "DirectStreamingIngress"

    @pytest.mark.asyncio
    async def test_wrong_method_on_an_ingress_path_is_not_the_ingress(self):
        """`GET /v1/models` is the ingress's; `POST /v1/models` is not, so it
        falls through to model selection."""
        router = _ingress_router()
        router._pick_replica = AsyncMock(return_value=_PICK)

        result = await router.route(
            _req(b'{"model":"model-b","prompt":"hi"}', method="POST", path="/v1/models")
        )

        assert result["deployment"] == "LLMServer:model-b"

    @pytest.mark.asyncio
    async def test_unmatched_path_proceeds_to_model_selection(self):
        router = _ingress_router()
        router._pick_replica = AsyncMock(return_value=_PICK)

        result = await router.route(_req(method="POST", path="/v1/chat/completions"))

        assert result["deployment"] == "LLMServer:model-b"
        assert (
            router._pick_replica.call_args.kwargs["handle"]
            is router._servers["model-b"]
        )

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "method,path",
        [
            (None, "/v1/models"),  # older HAProxy: no method header
            ("GET", None),  # ...or no path header
            (None, None),
        ],
    )
    async def test_missing_headers_skip_priority_matching(self, method, path):
        """Both values are required to match. Without them the router keeps its
        previous model-only behavior rather than guessing."""
        router = _ingress_router()
        router._pick_replica = AsyncMock(return_value=_PICK)

        result = await router.route(_req(method=method, path=path))

        assert result["deployment"] == "LLMServer:model-b"

    @pytest.mark.asyncio
    async def test_ingress_branch_does_not_read_the_body(self):
        """A control request's body is never parsed: it has nothing to route on,
        and HAProxy may have truncated it."""
        router = _ingress_router()
        router._pick_replica = AsyncMock(return_value=_PICK)
        request = _req(b"not json at all", method="GET", path="/v1/models")
        request.body = AsyncMock(side_effect=AssertionError("body must not be read"))

        result = await router.route(request)

        assert result["deployment"] == "DirectStreamingIngress"
        request.body.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_unparseable_body_on_an_ingress_route_still_succeeds(self):
        """The same body would be a 400 on the model path with two models
        configured. On an ingress route it is simply irrelevant."""
        router = _ingress_router()
        router._pick_replica = AsyncMock(return_value=_PICK)

        result = await router.route(_req(b"", method="GET", path="/v1/models"))

        assert result["deployment"] == "DirectStreamingIngress"

    @pytest.mark.asyncio
    async def test_ingress_branch_applies_no_session_affinity(self):
        """Session pinning is a model-deployment concern; the ingress handle is
        used as-is so `.options()` is never called on it."""
        router = _ingress_router()
        router._pick_replica = AsyncMock(return_value=_PICK)

        await router.route(
            _FakeRequest(
                b"{}",
                headers={
                    "x-serve-request-method": "GET",
                    "x-serve-request-path": "/v1/models",
                    SERVE_SESSION_ID: "session-1",
                },
            )
        )

        router._ingress.options.assert_not_called()
        assert router._pick_replica.call_args.kwargs["handle"] is router._ingress

    @pytest.mark.asyncio
    async def test_ingress_branch_does_not_tokenize(self):
        """KV-aware tokenization is for model traffic; a control request must
        not reach the tokenizer even when one is configured."""
        router = _ingress_router()
        router._pick_replica = AsyncMock(return_value=_PICK)
        router._tokenizer = MagicMock(tokenize=AsyncMock())

        result = await router.route(
            _req(
                b'{"messages":[{"role":"user","content":"hi"}]}',
                method="GET",
                path="/v1/models",
            )
        )

        assert result["deployment"] == "DirectStreamingIngress"
        router._tokenizer.tokenize.assert_not_awaited()
        assert router._pick_replica.call_args.kwargs["request_token_ids"] is None
        assert router._pick_replica.call_args.kwargs["routing_payload"] is None

    @pytest.mark.asyncio
    async def test_reports_replica_id_with_the_ingress_deployment(self):
        """HAProxy resolves the pick as [deployment][replica_id], so both levels
        have to name the ingress consistently."""
        router = _ingress_router()
        router._pick_replica = AsyncMock(return_value=_PICK)

        result = await router.route(_req(method="GET", path="/v1/models"))

        assert result["deployment"] == "DirectStreamingIngress"
        assert result["replica_id"] == _PICK[2]

    @pytest.mark.asyncio
    async def test_no_ingress_configured_never_matches(self):
        """The builders whose ingress *is* the model server pass no ingress
        handle; every request is model traffic for them."""
        router = _two_model_router()
        router._pick_replica = AsyncMock(return_value=_PICK)

        result = await router.route(_req(method="GET", path="/v1/models"))

        assert result["deployment"] == "LLMServer:model-b"


class TestDirectStreamingRouterInit:
    """Construction-time contracts of ``LLMRouter``."""

    @pytest.mark.asyncio
    async def test_empty_servers_is_rejected(self):
        with pytest.raises(ValueError, match="at least one model id"):
            await _init_router(servers={})

    @pytest.mark.asyncio
    async def test_every_model_handle_is_initialized(self):
        servers = {
            "model-a": _fake_handle("LLMServer:model-a"),
            "model-b": _fake_handle("LLMServer:model-b"),
        }
        await _init_router(servers=servers)
        for handle in servers.values():
            handle._init.assert_called_once()

    @pytest.mark.asyncio
    async def test_ingress_is_initialized_and_routes_fetched_once(self):
        """Routes are read from the running replica, not build metadata, and
        cached for this replica's life."""
        ingress = _fake_ingress_handle()

        router = await _init_router(
            servers={"model-a": _fake_handle("LLMServer:model-a")}, ingress=ingress
        )

        ingress._init.assert_called_once()
        ingress.__serve_route_patterns__.remote.assert_awaited_once()
        assert router._ingress_routes.matches("GET", "/v1/models")

    @pytest.mark.asyncio
    async def test_route_fetch_failure_fails_initialization(self):
        """Deliberately not tolerated: a router with no idea which paths the
        ingress owns would answer control requests from a model deployment."""
        ingress = _fake_ingress_handle(error=RuntimeError("replica unreachable"))

        with pytest.raises(RuntimeError, match="replica unreachable"):
            await _init_router(
                servers={"model-a": _fake_handle("LLMServer:model-a")}, ingress=ingress
            )

    @pytest.mark.asyncio
    async def test_invalid_ingress_routes_fail_initialization(self):
        ingress = _fake_ingress_handle(
            routes=[RoutePattern(methods=["GET"], path="no-leading-slash")]
        )

        with pytest.raises(AssertionError):
            await _init_router(
                servers={"model-a": _fake_handle("LLMServer:model-a")}, ingress=ingress
            )

    @pytest.mark.asyncio
    async def test_no_ingress_leaves_matching_disabled(self):
        router = await _init_router(
            servers={"model-a": _fake_handle("LLMServer:model-a")}
        )
        assert router._ingress is None
        assert router._ingress_routes is None

    @pytest.mark.asyncio
    async def test_multi_model_with_llm_config_is_rejected(self):
        """KV-aware routing keys a process-global tracker off one model; the
        builder must not reach here with several."""
        with pytest.raises(ValueError, match="exactly one"):
            await _init_router(
                servers={
                    "model-a": _fake_handle("LLMServer:model-a"),
                    "model-b": _fake_handle("LLMServer:model-b"),
                },
                llm_config=MagicMock(),
            )


class TestRoutingPayload:
    """Unit coverage for wrapping a body as a routing namespace."""

    def test_parses_chat_messages(self):
        body = b'{"model":"x","messages":[{"role":"user","content":"hi"}]}'
        payload = _parse_routing_payload(body)
        assert isinstance(payload, SimpleNamespace)
        assert payload.messages == [{"role": "user", "content": "hi"}]
        # A chat body exposes no `prompt`, so `_extract_text_from_request`
        # resolves it as a chat request. Other fields are still exposed.
        assert not hasattr(payload, "prompt")
        assert payload.model == "x"

    def test_parses_completion_prompt(self):
        payload = _parse_routing_payload(b'{"model":"x","prompt":"hello"}')
        assert isinstance(payload, SimpleNamespace)
        assert payload.prompt == "hello"
        assert not hasattr(payload, "messages")

    @pytest.mark.parametrize(
        "body",
        [
            b"",  # empty
            b'{"model":"x","prompt":"' + (b"x" * 64),  # truncated, invalid JSON
            b"not json",  # unparseable
            b"[1, 2, 3]",  # valid JSON but not an object
            b'{"model":"x","max_tokens":8}',  # object without messages or prompt
            b'{"messages":[]}',  # empty messages carry no routing signal
            b'{"prompt":""}',  # empty prompt carries no routing signal
            b'{"model":"x","input":"hello"}',  # other request type, no routing key
        ],
    )
    def test_returns_none_when_no_key_derivable(self, body):
        assert _parse_routing_payload(body) is None

    @pytest.mark.asyncio
    async def test_payload_satisfies_prefix_router_contract(self):
        """The normalized payload is read by the real
        ``PrefixCacheAffinityRouter._extract_text_from_request``, the consumer
        that regressed in #64326.

        Async so a running event loop exists for the ``PendingRequest`` default
        ``asyncio.Future``.
        """
        from ray.llm._internal.serve.routing_policies.prefix_aware.prefix_aware_router import (
            PrefixCacheAffinityRouter,
        )  # noqa: E501
        from ray.serve._private.request_router.common import PendingRequest

        # __new__ avoids the tree-actor setup in __init__. The method under test
        # only uses self for the pure `_normalize_prompt_to_string` helper.
        router = PrefixCacheAffinityRouter.__new__(PrefixCacheAffinityRouter)

        chat = _parse_routing_payload(
            b'{"messages":[{"role":"user","content":"hello world"}]}'
        )
        pr = PendingRequest(args=[chat], kwargs={}, metadata=MagicMock())
        assert router._extract_text_from_request(pr) == "hello world"

        completion = _parse_routing_payload(b'{"prompt":"hello world"}')
        pr = PendingRequest(args=[completion], kwargs={}, metadata=MagicMock())
        assert router._extract_text_from_request(pr) == "hello world"


class TestOpenAiIngress:
    @pytest.mark.parametrize("stream_batching_interval_ms", [None, 0, 10000])
    @pytest.mark.parametrize("stream", [True, False])
    @pytest.mark.asyncio
    async def test_chat(self, stream_batching_interval_ms, client, stream):
        """Tests chat streaming with different stream_batching_interval_ms values.

        0ms super fast batching (no batching)
        10000ms basically should be equivalent to non-streaming
        None is default, which is some fixed non-zero value.
        """

        # Generate 1000 chunks
        n_tokens = 1000

        response = client.chat.completions.create(
            model="llm_model_id",
            messages=[dict(role="user", content="Hello")],
            stream=stream,
            max_tokens=n_tokens,
        )

        if stream:
            text = ""
            role = None
            for chunk in response:
                if chunk.choices[0].delta.role is not None and role is None:
                    role = chunk.choices[0].delta.role
                if chunk.choices[0].delta.content:
                    text += chunk.choices[0].delta.content
        else:
            text = response.choices[0].message.content
            role = response.choices[0].message.role

        assert role == "assistant"
        assert text.strip() == " ".join([f"test_{i}" for i in range(n_tokens)])

    @pytest.mark.parametrize("stream_batching_interval_ms", [None, 0, 10000])
    @pytest.mark.parametrize("stream", [True, False])
    @pytest.mark.asyncio
    async def test_completion(self, stream_batching_interval_ms, client, stream):
        """Tests text completions streaming with different stream_batching_interval_ms values."""

        # Generate tokens
        n_tokens = 1000

        response = client.completions.create(
            model="llm_model_id",
            prompt="Hello",
            stream=stream,
            max_tokens=n_tokens,
        )

        if stream:
            text = ""
            for chunk in response:
                text += chunk.choices[0].text
        else:
            text = response.choices[0].text

        # The mock engine produces "test_0 test_1 test_2 ..." pattern
        expected_text = " ".join([f"test_{i}" for i in range(n_tokens)])
        assert text.strip() == expected_text

    @pytest.mark.parametrize("stream", [True, False])
    @pytest.mark.asyncio
    async def test_tool_call(self, client, stream):
        response = client.chat.completions.create(
            model="llm_model_id",
            messages=[
                {
                    "role": "user",
                    "content": "Can you tell me what the temperate will be in Dallas, in fahrenheit?",
                },
                {
                    "content": None,
                    "role": "assistant",
                    "tool_calls": [
                        {
                            "id": "RBS92VTjJ",
                            "function": {
                                "arguments": '{"city": "Dallas", "state": "TX", "unit": "fahrenheit"}',
                                "name": "get_current_weather",
                            },
                            "type": "function",
                        }
                    ],
                },
                {
                    "role": "tool",
                    "content": "The weather in Dallas, TX is 85 degrees fahrenheit. It is partly cloudly, with highs in the 90's.",
                    "tool_call_id": "n3OMUpydP",
                },
            ],
            stream=stream,
            max_tokens=200,
        )

        if stream:
            text = ""
            role = None
            for chunk in response:
                if chunk.choices[0].delta.role is not None and role is None:
                    role = chunk.choices[0].delta.role
                if chunk.choices[0].delta.content:
                    text += chunk.choices[0].delta.content
        else:
            text = response.choices[0].message.content
            role = response.choices[0].message.role

        assert text

    @pytest.mark.asyncio
    async def test_check_health(self, llm_config: LLMConfig):
        """Test health check functionality."""

        server = MagicMock()
        server.check_health = MagicMock()
        server.check_health.remote = AsyncMock()

        router = OpenAiIngress(
            llm_deployments={llm_config.model_id: server},
            model_cards={
                llm_config.model_id: to_model_metadata(llm_config.model_id, llm_config)
            },
        )

        await router.check_health()

    @pytest.mark.asyncio
    async def test_raw_request_info_passed_to_deployment_handle(
        self, llm_config: LLMConfig
    ):
        """Test that raw_request_info is passed to the deployment handle."""
        from ray.llm._internal.serve.core.configs.openai_api_models import (
            ChatCompletionRequest,
            ChatCompletionResponse,
        )
        from ray.llm._internal.serve.core.protocol import RawRequestInfo

        # Track if raw_request_info was received
        captured_raw_request_infos = []

        # Create a mock deployment handle that captures raw_request_info
        async def mock_chat_generator(request, raw_request_info):
            captured_raw_request_infos.append(raw_request_info)
            # Return a valid response
            yield ChatCompletionResponse(
                id="test_id",
                choices=[
                    {
                        "index": 0,
                        "message": {"role": "assistant", "content": "Hello!"},
                        "finish_reason": "stop",
                    }
                ],
                model="llm_model_id",
                object="chat.completion",
                usage={
                    "prompt_tokens": 1,
                    "completion_tokens": 1,
                    "total_tokens": 2,
                },
            )

        mock_handle = MagicMock()
        mock_handle.chat = MagicMock()
        mock_handle.chat.remote = mock_chat_generator
        # Make options() return the same mock so chat.remote is preserved
        mock_handle.options.return_value = mock_handle

        # Create router with mock handle
        router = OpenAiIngress(
            llm_deployments={llm_config.model_id: mock_handle},
            model_cards={
                llm_config.model_id: to_model_metadata(llm_config.model_id, llm_config)
            },
        )

        # Create a mock FastAPI request
        from starlette.datastructures import Headers

        mock_request = MagicMock()
        mock_headers = {
            "content-type": "application/json",
            "x-ray-serve-llm-test-header": "router-raw-request-info",
        }
        mock_request.headers = Headers(mock_headers)

        # Make a request through the router
        request_body = ChatCompletionRequest(
            model="llm_model_id",
            messages=[{"role": "user", "content": "Hello"}],
            stream=False,
        )

        await router.chat(request_body, mock_request)

        # Verify that raw_request_info was passed to the deployment handle
        assert len(captured_raw_request_infos) == 1
        assert isinstance(captured_raw_request_infos[0], RawRequestInfo)
        assert captured_raw_request_infos[0].headers == mock_headers


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
