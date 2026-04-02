import asyncio
import copy
import json
import os
import sys
from contextlib import asynccontextmanager
from enum import Enum
from typing import (
    Annotated,
    Any,
    AsyncGenerator,
    Awaitable,
    Callable,
    Dict,
    List,
    Optional,
    Tuple,
    Type,
    TypeVar,
    Union,
)

from fastapi import FastAPI, Form, HTTPException, Request, status
from fastapi.middleware.cors import CORSMiddleware
from starlette.responses import JSONResponse, Response, StreamingResponse

from ray import serve
from ray._common.utils import get_or_create_event_loop
from ray.llm._internal.common.utils.lora_utils import (
    get_base_model_id,
    get_lora_model_ids,
)
from ray.llm._internal.serve.constants import (
    DEFAULT_LLM_ROUTER_HTTP_TIMEOUT,
    DEFAULT_MAX_ONGOING_REQUESTS,
    DEFAULT_MAX_TARGET_ONGOING_REQUESTS,
)
from ray.llm._internal.serve.core.configs.llm_config import LLMConfig
from ray.llm._internal.serve.core.configs.openai_api_models import (
    ChatCompletionRequest,
    ChatCompletionResponse,
    ChatCompletionStreamResponse,
    CompletionRequest,
    CompletionResponse,
    CompletionStreamResponse,
    DetokenizeRequest,
    DetokenizeResponse,
    EmbeddingRequest,
    EmbeddingResponse,
    ErrorResponse,
    LLMChatResponse,
    LLMCompletionsResponse,
    LLMEmbeddingsResponse,
    LLMScoreResponse,
    LLMTranscriptionResponse,
    ModelCard,
    ModelList,
    OpenAIHTTPException,
    ScoreRequest,
    ScoreResponse,
    TokenizeCompletionRequest,
    TokenizeResponse,
    TranscriptionRequest,
    TranscriptionResponse,
    TranscriptionStreamResponse,
    to_model_metadata,
)
from ray.llm._internal.serve.core.ingress.middleware import (
    SetRequestIdMiddleware,
    add_exception_handling_middleware,
)
from ray.llm._internal.serve.core.protocol import DeploymentProtocol, RawRequestInfo
from ray.llm._internal.serve.observability.logging import get_logger
from ray.llm._internal.serve.observability.metrics.fast_api_metrics import (
    add_http_metrics_middleware,
    metrics_lifespan,
)
from ray.llm._internal.serve.utils.lora_serve_utils import (
    get_lora_model_metadata,
)
from ray.llm._internal.serve.utils.server_utils import replace_prefix
from ray.serve.handle import DeploymentHandle

# Import asyncio timeout depends on python version
if sys.version_info >= (3, 11):
    from asyncio import timeout
else:
    from async_timeout import timeout

logger = get_logger(__name__)

T = TypeVar("T")


DEFAULT_INGRESS_OPTIONS = {
    "max_ongoing_requests": DEFAULT_MAX_ONGOING_REQUESTS,
    "autoscaling_config": {
        "target_ongoing_requests": DEFAULT_MAX_TARGET_ONGOING_REQUESTS,
    },
}


def _get_min_replicas_from_llm_config(config: LLMConfig) -> Optional[int]:
    autoscaling_config = config.deployment_config.get("autoscaling_config")
    if autoscaling_config is None:
        return None
    if isinstance(autoscaling_config, dict):
        return autoscaling_config.get("min_replicas")
    return getattr(autoscaling_config, "min_replicas", None)


def _all_models_scale_to_zero(llm_configs: Optional[List[LLMConfig]]) -> bool:
    """Check if all models are configured with min_replicas == 0."""
    if not llm_configs:
        return False
    return all(_get_min_replicas_from_llm_config(config) == 0 for config in llm_configs)


# These methods correspond to functions defined in the LLMEngine class in python/ray/llm/_internal/serve/deployments/llm/llm_engine.py
class CallMethod(Enum):
    CHAT = "chat"
    COMPLETIONS = "completions"
    TRANSCRIPTIONS = "transcriptions"


NON_STREAMING_RESPONSE_TYPES = (
    ChatCompletionResponse,
    CompletionResponse,
    TranscriptionResponse,
)


def _sanitize_chat_completion_request(
    request: ChatCompletionRequest,
) -> ChatCompletionRequest:
    """Sanitize ChatCompletionRequest to fix Pydantic ValidatorIterator serialization issue.

    This addresses a known Pydantic bug where tool_calls fields become ValidatorIterator
    objects that cannot be pickled for Ray remote calls.

    Workaround logic adapted from vLLM (credits: @gcalmettes):
    - vLLM PR: https://github.com/vllm-project/vllm/pull/9951
    - Pydantic Issue: https://github.com/pydantic/pydantic/issues/9467
    - Related Issue: https://github.com/pydantic/pydantic/issues/9541
    - Official Workaround: https://github.com/pydantic/pydantic/issues/9467#issuecomment-2442097291

    TODO(seiji): Remove when we update to Pydantic v2.11+ with the fix.
    """
    for i, message in enumerate(request.messages):
        # SGLang messages are Pydantic BaseModels (no .get()); convert to dicts
        # so the same logic works for both vLLM (TypedDict) and SGLang.
        if not isinstance(message, dict):
            request.messages[i] = message = message.model_dump()

        if message.get("role") == "assistant":
            tool_calls_val = message.get("tool_calls")
            if tool_calls_val is not None:
                try:
                    request.messages[i]["tool_calls"] = list(tool_calls_val)
                except (TypeError, ValueError) as e:
                    raise ValueError(
                        "Validating messages' `tool_calls` raised an error. "
                        "Please ensure `tool_calls` are iterable of tool calls."
                    ) from e

    return request


StreamResponseType = Union[
    ChatCompletionStreamResponse, CompletionStreamResponse, TranscriptionStreamResponse
]
BatchedStreamResponseType = List[StreamResponseType]


DEFAULT_ENDPOINTS = {
    "models": lambda app: app.get("/v1/models", response_model=ModelList),
    "model_data": lambda app: app.get(
        "/v1/models/{model:path}", response_model=ModelCard
    ),
    "completions": lambda app: app.post("/v1/completions"),
    "chat": lambda app: app.post("/v1/chat/completions"),
    "embeddings": lambda app: app.post("/v1/embeddings"),
    "transcriptions": lambda app: app.post(
        "/v1/audio/transcriptions",
    ),
    "score": lambda app: app.post("/v1/score"),
    "tokenize": lambda app: app.post("/tokenize"),
    "detokenize": lambda app: app.post("/detokenize"),
}


def init() -> FastAPI:
    _fastapi_router_app = FastAPI(lifespan=metrics_lifespan)

    # NOTE: PLEASE READ CAREFULLY BEFORE MODIFYING
    #
    # FastAPI middleware is executed in LIFO (last-in, first-out) order,
    # hence maintaining current ordering is crucial as some of the middleware
    # might have data dependency on the other: for ex, telemetry middleware
    # depends on middleware generating request-id
    #
    # Add exception handling middleware
    # NOTE: This middleware should be added first such that it's intercepting
    #       exceptions from the handlers, avoiding them propagating to other
    #       middleware (for ex, telemetry)
    add_exception_handling_middleware(_fastapi_router_app)
    # Configure CORS middleware
    _fastapi_router_app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )
    # Add HTTP metrics middleware
    add_http_metrics_middleware(_fastapi_router_app)

    # Inject unique per-request ID
    #
    # NOTE: This middleware should be executed among the last (since
    # middleware is executed in LIFO).
    _fastapi_router_app.add_middleware(SetRequestIdMiddleware)

    return _fastapi_router_app


def make_fastapi_ingress(
    cls: Type,
    *,
    endpoint_map: Optional[Dict[str, Callable[[FastAPI], Callable]]] = None,
    app: Optional[FastAPI] = None,
):
    """
    Create a Ray Serve ingress deployment from a class and endpoint mapping.

    Args:
        cls: The class to convert into an ingress deployment
        endpoint_map: Dictionary mapping method names to FastAPI route
            decorators. Each value is a lambda that takes a FastAPI app and
            returns a route decorator.
        app: Optional FastAPI app to use for the ingress deployment. If not
            provided, a new FastAPI app will be created.

    Returns:
        A class decorated with @serve.ingress

    Example:
        endpoint_map = {
            "increment": lambda app: app.post("/increment"),
            "get_counter": lambda app: app.get("/counter"),
        }

        # With additional FastAPI parameters:
        endpoint_map = {
            "increment": lambda app: app.post("/increment", status_code=201, tags=["counter"]),
            "get_counter": lambda app: app.get("/counter", response_model=CounterResponse),
        }
    """

    if app is None:
        app = init()

    if endpoint_map is None:
        endpoint_map = DEFAULT_ENDPOINTS

    # Create a new class that inherits from the original to avoid modifying it
    # in-place. We populate the new class's __dict__ with decorated methods.
    class_dict = {}

    # Apply route decorators to the class methods and store them in class_dict
    for method_name, route_factory in endpoint_map.items():
        # Get the route decorator from the lambda
        route_decorator = route_factory(app)
        # Get the original method from the class
        original_method = getattr(cls, method_name)
        # Apply the decorator to the original method
        decorated_method = route_decorator(original_method)
        # Store in the class dict so it will be properly bound to new_cls
        class_dict[method_name] = decorated_method

    # Create new class with the decorated methods in its __dict__.
    # We keep the same __name__ and __qualname__ as the original class
    # so that the new class properly represents the input class.
    new_cls = type(cls.__name__, (cls,), class_dict)
    new_cls.__qualname__ = cls.__qualname__

    # Apply the serve.ingress decorator to the new class
    return serve.ingress(app)(new_cls)


def _apply_openai_json_format(
    response: Union[StreamResponseType, BatchedStreamResponseType],
) -> str:
    """Converts the stream response to OpenAI format.

    Each model response is converted to the string:
        data: <response-json1>\n\n

    The converted strings are concatenated and returned:
        data: <response-json1>\n\ndata: <response-json2>\n\n...
    """
    if isinstance(response, list):
        first_response = next(iter(response))
        if isinstance(first_response, str):
            return "".join(response)
        if isinstance(first_response, dict):
            return "".join(f"data: {json.dumps(r)}\n\n" for r in response)
        if hasattr(first_response, "model_dump_json"):
            return "".join(f"data: {r.model_dump_json()}\n\n" for r in response)
        raise ValueError(
            f"Unexpected response type: {type(first_response)}, {first_response=}"
        )
    if hasattr(response, "model_dump_json"):
        return f"data: {response.model_dump_json()}\n\n"
    if isinstance(response, str):
        return response
    raise ValueError(f"Unexpected response type: {type(response)}, {response=}")


async def _peek_at_generator(
    gen: AsyncGenerator[T, None],
) -> Tuple[T, AsyncGenerator[T, None]]:
    # Peek at the first element
    first_item = await gen.__anext__()

    # Create a new generator that yields the peeked item first
    async def new_generator() -> AsyncGenerator[T, None]:
        yield first_item
        async for item in gen:
            yield item

    return first_item, new_generator()


async def _openai_json_wrapper(
    generator: AsyncGenerator[
        Union[StreamResponseType, BatchedStreamResponseType], None
    ],
) -> AsyncGenerator[str, None]:
    """Wrapper that converts stream responses into OpenAI JSON strings.

    Args:
        generator: an async generator that yields either individual stream responses
            (StreamResponseType) or batches of stream responses (BatchedStreamResponseType).
            Each response is converted into OpenAI JSON format and streamed to the client.
            For batched responses, the items are concatenated together as a single string.

    Yields:
        String chunks in OpenAI SSE format: "data: {json}\n\n", with a final
        "data: [DONE]\n\n" to indicate completion.
    """
    async for response in generator:
        packet = _apply_openai_json_format(response)
        yield packet

    yield "data: [DONE]\n\n"


@asynccontextmanager
async def router_request_timeout(timeout_duration: float):
    try:
        async with timeout(timeout_duration):
            yield
    except asyncio.TimeoutError as e:
        raise OpenAIHTTPException(
            status_code=status.HTTP_408_REQUEST_TIMEOUT,
            message="Request server side timeout",
            internal_message=str(e),
        )


class OpenAiIngress(DeploymentProtocol):
    def __init__(
        self,
        llm_deployments: List[DeploymentHandle],
        *,
        _get_lora_model_metadata_func: Optional[
            Callable[[str, LLMConfig], Awaitable[Dict[str, Any]]]
        ] = None,
    ):
        self._default_serve_handles: Dict[str, DeploymentHandle] = {}
        self._llm_configs: Dict[str, LLMConfig] = {}

        # Configuring a ServeHandle with .options() creates a new ServeHandle
        # object, which contains a new metrics pusher and long-polling call.
        # Creating too many ServeHandles can impact event-loop and Serve Controller
        # performance, so we save configured ServeHandles here and reuse them.
        self._configured_serve_handles: Dict[str, DeploymentHandle] = {}
        self._get_lora_model_metadata_func = (
            _get_lora_model_metadata_func or self._default_get_lora_model_metadata_func
        )

        # Setup _default_serve_handles and _llm_configs asynchronously.
        self._init_completed = asyncio.Event()
        self.running_setup_task = get_or_create_event_loop().create_task(
            self._setup_handle_and_config_maps(llm_deployments=llm_deployments)
        )

        # HTTP sidecar proxy mode: routes streaming requests directly to
        # replica HTTP sidecars, bypassing the DeploymentHandle transport.
        # Sidecar endpoints are discovered from RunningReplica.sidecar_endpoint
        # via the request router (propagated through RunningReplicaInfo long-poll).
        self._sidecar_enabled = os.environ.get("RAYLLM_HTTP_SIDECAR", "0") == "1"
        self._sidecar_session = None
        import random

        self._sidecar_rr_counter = random.randint(0, 10000)

        # Install raw ASGI middleware to intercept streaming requests before
        # FastAPI routing (bypasses pydantic parsing + Starlette StreamingResponse).
        # Also install in ingress bypass mode (route-only) so /internal/route works.
        self._ingress_bypass_enabled = False
        if self._sidecar_enabled:
            get_or_create_event_loop().create_task(self._install_sidecar_middleware())
        else:
            # Check for ingress bypass after config maps are loaded
            get_or_create_event_loop().create_task(
                self._maybe_install_ingress_bypass_middleware()
            )

    async def _maybe_install_ingress_bypass_middleware(self):
        """Check if any LLMConfig has ingress_bypass enabled and install middleware."""
        await self._init_completed.wait()
        if any(
            getattr(cfg, "ingress_bypass", False) for cfg in self._llm_configs.values()
        ):
            self._ingress_bypass_enabled = True
            self._sidecar_enabled = True  # Enable sidecar path for /internal/route
            os.environ["RAYLLM_SIDECAR_ROUTE_ONLY"] = "1"
            await self._install_sidecar_middleware()

    async def _install_sidecar_middleware(self):
        """Install ASGI middleware that serves the /internal/route endpoint.

        HAProxy Lua calls this endpoint to get the sidecar address for a
        request. The ingress is only in the routing path, not the streaming
        path — tokens flow directly from the sidecar through HAProxy to the
        client.
        """
        # _asgi_app is set by ASGIAppReplicaWrapper.__init__, which runs
        # after OpenAiIngress.__init__ in the MRO. This task is created from
        # __init__ and must wait for the MRO chain to complete.
        while not hasattr(self, "_asgi_app"):
            await asyncio.sleep(0.01)

        original_app = self._asgi_app
        ingress = self

        async def sidecar_asgi_wrapper(scope, receive, send):
            if scope["type"] == "http" and scope["path"] == "/internal/route":
                if scope["method"] == "GET":
                    await ingress._handle_route_get_endpoint(scope, receive, send)
                    return
                if scope["method"] == "POST":
                    await ingress._handle_route_endpoint(scope, receive, send)
                    return
            await original_app(scope, receive, send)

        self._asgi_app = sidecar_asgi_wrapper
        logger.info("Sidecar routing middleware installed.")

    # Timing stats for /internal/route (sampled logging)
    _route_timing_count = 0
    _route_timing_sum_receive = 0.0
    _route_timing_sum_route = 0.0
    _route_timing_sum_send = 0.0
    _route_timing_log_interval = 500
    _route_pick_trace_count = 0

    async def _handle_route_get_endpoint(self, scope, receive, send):
        """Lightweight GET handler for /internal/route?model=xxx."""
        import time
        from urllib.parse import parse_qs

        import orjson

        t0 = time.monotonic()

        # Drain request body (ASGI requires it even for GET)
        message = await receive()
        while message.get("more_body", False):
            message = await receive()

        t1 = time.monotonic()

        # Extract model from query string
        query_string = scope.get("query_string", b"").decode("utf-8", errors="replace")
        params = parse_qs(query_string)
        model = params.get("model", [""])[0]

        default_model_id = (
            next(iter(self._llm_configs.keys())) if self._llm_configs else None
        )
        if not model:
            model = default_model_id
        if model and model not in self._llm_configs:
            base = get_base_model_id(model)
            if base not in self._llm_configs:
                model = None
        model_id = model or default_model_id

        if model_id is None:
            await send(
                {
                    "type": "http.response.start",
                    "status": 404,
                    "headers": [[b"content-type", b"application/json"]],
                }
            )
            await send({"type": "http.response.body", "body": b'{"error":"no model"}'})
            return

        try:
            host, port = await self._pick_routed_sidecar_endpoint(model_id)
        except Exception as e:
            logger.warning(f"Route GET endpoint: routing failed: {e}")
            await send(
                {
                    "type": "http.response.start",
                    "status": 503,
                    "headers": [[b"content-type", b"application/json"]],
                }
            )
            await send(
                {
                    "type": "http.response.body",
                    "body": b'{"error":"no sidecar available"}',
                }
            )
            return

        t2 = time.monotonic()

        resp = orjson.dumps({"host": host, "port": port})
        await send(
            {
                "type": "http.response.start",
                "status": 200,
                "headers": [
                    [b"content-type", b"application/json"],
                    [b"content-length", str(len(resp)).encode()],
                ],
            }
        )
        await send({"type": "http.response.body", "body": resp})

        t3 = time.monotonic()

        # Accumulate and log timing stats
        cls = type(self)
        cls._route_timing_count += 1
        cls._route_timing_sum_receive += t1 - t0
        cls._route_timing_sum_route += t2 - t1
        cls._route_timing_sum_send += t3 - t2
        if cls._route_timing_count % cls._route_timing_log_interval == 0:
            n = cls._route_timing_count
            logger.info(
                f"[route-timing] n={n} "
                f"avg_receive={cls._route_timing_sum_receive/n*1000:.2f}ms "
                f"avg_route={cls._route_timing_sum_route/n*1000:.2f}ms "
                f"avg_send={cls._route_timing_sum_send/n*1000:.2f}ms "
                f"avg_total={(cls._route_timing_sum_receive+cls._route_timing_sum_route+cls._route_timing_sum_send)/n*1000:.2f}ms"
            )
            cls._route_timing_count = 0
            cls._route_timing_sum_receive = 0.0
            cls._route_timing_sum_route = 0.0
            cls._route_timing_sum_send = 0.0

    async def _handle_route_endpoint(self, scope, receive, send):
        """Return sidecar endpoint for a request (called by HAProxy Lua).

        Reads the request body, extracts model, picks a sidecar endpoint via
        the request router, and returns {"host": ..., "port": ...} as JSON.
        """
        import orjson

        message = await receive()
        body_bytes = message.get("body", b"")
        if message.get("more_body", False):
            parts = [body_bytes]
            while True:
                message = await receive()
                parts.append(message.get("body", b""))
                if not message.get("more_body", False):
                    break
            body_bytes = b"".join(parts)

        try:
            parsed = orjson.loads(body_bytes)
        except (orjson.JSONDecodeError, ValueError):
            await send(
                {
                    "type": "http.response.start",
                    "status": 400,
                    "headers": [[b"content-type", b"application/json"]],
                }
            )
            await send(
                {"type": "http.response.body", "body": b'{"error":"invalid json"}'}
            )
            return

        default_model_id = (
            next(iter(self._llm_configs.keys())) if self._llm_configs else None
        )
        model = parsed.get("model", default_model_id)
        if model and model not in self._llm_configs:
            base = get_base_model_id(model)
            if base not in self._llm_configs:
                model = None
        model_id = model or default_model_id

        if model_id is None:
            await send(
                {
                    "type": "http.response.start",
                    "status": 404,
                    "headers": [[b"content-type", b"application/json"]],
                }
            )
            await send({"type": "http.response.body", "body": b'{"error":"no model"}'})
            return

        try:
            host, port = await self._pick_routed_sidecar_endpoint(model_id)
        except Exception as e:
            logger.warning(f"Route endpoint: sidecar routing failed: {e}")
            await send(
                {
                    "type": "http.response.start",
                    "status": 503,
                    "headers": [[b"content-type", b"application/json"]],
                }
            )
            await send(
                {
                    "type": "http.response.body",
                    "body": b'{"error":"no sidecar available"}',
                }
            )
            return

        resp = orjson.dumps({"host": host, "port": port})
        await send(
            {
                "type": "http.response.start",
                "status": 200,
                "headers": [[b"content-type", b"application/json"]],
            }
        )
        await send({"type": "http.response.body", "body": resp})

    async def _default_get_lora_model_metadata_func(
        self, model_id: str, llm_config: LLMConfig
    ) -> Dict[str, Any]:
        return await get_lora_model_metadata(model_id, llm_config)

    async def _setup_handle_and_config_maps(
        self, llm_deployments: List[DeploymentHandle]
    ):
        for handle in llm_deployments:
            llm_config = await handle.llm_config.remote()
            self._default_serve_handles[llm_config.model_id] = handle
            self._llm_configs[llm_config.model_id] = llm_config

        # Note (genesu): Even though we have already checked model id uniqueness in
        # `router_application()` under run.py. When we OSS this router component, users
        # would be able to directly use the lower level api and bypass that check. We
        # check it again here to ensure all the model ids are unique.
        if len(llm_deployments) != len(self._llm_configs):
            raise ValueError("Duplicate models found. Make sure model ids are unique.")

        self._init_completed.set()

    async def check_health(self):
        await self._init_completed.wait()

    def _get_configured_serve_handle(self, model_id: str):
        """Gets a ServeHandle to a model deployment.

        Configures the handle's options, and stores it in a cache.

        If the model_id includes LoRA suffix, we set the model ID as
        the multiplexed_model_id, so the request uses Serve's multiplexed
        routing logic.

        If the model_id is a base model- even if the model has LoRA
        adapters- we don't set multiplexed_model_id. Setting
        multiplexed_model_id would cause base model requests to be
        sent to a single model replica, instead of being load
        balanced across all replicas. This is undesirable for base
        model requests (unlike LoRA requests) because all the replicas
        have a copy of the base model.
        """

        if model_id not in self._configured_serve_handles:
            base_model_id = get_base_model_id(model_id)
            if base_model_id in self._default_serve_handles:
                if model_id == base_model_id:
                    default_handle = self._default_serve_handles[model_id]
                    configured_handle = default_handle.options(stream=True)
                    self._configured_serve_handles[model_id] = configured_handle
                else:
                    default_handle = self._default_serve_handles[base_model_id]
                    configured_handle = default_handle.options(
                        stream=True,
                        multiplexed_model_id=model_id,
                    )
                    self._configured_serve_handles[model_id] = configured_handle
            else:
                raise HTTPException(
                    status.HTTP_404_NOT_FOUND,
                    f'Could not find model with id "{model_id}".',
                )

        return self._configured_serve_handles[model_id]

    async def _get_model_id(self, model: Optional[str]) -> str:
        # Default to the only configured model if no model specified
        if model is None:
            if len(self._llm_configs) == 1:
                model = next(iter(self._llm_configs.keys()))
            else:
                raise HTTPException(
                    status.HTTP_400_BAD_REQUEST,
                    "Model parameter is required when multiple models are configured. "
                    f"Available models: {list(self._llm_configs.keys())}",
                )

        base_model_id = get_base_model_id(model)
        if base_model_id not in self._llm_configs:
            raise HTTPException(
                status.HTTP_404_NOT_FOUND,
                f'Got request for model "{model}". '
                f'Could not find base model with ID "{base_model_id}".',
            )

        # Return original model ID so multiplexed routing works correctly.
        return model

    def _get_replica_endpoint(self, replica) -> Optional[Tuple[str, int]]:
        """Get the direct ingress HTTP endpoint for a replica."""
        return getattr(replica, "direct_ingress_endpoint", None)

    async def _pick_routed_sidecar_endpoint(self, model_id: str) -> Tuple[str, int]:
        """Pick a replica via the request router and return its (host, port)."""
        base_model_id = get_base_model_id(model_id)
        handle = self._default_serve_handles.get(base_model_id)
        if handle is None:
            raise RuntimeError(f"No handle for model {model_id}")

        request_router = handle.get_request_router()
        if request_router is None:
            raise RuntimeError(f"Request router not initialized for {model_id}")

        replicas = list(request_router.curr_replicas.values())
        eligible_replicas = [
            r for r in replicas if self._get_replica_endpoint(r) is not None
        ]
        if not eligible_replicas:
            raise RuntimeError(f"No endpoint-enabled replicas for {model_id}")

        # Direct round-robin (stub: bypasses choose_replicas to test distribution)
        idx = self._sidecar_rr_counter % len(eligible_replicas)
        self._sidecar_rr_counter += 1
        selected = self._get_replica_endpoint(eligible_replicas[idx])

        trace_path = os.environ.get("RAYLLM_ROUTE_PICK_TRACE_PATH")
        if trace_path:
            cls = type(self)
            trace_limit = int(os.environ.get("RAYLLM_ROUTE_PICK_TRACE_LIMIT", "128"))
            if cls._route_pick_trace_count < trace_limit:
                cls._route_pick_trace_count += 1
                eligible_endpoints = [
                    self._get_replica_endpoint(replica) for replica in eligible_replicas
                ]
                trace_row = {
                    "n": cls._route_pick_trace_count,
                    "pid": os.getpid(),
                    "model_id": model_id,
                    "selected": selected,
                    "eligible": eligible_endpoints,
                }
                with open(trace_path, "a") as f:
                    f.write(json.dumps(trace_row) + "\n")

        return selected

    async def _get_response(
        self,
        *,
        body: Union[
            CompletionRequest,
            ChatCompletionRequest,
            EmbeddingRequest,
            TranscriptionRequest,
            ScoreRequest,
        ],
        call_method: str,
        raw_request: Optional[Request] = None,
    ) -> AsyncGenerator[
        Union[
            LLMChatResponse,
            LLMCompletionsResponse,
            LLMEmbeddingsResponse,
            LLMTranscriptionResponse,
            LLMScoreResponse,
        ],
        None,
    ]:
        """Calls the model deployment and returns the stream."""
        model_id = await self._get_model_id(body.model)
        model_handle = self._get_configured_serve_handle(model_id)

        # TODO(seiji): Remove when we update to Pydantic v2.11+ with the fix
        # for tool calling ValidatorIterator serialization issue.
        if isinstance(body, ChatCompletionRequest):
            body = _sanitize_chat_completion_request(body)

        # Convert Starlette request to serializable RawRequestInfo
        raw_request_info: Optional[RawRequestInfo] = None
        if raw_request is not None:
            raw_request_info = RawRequestInfo.from_starlette_request(raw_request)

        async for response in getattr(model_handle, call_method).remote(
            body, raw_request_info
        ):
            yield response

    async def model(self, model_id: str) -> Optional[ModelCard]:
        if model_id in self._llm_configs:
            return to_model_metadata(model_id, self._llm_configs[model_id])

        base_model_id = get_base_model_id(model_id)
        if (
            base_model_id in self._llm_configs
            and self._llm_configs[base_model_id].lora_config
        ):
            try:
                overrides = await self._get_lora_model_metadata_func(
                    model_id, self._llm_configs[base_model_id]
                )

                return to_model_metadata(
                    model_id=model_id,
                    model_config=self._llm_configs[base_model_id],
                    overrides=overrides,
                )
            except HTTPException:
                logger.exception(
                    "Unable to retrieve LoRA adapter config file for "
                    f'"{model_id}". Omitting it from list of available models. '
                    "Check that adapter config file exists in cloud bucket."
                )

    async def models(self) -> ModelList:
        """OpenAI API-compliant endpoint to get all rayllm models."""
        all_models = dict()
        for base_model_id, llm_config in self._llm_configs.items():
            # Add the base model.
            all_models[base_model_id] = await self.model(base_model_id)

            if llm_config.lora_config is not None:
                # Add all the fine-tuned models.
                lora_model_ids = get_lora_model_ids(
                    dynamic_lora_loading_path=llm_config.lora_config.dynamic_lora_loading_path,
                    base_model_id=base_model_id,
                )
                for lora_id in lora_model_ids:
                    model_data = await self.model(lora_id)
                    if model_data is not None:
                        all_models[lora_id] = model_data

        return ModelList(data=list(all_models.values()))

    async def model_data(self, model: str) -> ModelCard:
        """OpenAI API-compliant endpoint to get one rayllm model.

        :param model: The model ID (e.g. "amazon/LightGPT")
        """
        model = replace_prefix(model)
        model_data = await self.model(model)
        if model_data is None:
            raise OpenAIHTTPException(
                message=f"Unable to find {model}. Please ensure that the model exists and you have permission.",
                status_code=status.HTTP_404_NOT_FOUND,
                type="InvalidModel",
            )
        return model_data

    async def _process_llm_request(
        self,
        body: Union[CompletionRequest, ChatCompletionRequest, TranscriptionRequest],
        call_method: str,
        raw_request: Optional[Request] = None,
    ) -> Response:

        async with router_request_timeout(DEFAULT_LLM_ROUTER_HTTP_TIMEOUT):

            gen = self._get_response(
                body=body, call_method=call_method, raw_request=raw_request
            )

            # In streaming with batching enabled, this first response can be a list of chunks.
            initial_response, gen = await _peek_at_generator(gen)

            if isinstance(initial_response, list):
                first_chunk = initial_response[0]
            else:
                first_chunk = initial_response

            if isinstance(first_chunk, ErrorResponse):
                raise OpenAIHTTPException(
                    message=first_chunk.error.message,
                    status_code=first_chunk.error.code,
                    type=first_chunk.error.type,
                )

            if isinstance(first_chunk, NON_STREAMING_RESPONSE_TYPES):
                # Not streaming, first chunk should be a single response
                return JSONResponse(content=first_chunk.model_dump())

            # In case of streaming we need to iterate over the chunks and yield them
            openai_stream_generator = _openai_json_wrapper(gen)

            return StreamingResponse(
                openai_stream_generator, media_type="text/event-stream"
            )

    async def completions(self, body: CompletionRequest, request: Request) -> Response:
        """Given a prompt, the model will return one or more predicted completions,
        and can also return the probabilities of alternative tokens at each position.

        Args:
            body: The completion request.
            request: The raw FastAPI request object.

        Returns:
            A response object with completions.
        """
        return await self._process_llm_request(
            body, call_method=CallMethod.COMPLETIONS.value, raw_request=request
        )

    async def chat(self, body: ChatCompletionRequest, request: Request) -> Response:
        """Given a prompt, the model will return one or more predicted completions,
        and can also return the probabilities of alternative tokens at each position.

        Args:
            body: The chat completion request.
            request: The raw FastAPI request object.

        Returns:
            A response object with completions.
        """
        return await self._process_llm_request(
            body, call_method=CallMethod.CHAT.value, raw_request=request
        )

    async def embeddings(self, body: EmbeddingRequest, request: Request) -> Response:
        """Create embeddings for the provided input.

        Args:
            body: The embedding request.
            request: The raw FastAPI request object.

        Returns:
            A response object with embeddings.
        """
        async with router_request_timeout(DEFAULT_LLM_ROUTER_HTTP_TIMEOUT):
            results = self._get_response(
                body=body, call_method="embeddings", raw_request=request
            )
            result = await results.__anext__()
            if isinstance(result, ErrorResponse):
                raise OpenAIHTTPException(
                    message=result.error.message,
                    status_code=result.error.code,
                    type=result.error.type,
                )

            if isinstance(result, EmbeddingResponse):
                return JSONResponse(content=result.model_dump())

    # Annotated[..., Form()] is wrapper that is used to handle multiple form data, which is how audio is sent in transcription requests.
    # vLLM implementation for handling transcription requests: https://github.com/vllm-project/vllm/blob/0825197bee8dea547f2ab25f48afd8aea0cd2578/vllm/entrypoints/openai/api_server.py#L839.
    async def transcriptions(
        self, body: Annotated[TranscriptionRequest, Form()], request: Request
    ) -> Response:
        """Create transcription for the provided audio input.

        Args:
            body: The TranscriptionRequest object.
            request: The raw FastAPI request object.

        Returns:
            A response object with transcriptions.
        """

        return await self._process_llm_request(
            body, call_method=CallMethod.TRANSCRIPTIONS.value, raw_request=request
        )

    async def score(self, body: ScoreRequest, request: Request) -> Response:
        """Create scores for the provided text pairs.

        Note: This is a vLLM specific endpoint.

        Args:
            body: The score request containing input text pairs to score.
            request: The raw FastAPI request object.

        Returns:
            A response object with scores.
        """

        async with router_request_timeout(DEFAULT_LLM_ROUTER_HTTP_TIMEOUT):
            results = self._get_response(
                body=body, call_method="score", raw_request=request
            )
            result = await results.__anext__()
            if isinstance(result, ErrorResponse):
                raise OpenAIHTTPException(
                    message=result.error.message,
                    status_code=result.error.code,
                    type=result.error.type,
                )

            if isinstance(result, ScoreResponse):
                return JSONResponse(content=result.model_dump())

    async def tokenize(
        self, body: TokenizeCompletionRequest, request: Request
    ) -> Response:
        """Tokenize text into token IDs.

        This endpoint tokenizes the provided text prompt and returns the token IDs,
        counts, and optionally token strings.

        Note: This is a vLLM specific endpoint.

        Args:
            body: The tokenize request containing the text to tokenize.
            request: The raw FastAPI request object.

        Returns:
            A response object with token IDs and metadata.
        """
        async with router_request_timeout(DEFAULT_LLM_ROUTER_HTTP_TIMEOUT):
            results = self._get_response(
                body=body, call_method="tokenize", raw_request=request
            )
            result = await results.__anext__()
            if isinstance(result, ErrorResponse):
                raise OpenAIHTTPException(
                    message=result.error.message,
                    status_code=result.error.code,
                    type=result.error.type,
                )

            if isinstance(result, TokenizeResponse):
                return JSONResponse(content=result.model_dump())

    async def detokenize(self, body: DetokenizeRequest, request: Request) -> Response:
        """Convert token IDs back to text.

        This endpoint detokenizes the provided token IDs and returns the
        corresponding text.

        Note: This is a vLLM specific endpoint.

        Args:
            body: The detokenize request containing the token IDs.
            request: The raw FastAPI request object.

        Returns:
            A response object with the detokenized text.
        """
        async with router_request_timeout(DEFAULT_LLM_ROUTER_HTTP_TIMEOUT):
            results = self._get_response(
                body=body, call_method="detokenize", raw_request=request
            )
            result = await results.__anext__()
            if isinstance(result, ErrorResponse):
                raise OpenAIHTTPException(
                    message=result.error.message,
                    status_code=result.error.code,
                    type=result.error.type,
                )

            if isinstance(result, DetokenizeResponse):
                return JSONResponse(content=result.model_dump())

    @classmethod
    def get_deployment_options(
        cls, llm_configs: Optional[List[LLMConfig]] = None
    ) -> Dict[str, Any]:
        """Get the deployment options for the ingress deployment.

        If all models are configured with min_replicas=0 (scale-to-zero),
        the ingress will also be configured with min_replicas=0 so that
        the worker node/GPU instance can be fully released when idle.

        Args:
            llm_configs: The LLM configs to infer the number of ingress replicas from.

        Returns:
            A dictionary containing the deployment options for the ingress deployment.
        """
        options = copy.deepcopy(DEFAULT_INGRESS_OPTIONS)
        if _all_models_scale_to_zero(llm_configs):
            options.setdefault("autoscaling_config", {})["min_replicas"] = 0
        return options
