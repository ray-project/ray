import asyncio
import copy
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
    Union,
)

from fastapi import FastAPI, Form, HTTPException, Request, status
from fastapi.middleware.cors import CORSMiddleware
from starlette.responses import JSONResponse, Response, StreamingResponse

from ray import serve
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
    CompletionRequest,
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
)
from ray.llm._internal.serve.core.ingress.middleware import (
    SetRequestIdMiddleware,
    add_exception_handling_middleware,
)
from ray.llm._internal.serve.core.ingress.utils import (
    NON_STREAMING_RESPONSE_TYPES,
    _openai_json_wrapper,
    _peek_at_generator,
    _sanitize_chat_completion_request,
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
from ray.serve._private.http_util import session_id_from_headers
from ray.serve._private.thirdparty.get_asgi_route_name import (
    RoutePattern,
    extract_route_patterns,
)
from ray.serve.handle import DeploymentHandle

# Import asyncio timeout depends on python version
if sys.version_info >= (3, 11):
    from asyncio import timeout
else:
    from async_timeout import timeout

logger = get_logger(__name__)


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


# Model-discovery routes. Defined once and shared by every ingress class so a
# request the OpenAI ingress answers on one path is answered on the same path by
# the direct-streaming control ingress.
DISCOVERY_ENDPOINTS = {
    "models": lambda app: app.get("/v1/models", response_model=ModelList),
    "model_data": lambda app: app.get(
        "/v1/models/{model:path}", response_model=ModelCard
    ),
}


DEFAULT_ENDPOINTS = {
    **DISCOVERY_ENDPOINTS,
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


def init(*, enable_docs: bool = True) -> FastAPI:
    """Build the FastAPI app shared by every Serve LLM ingress deployment.

    ``enable_docs=False`` drops FastAPI's own ``/docs``, ``/redoc`` and
    ``/openapi.json`` routes. The direct-streaming control ingress needs that:
    its route inventory is what the ingress request router uses to decide which
    requests belong to the ingress rather than to a model deployment, so every
    route it declares is a path it claims away from the models. An interactive
    schema browser for two discovery endpoints is not worth that.
    """
    docs_options = (
        {}
        if enable_docs
        else {"docs_url": None, "redoc_url": None, "openapi_url": None}
    )
    _fastapi_router_app = FastAPI(lifespan=metrics_lifespan, **docs_options)

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


def make_direct_streaming_control_ingress() -> Tuple[Type, List[RoutePattern]]:
    """Build the direct-streaming ingress class and its routing metadata.

    The endpoint map is passed explicitly rather than defaulted so the route
    inventory is stated in one place, and the FastAPI app drops the docs routes
    -- see ``init``. Route patterns are extracted from that exact app at build
    time and passed to ``LLMRouter``, so route ownership cannot drift from the
    routes served by the ingress.
    """
    app = init(enable_docs=False)
    ingress_cls = make_fastapi_ingress(
        DirectStreamingIngress,
        endpoint_map=DISCOVERY_ENDPOINTS,
        app=app,
    )
    return ingress_cls, extract_route_patterns(app)


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


class _ModelDiscovery:
    """Model-discovery state and behavior shared by the ingress classes.

    ``OpenAiIngress`` and ``DirectStreamingIngress`` answer the same two
    discovery routes in the same way; they differ only in what else they do.
    Discovery reads nothing but the model cards and the LoRA paths -- it never
    calls a model deployment -- so it factors out cleanly and both ingresses
    are guaranteed to report the same models rather than drifting apart.

    Owns its copies of ``model_cards`` and ``lora_paths``: they are the
    discovery answer, and an ingress holding a second copy is a chance for the
    two to disagree.
    """

    def __init__(
        self,
        model_cards: Dict[str, ModelCard],
        *,
        lora_paths: Optional[Dict[str, str]] = None,
        _get_lora_model_metadata_func: Optional[
            Callable[[str, str], Awaitable[Dict[str, Any]]]
        ] = None,
    ):
        self.model_cards: Dict[str, ModelCard] = dict(model_cards)
        self.lora_paths: Dict[str, str] = dict(lora_paths or {})
        self._get_lora_model_metadata_func = (
            _get_lora_model_metadata_func or self._default_get_lora_model_metadata_func
        )

    async def _default_get_lora_model_metadata_func(
        self, model_id: str, base_path: str
    ) -> Dict[str, Any]:
        return await get_lora_model_metadata(model_id, base_path)

    async def model(self, model_id: str) -> Optional[ModelCard]:
        if model_id in self.model_cards:
            return self.model_cards[model_id]

        base_model_id = get_base_model_id(model_id)
        base_path = self.lora_paths.get(base_model_id)
        if base_path is not None:
            try:
                overrides = await self._get_lora_model_metadata_func(
                    model_id, base_path
                )
                base_card = self.model_cards[base_model_id]
                return ModelCard(
                    id=model_id,
                    object="model",
                    owned_by=base_card.owned_by,
                    permission=list(base_card.permission),
                    metadata={**base_card.metadata, **overrides},
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
        for base_model_id in self.model_cards:
            # Add the base model.
            all_models[base_model_id] = await self.model(base_model_id)

            base_path = self.lora_paths.get(base_model_id)
            if base_path is not None:
                # Add all the fine-tuned models.
                lora_model_ids = get_lora_model_ids(
                    dynamic_lora_loading_path=base_path,
                    base_model_id=base_model_id,
                )
                for lora_id in lora_model_ids:
                    model_data = await self.model(lora_id)
                    if model_data is not None:
                        all_models[lora_id] = model_data

        return ModelList(data=list(all_models.values()))

    async def model_data(self, model: str) -> ModelCard:
        """OpenAI API-compliant endpoint to get one rayllm model.

        Args:
            model: The model ID (e.g. "amazon/LightGPT").

        Returns:
            The ``ModelCard`` for ``model``.
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


class OpenAiIngress(DeploymentProtocol):
    def __init__(
        self,
        llm_deployments: Dict[str, DeploymentHandle],
        model_cards: Dict[str, ModelCard],
        *,
        lora_paths: Optional[Dict[str, str]] = None,
        _get_lora_model_metadata_func: Optional[
            Callable[[str, str], Awaitable[Dict[str, Any]]]
        ] = None,
    ):
        if set(llm_deployments) != set(model_cards):
            raise ValueError(
                "llm_deployments and model_cards must have the same model IDs. "
                f"Got llm_deployments={sorted(llm_deployments)}, "
                f"model_cards={sorted(model_cards)}."
            )

        self._default_serve_handles: Dict[str, DeploymentHandle] = dict(llm_deployments)
        self._discovery = _ModelDiscovery(
            model_cards,
            lora_paths=lora_paths,
            _get_lora_model_metadata_func=_get_lora_model_metadata_func,
        )
        # Alias the discovery helper's dicts rather than taking a second copy:
        # `_get_model_id` resolves against exactly the set of models discovery
        # reports.
        self._model_cards: Dict[str, ModelCard] = self._discovery.model_cards
        self._lora_paths: Dict[str, str] = self._discovery.lora_paths

        # Configuring a ServeHandle with .options() creates a new ServeHandle
        # object, which contains a new metrics pusher and long-polling call.
        # Creating too many ServeHandles can impact event-loop and Serve Controller
        # performance, so we save configured ServeHandles here and reuse them.
        self._configured_serve_handles: Dict[str, DeploymentHandle] = {}

    async def check_health(self):
        pass

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
            if len(self._model_cards) == 1:
                model = next(iter(self._model_cards.keys()))
            else:
                raise HTTPException(
                    status.HTTP_400_BAD_REQUEST,
                    "Model parameter is required when multiple models are configured. "
                    f"Available models: {list(self._model_cards.keys())}",
                )

        base_model_id = get_base_model_id(model)
        if base_model_id not in self._model_cards:
            raise HTTPException(
                status.HTTP_404_NOT_FOUND,
                f'Got request for model "{model}". '
                f'Could not find base model with ID "{base_model_id}".',
            )

        # Return original model ID so multiplexed routing works correctly.
        return model

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

        # Propagate the session id from the client request to the downstream
        # LLMServer handle. The Serve HTTP proxy attaches session_id to the
        # *ingress* deployment handle (proxy.py:_setup_request_context), but
        # that does NOT carry over to a second handle hop (here -> LLMServer).
        # Re-read the configured session header from the raw request and apply
        # it via .options(session_id=...) so session-aware request routers
        # (e.g. ConsistentHashRouter) on the LLMServer deployment see it.
        # Uses the same case-insensitive, separator-tolerant matcher as
        # proxy.py so a `-`/`_` rewrite by an intermediate proxy doesn't
        # silently drop session affinity on this second hop.
        if raw_request is not None:
            session_id = session_id_from_headers(raw_request.headers)
            if session_id:
                model_handle = model_handle.options(session_id=session_id)

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
        return await self._discovery.model(model_id)

    async def models(self) -> ModelList:
        """OpenAI API-compliant endpoint to get all rayllm models."""
        return await self._discovery.models()

    async def model_data(self, model: str) -> ModelCard:
        """OpenAI API-compliant endpoint to get one rayllm model.

        Args:
            model: The model ID (e.g. "amazon/LightGPT").

        Returns:
            The ``ModelCard`` for ``model``.
        """
        return await self._discovery.model_data(model)

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


class DirectStreamingIngress(DeploymentProtocol):
    """Control-plane ingress for multi-model direct streaming.

    Deliberately **not** an ``OpenAiIngress`` subclass. Under direct streaming
    the ingress and the ingress request router split the application's traffic
    by route: every route this deployment declares is claimed by the ingress,
    and everything else is routed by the ``model`` field to an ``LLMServer``
    deployment whose replicas HAProxy talks to directly. Inheriting the OpenAI
    endpoints would therefore claim ``/v1/chat/completions`` for the ingress and
    put the proxy hop this feature exists to remove straight back onto the
    inference path.

    So this class exposes model discovery and nothing else:

    * ``GET /v1/models``
    * ``GET /v1/models/{model:path}``

    It holds the model deployment handles but never calls them. Keeping them in
    the constructor is what puts the model deployments in the application graph
    (Serve builds an app by walking the bound arguments), and it gives future
    control-plane operations -- LoRA registration, sleep/wake -- a handle to
    reach. Discovery itself reads only the model cards.

    Internal for the direct-streaming MVP: not exported from ``ray.serve.llm``,
    not user-subclassable, and the builder rejects a custom ingress class while
    direct streaming is on.
    """

    def __init__(
        self,
        llm_deployments: Dict[str, DeploymentHandle],
        model_cards: Dict[str, ModelCard],
        *,
        lora_paths: Optional[Dict[str, str]] = None,
        _get_lora_model_metadata_func: Optional[
            Callable[[str, str], Awaitable[Dict[str, Any]]]
        ] = None,
    ):
        if set(llm_deployments) != set(model_cards):
            raise ValueError(
                "llm_deployments and model_cards must have the same model IDs. "
                f"Got llm_deployments={sorted(llm_deployments)}, "
                f"model_cards={sorted(model_cards)}."
            )

        # Retained so the model deployments stay in the application graph and so
        # later control-plane work has somewhere to call. Never invoked for
        # discovery: a model listing must answer while a replica is busy
        # streaming, and must not fail because one model is unhealthy.
        self._llm_deployments: Dict[str, DeploymentHandle] = dict(llm_deployments)
        self._discovery = _ModelDiscovery(
            model_cards,
            lora_paths=lora_paths,
            _get_lora_model_metadata_func=_get_lora_model_metadata_func,
        )

    async def check_health(self):
        pass

    async def model(self, model_id: str) -> Optional[ModelCard]:
        return await self._discovery.model(model_id)

    async def models(self) -> ModelList:
        """OpenAI API-compliant endpoint to get all rayllm models."""
        return await self._discovery.models()

    async def model_data(self, model: str) -> ModelCard:
        """OpenAI API-compliant endpoint to get one rayllm model.

        Args:
            model: The model ID (e.g. "amazon/LightGPT").

        Returns:
            The ``ModelCard`` for ``model``.
        """
        return await self._discovery.model_data(model)

    @classmethod
    def get_deployment_options(
        cls, llm_configs: Optional[List[LLMConfig]] = None
    ) -> Dict[str, Any]:
        """Get the deployment options for the control-plane ingress.

        The lightweight-ingress defaults, minus ``OpenAiIngress``'s
        scale-to-zero behavior. That rule exists so an all-``min_replicas=0``
        app can release its GPU node entirely, but under direct streaming the
        ingress is also the only deployment that can answer ``GET /v1/models``,
        and the ingress request router needs a live ingress replica to resolve
        the discovery routes at all. Scaling it to zero would make model
        discovery -- the thing that tells a client which models exist before it
        wakes one -- the first casualty of an idle app.
        """
        return copy.deepcopy(DEFAULT_INGRESS_OPTIONS)
