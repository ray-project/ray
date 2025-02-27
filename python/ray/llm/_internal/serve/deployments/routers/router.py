import asyncio
import json
import os
import sys
from typing import (
    Any,
    AsyncGenerator,
    Awaitable,
    Callable,
    Dict,
    List,
    Optional,
    Tuple,
    Union,
)


from fastapi import FastAPI, HTTPException, status
from fastapi.middleware.cors import CORSMiddleware
from ray import serve
from ray._private.utils import get_or_create_event_loop
from ray.serve.handle import DeploymentHandle
from starlette.responses import JSONResponse, Response, StreamingResponse

from ray.llm._internal.serve.configs.constants import (
    RAYLLM_ROUTER_HTTP_TIMEOUT,
    ROUTER_TO_MODEL_REPLICA_RATIO,
    RAYLLM_ROUTER_MIN_REPLICAS,
    RAYLLM_ROUTER_INITIAL_REPLICAS,
    RAYLLM_ROUTER_MAX_REPLICAS,
    RAYLLM_ROUTER_TARGET_ONGOING_REQUESTS,
)
from ray.llm._internal.serve.observability.logging import get_logger
from ray.llm._internal.serve.observability.metrics.fast_api_metrics import (
    add_http_metrics_middleware,
    metrics_lifespan,
)
from ray.llm._internal.serve.deployments.llm.multiplex.utils import (
    get_base_model_id,
    get_lora_model_ids,
    get_lora_model_metadata,
)
from ray.llm._internal.serve.configs.openai_api_models import (
    ChatCompletionRequest,
    ChatCompletionResponse,
    ChatCompletionStreamResponse,
    CompletionRequest,
    CompletionResponse,
    CompletionStreamResponse,
    LLMChatResponse,
    LLMCompletionsResponse,
    OpenAIHTTPException,
    to_model_metadata,
)
from ray.llm._internal.serve.configs.openai_api_models_patch import (
    ErrorResponse,
)
from ray.llm._internal.serve.configs.server_models import (
    LLMConfig,
    ModelData,
    Model,
)
from ray.llm._internal.serve.deployments.routers.middleware import (
    SetRequestIdMiddleware,
    add_exception_handling_middleware,
)
from ray.llm._internal.serve.deployments.utils.server_utils import replace_prefix
from ray.serve.config import AutoscalingConfig

# Import asyncio timeout depends on python version
if sys.version_info >= (3, 11):
    from asyncio import timeout
else:
    from async_timeout import timeout

logger = get_logger(__name__)


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


fastapi_router_app = init()


def _apply_openai_json_format(
    response: Union[ChatCompletionStreamResponse, CompletionStreamResponse]
) -> str:
    """Converts a CompletionStreamResponse to OpenAI format.

    Each model response is converted to the string:
        data: <response-json1>\n\n

    The converted strings are concatenated and returned:
        data: <response-json1>\n\ndata: <response-json2>\n\n...
    """

    return "".join(f"data: {response.model_dump_json()}\n\n")


async def _openai_json_wrapper(
    generator: AsyncGenerator[
        Union[ChatCompletionStreamResponse, CompletionStreamResponse], None
    ],
    first_response: Union[ChatCompletionStreamResponse, CompletionStreamResponse],
) -> AsyncGenerator[str, None]:
    """Wrapper that converts CompletionStreamResponse into OpenAI JSON strings.

    Args:
        generator: an async generator that yields CompletionStreamResponse.
            Each response is converted into OpenAI JSON
            format. The jsonified responses from a list are concatenated
            together and yielded as a single string.
        first_response: the first CompletionStreamResponse to yield.

    Yields:
        Concatenated JSON strings that represent CompletionStreamResponse.
    """
    yield _apply_openai_json_format(first_response)

    async for response in generator:
        yield _apply_openai_json_format(response)

    yield "data: [DONE]\n\n"


async def _peek_at_openai_json_generator(
    generator: Union[LLMChatResponse, LLMCompletionsResponse],
) -> Tuple[
    Union[ChatCompletionStreamResponse, CompletionStreamResponse, ErrorResponse],
    AsyncGenerator[str, None],
]:
    """Runs one iteration of the underlying generator
    and returns the result, alongside the generator itself (with the
    first iteration still there).
    """
    first_response = await generator.__anext__()

    return first_response, _openai_json_wrapper(generator, first_response)


class LLMRouter:
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

    async def _get_response(
        self,
        *,
        body: Union[CompletionRequest, ChatCompletionRequest],
        call_method: str,
    ) -> AsyncGenerator[Union[LLMChatResponse, LLMCompletionsResponse], None]:
        """Calls the model deployment and returns the stream."""
        model: str = body.model
        base_model_id = get_base_model_id(model)
        if base_model_id not in self._llm_configs:
            raise HTTPException(
                status.HTTP_404_NOT_FOUND,
                f'Got request for model "{model}". '
                f'Could not find base model with ID "{base_model_id}".',
            )

        model_handle = self._get_configured_serve_handle(model)

        async for response in getattr(model_handle, call_method).remote(body):
            yield response

    async def model(self, model_id: str) -> Optional[ModelData]:
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

    @fastapi_router_app.get("/v1/models", response_model=Model)
    async def models(self) -> Model:
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

        return Model(data=list(all_models.values()))

    # :path allows us to have slashes in the model name
    @fastapi_router_app.get("/v1/models/{model:path}", response_model=ModelData)
    async def model_data(self, model: str) -> ModelData:
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

    @fastapi_router_app.post("/v1/completions")
    async def completions(self, body: CompletionRequest) -> Response:
        """Given a prompt, the model will return one or more predicted completions,
        and can also return the probabilities of alternative tokens at each position.

        Returns:
            A response object with completions.
        """
        async with timeout(RAYLLM_ROUTER_HTTP_TIMEOUT):
            results = self._get_response(body=body, call_method="completions")
            if body.stream:
                first_response, wrapper = await _peek_at_openai_json_generator(results)
                if isinstance(first_response, ErrorResponse):
                    raise OpenAIHTTPException(
                        message=first_response.message,
                        status_code=first_response.code,
                        type=first_response.type,
                    )
                return StreamingResponse(wrapper, media_type="text/event-stream")

            result = await results.__anext__()
            if isinstance(result, ErrorResponse):
                raise OpenAIHTTPException(
                    message=result.message,
                    status_code=result.code,
                    type=result.type,
                )

            if isinstance(result, CompletionResponse):
                return JSONResponse(content=result.model_dump())

    @fastapi_router_app.post("/v1/chat/completions")
    async def chat(self, body: ChatCompletionRequest) -> Response:
        """Given a prompt, the model will return one or more predicted completions,
        and can also return the probabilities of alternative tokens at each position.

        Returns:
            A response object with completions.
        """

        async with timeout(RAYLLM_ROUTER_HTTP_TIMEOUT):
            results = self._get_response(body=body, call_method="chat")
            if body.stream:
                first_response, wrapper = await _peek_at_openai_json_generator(results)
                if isinstance(first_response, ErrorResponse):
                    raise OpenAIHTTPException(
                        message=first_response.message,
                        status_code=first_response.code,
                        type=first_response.type,
                    )
                return StreamingResponse(wrapper, media_type="text/event-stream")

            result = await results.__anext__()
            if isinstance(result, ErrorResponse):
                raise OpenAIHTTPException(
                    message=result.message,
                    status_code=result.code,
                    type=result.type,
                )

            if isinstance(result, ChatCompletionResponse):
                return JSONResponse(content=result.model_dump())

    @classmethod
    def as_deployment(
        cls, llm_configs: Optional[List[LLMConfig]] = None
    ) -> serve.Deployment:
        """Converts this class to a Ray Serve deployment with ingress.

        Returns:
            A Ray Serve deployment.
        """
        min_replicas = RAYLLM_ROUTER_MIN_REPLICAS
        initial_replicas = RAYLLM_ROUTER_INITIAL_REPLICAS
        max_replicas = RAYLLM_ROUTER_MAX_REPLICAS

        # Note (genesu): Based on our internal benchmark, we are currently bottleneck
        # by the router replicas during high concurrency situation. We are setting the
        # router replicas to be ~2x the total model replicas and making it scale faster.
        if llm_configs:
            model_min_replicas = 0
            model_initial_replicas = 0
            model_max_replicas = 0
            for llm_config in llm_configs:
                if "autoscaling_config" in llm_config.deployment_config:
                    autoscaling_config = llm_config.deployment_config[
                        "autoscaling_config"
                    ]
                    if isinstance(autoscaling_config, dict):
                        autoscaling_config = AutoscalingConfig(
                            **llm_config.deployment_config["autoscaling_config"]
                        )
                else:
                    # When autoscaling config is not provided, we use the default.
                    autoscaling_config = AutoscalingConfig()
                model_min_replicas += autoscaling_config.min_replicas
                model_initial_replicas += (
                    autoscaling_config.initial_replicas
                    or autoscaling_config.min_replicas
                )
                model_max_replicas += autoscaling_config.max_replicas
            min_replicas = int(model_min_replicas * ROUTER_TO_MODEL_REPLICA_RATIO)
            initial_replicas = int(
                model_initial_replicas * ROUTER_TO_MODEL_REPLICA_RATIO
            )
            max_replicas = int(model_max_replicas * ROUTER_TO_MODEL_REPLICA_RATIO)

        ingress_cls = serve.ingress(fastapi_router_app)(cls)
        deployment_decorator = serve.deployment(
            autoscaling_config={
                "min_replicas": min_replicas,
                "initial_replicas": initial_replicas,
                "max_replicas": max_replicas,
                "target_ongoing_requests": RAYLLM_ROUTER_TARGET_ONGOING_REQUESTS,
            },
            ray_actor_options=json.loads(
                os.environ.get("RAYLLM_ROUTER_RAY_ACTOR_OPTIONS", "{}")
            ),
            max_ongoing_requests=1000,  # Maximum backlog for a single replica
        )

        deployment_cls = deployment_decorator(ingress_cls)

        return deployment_cls
