"""Using Ray Serve to deploy LLM models with P/D disaggregation.
"""
import logging
import uuid
from typing import Any, AsyncGenerator, Dict, Union

from pydantic import Field

from ray import serve
from ray.llm._internal.common.base_pydantic import BaseModelExtended
from ray.llm._internal.serve.configs.openai_api_models import (
    ChatCompletionRequest,
    ChatCompletionResponse,
    CompletionRequest,
    CompletionResponse,
    EmbeddingRequest,
    EmbeddingResponse,
    ErrorResponse,
)
from ray.llm._internal.serve.configs.server_models import (
    parse_args as parse_llm_configs,
)
from ray.serve.deployment import Application
from ray.serve.handle import DeploymentHandle
from ray.serve.llm import (
    LLMConfig,
    LLMRouter,
    LLMServer,
    build_llm_deployment,
)

logger = logging.getLogger(__name__)
RequestType = Union[ChatCompletionRequest, CompletionRequest]


class PDServingArgs(BaseModelExtended):
    """Schema for P/D serving args."""

    prefill_config: Union[str, LLMConfig]
    decode_config: Union[str, LLMConfig]
    proxy_deployment_config: Dict[str, Any] = Field(
        default_factory=dict,
        description="""
            The Ray @server.deployment options for the proxy server.
        """,
    )

    def parse_args(self) -> "PDServingArgs":
        """Converts this LLMServingArgs object into an DeployArgs object."""

        def parse_configs_and_cast_type(config: Union[str, LLMConfig]) -> LLMConfig:
            # ray.serve.llm.__init__ imports internal LLMConfig, and extends it to external-facing LLMConfig.
            # parse_llm_configs returns internal LLMConfig, while {prefill, decode}_configs expect external-facing LLMConfig.
            # So the model_dump() here is to convert the type, to satisfy pydantic.
            # TODO(lk-chen): refactor llm_config parsing to avoid this model_dump, and make llm_config more reusable.
            config = parse_llm_configs([config])[0]
            return LLMConfig(**config.model_dump())

        return PDServingArgs(
            # Parse string file path into LLMConfig
            prefill_config=parse_configs_and_cast_type(self.prefill_config),
            decode_config=parse_configs_and_cast_type(self.decode_config),
            proxy_deployment_config=self.proxy_deployment_config,
        )


class PDProxyServer(LLMServer):
    _default_engine_cls = None
    """Proxy between P/D LLM servers.

    For chat and completions, proxy sends the request to the prefill server and
    then parses the response to send to the decode server.

    Args:
        prefill_server: The prefill server deployment handle.
        decode_server: The decode server deployment handle.
    """

    async def __init__(
        self,
        prefill_server: DeploymentHandle,
        decode_server: DeploymentHandle,
    ):

        # We pass `llm_config` here to let super() extract the model_id,
        # such that /v1/models endpoint can work correctly.
        # TODO(lk-chen): refactor LLMRouter <-> LLMServer such that router
        # query model_id through API, instead of passing it in as an argument.
        # We can obtain llm_config from prefill_server for obtaining model_id
        # assuming there is no mismatch between prefill and decode server.
        llm_config = await prefill_server.llm_config.remote()
        await super().__init__(llm_config)
        self.prefill_server = prefill_server.options(stream=True)
        self.decode_server = decode_server.options(stream=True)

    async def embeddings(
        self, request: EmbeddingRequest
    ) -> AsyncGenerator[EmbeddingResponse, None]:
        raise NotImplementedError("Embedding is not supported for P/D disaggregation")

    def _prepare_prefill_request(self, request: RequestType) -> RequestType:
        assert (
            getattr(request, "kv_transfer_params", None) is None
        ), "kv_transfer_params should be empty before proxy"
        prefill_request = request.model_copy(deep=True)
        prefill_request.kv_transfer_params = {
            "do_remote_decode": True,
            "do_remote_prefill": False,
            "remote_engine_id": None,
            "remote_block_ids": None,
            "remote_host": None,
            "remote_port": None,
        }
        prefill_request.max_tokens = 1
        prefill_request.stream = False

        return prefill_request

    def _prepare_decode_request(
        self,
        request: RequestType,
        prefill_chunk: Union[ChatCompletionResponse, CompletionResponse],
    ) -> RequestType:
        decode_request = request.model_copy(deep=True)
        decode_request.kv_transfer_params = prefill_chunk.kv_transfer_params

        return decode_request

    async def _handle_request(
        self,
        request: RequestType,
    ) -> AsyncGenerator[
        Union[str, ChatCompletionResponse, CompletionResponse, ErrorResponse], None
    ]:

        await self._maybe_add_request_id_to_request(request)

        if isinstance(request, ChatCompletionRequest):
            method = "chat"
        elif isinstance(request, CompletionRequest):
            method = "completions"
        else:
            raise ValueError(f"Unsupported request type: {type(request)}")

        prefill_request = self._prepare_prefill_request(request)
        prefill_gen = getattr(self.prefill_server, method).remote(prefill_request)

        prefill_chunk = await prefill_gen.__anext__()

        if isinstance(prefill_chunk, ErrorResponse):
            logger.error(f"Prefill returned error: {prefill_chunk}")
            yield prefill_chunk
            return

        decode_request = self._prepare_decode_request(request, prefill_chunk)
        decode_gen = getattr(self.decode_server, method).remote(decode_request)

        async for chunk in decode_gen:
            yield chunk

    async def chat(
        self, request: ChatCompletionRequest
    ) -> AsyncGenerator[Union[str, ChatCompletionResponse, ErrorResponse], None]:
        return self._handle_request(request)

    async def completions(
        self, request: CompletionRequest
    ) -> AsyncGenerator[Union[str, CompletionResponse, ErrorResponse], None]:
        return self._handle_request(request)

    @classmethod
    def as_deployment(cls) -> serve.Deployment:
        """Turns PDProxyServer into a Ray Serve deployment."""
        return serve.deployment()(cls)


def build_pd_openai_app(pd_serving_args: dict) -> Application:
    """Build a deployable application utilizing prefill/decode disaggregation."""

    pd_config = PDServingArgs.model_validate(pd_serving_args).parse_args()

    model_id = pd_config.decode_config.model_id
    assert model_id == pd_config.prefill_config.model_id, "P/D model id mismatch"

    for config in [pd_config.prefill_config, pd_config.decode_config]:
        if "kv_transfer_config" not in config.engine_kwargs:
            config.update_engine_kwargs(
                kv_transfer_config=dict(
                    kv_connector="NixlConnector",
                    kv_role="kv_both",
                    engine_id=str(uuid.uuid4()),
                )
            )

    prefill_deployment = build_llm_deployment(
        pd_config.prefill_config, name_prefix="Prefill:"
    )
    decode_deployment = build_llm_deployment(
        pd_config.decode_config, name_prefix="Decode:"
    )

    proxy_server_deployment = (
        PDProxyServer.as_deployment()
        .options(**pd_config.proxy_deployment_config)
        .bind(
            prefill_server=prefill_deployment,
            decode_server=decode_deployment,
        )
    )

    return LLMRouter.as_deployment().bind(llm_deployments=[proxy_server_deployment])
