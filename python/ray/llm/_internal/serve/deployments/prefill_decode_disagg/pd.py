"""Using Ray Serve to deploy LLM models with P/D disaggregation.
"""
import logging
from typing import Any, AsyncGenerator, Dict, Union

from ray.llm._internal.serve.configs.constants import DEFAULT_MAX_ONGOING_REQUESTS
from ray.llm._internal.serve.configs.openai_api_models import (
    ChatCompletionRequest,
    ChatCompletionResponse,
    CompletionRequest,
    CompletionResponse,
    EmbeddingRequest,
    EmbeddingResponse,
    ErrorResponse,
)
from ray.llm._internal.serve.deployments.llm.llm_server import LLMServer
from ray.serve.handle import DeploymentHandle
from ray.serve.llm import LLMConfig

logger = logging.getLogger(__name__)
RequestType = Union[ChatCompletionRequest, CompletionRequest]

DEFAULT_PD_PROXY_SERVER_OPTIONS = {
    "max_ongoing_requests": DEFAULT_MAX_ONGOING_REQUESTS,
}


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
        # TODO(lk-chen): refactor OpenAiIngress <-> LLMServer such that router
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
    def get_deployment_options(
        cls, prefill_config: "LLMConfig", decode_config: "LLMConfig"
    ) -> Dict[str, Any]:
        return DEFAULT_PD_PROXY_SERVER_OPTIONS
