"""Using Ray Serve to deploy LLM models with P/D disaggregation.
"""
import logging
from typing import Any, AsyncGenerator, Dict, Optional, Union

from ray.llm._internal.serve.constants import DEFAULT_MAX_ONGOING_REQUESTS
from ray.llm._internal.serve.core.configs.openai_api_models import (
    ChatCompletionRequest,
    ChatCompletionResponse,
    CompletionRequest,
    CompletionResponse,
    EmbeddingRequest,
    EmbeddingResponse,
    ErrorResponse,
)
from ray.llm._internal.serve.core.protocol import LLMServerProtocol, RawRequestInfo
from ray.llm._internal.serve.utils.server_utils import (
    get_serve_request_id,
)
from ray.serve.handle import DeploymentHandle
from ray.serve.llm import LLMConfig

logger = logging.getLogger(__name__)
RequestType = Union[ChatCompletionRequest, CompletionRequest]

DEFAULT_PD_PROXY_SERVER_OPTIONS = {
    "max_ongoing_requests": DEFAULT_MAX_ONGOING_REQUESTS,
}


class PDProxyServer(LLMServerProtocol):
    """Proxy between P/D LLM servers.

    This class implements the LLMServerProtocol but doesn't have a real engine.
    It forwards requests to prefill and decode servers for disaggregated inference.

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
        # Store llm_config from prefill_server for the model_id,
        # such that /v1/models endpoint can work correctly.
        # TODO(lk-chen): refactor OpenAiIngress <-> LLMServer such that router
        # query model_id through API, instead of passing it in as an argument.
        # We obtain llm_config from prefill_server for obtaining model_id
        # assuming there is no mismatch between prefill and decode server.
        self._llm_config = await prefill_server.llm_config.remote()
        self.prefill_server = prefill_server.options(stream=True)
        self.decode_server = decode_server.options(stream=True)

    async def start(self) -> None:
        """Start is a no-op for PDProxyServer since it's just a proxy."""
        pass

    async def check_health(self) -> None:
        """Health check is a no-op for PDProxyServer."""
        pass

    async def reset_prefix_cache(self) -> None:
        """Prefix cache reset is not supported for P/D disaggregation."""
        raise NotImplementedError(
            "reset_prefix_cache is not supported for P/D disaggregation"
        )

    async def start_profile(self) -> None:
        """Profiling is not supported for P/D disaggregation."""
        raise NotImplementedError(
            "start_profile is not supported for P/D disaggregation"
        )

    async def stop_profile(self) -> None:
        """Profiling is not supported for P/D disaggregation."""
        raise NotImplementedError(
            "stop_profile is not supported for P/D disaggregation"
        )

    async def llm_config(self) -> Optional[LLMConfig]:
        """Return the LLM configuration."""
        return self._llm_config

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

    def _maybe_add_request_id_to_request(
        self,
        request: Union[ChatCompletionRequest, CompletionRequest],
    ) -> None:
        """Add the request id to the request."""
        request_id = get_serve_request_id()
        if request_id:
            request.request_id = request_id

    async def _handle_request(
        self,
        request: RequestType,
        raw_request_info: Optional[RawRequestInfo] = None,
    ) -> AsyncGenerator[
        Union[str, ChatCompletionResponse, CompletionResponse, ErrorResponse], None
    ]:

        self._maybe_add_request_id_to_request(request)

        if isinstance(request, ChatCompletionRequest):
            method = "chat"
        elif isinstance(request, CompletionRequest):
            method = "completions"
        else:
            raise ValueError(f"Unsupported request type: {type(request)}")

        prefill_request = self._prepare_prefill_request(request)
        prefill_gen = getattr(self.prefill_server, method).remote(
            prefill_request, raw_request_info
        )

        prefill_chunk = await prefill_gen.__anext__()

        if isinstance(prefill_chunk, ErrorResponse):
            logger.error(f"Prefill returned error: {prefill_chunk}")
            yield prefill_chunk
            return

        decode_request = self._prepare_decode_request(request, prefill_chunk)
        decode_gen = getattr(self.decode_server, method).remote(
            decode_request, raw_request_info
        )

        async for chunk in decode_gen:
            yield chunk

    async def chat(
        self,
        request: ChatCompletionRequest,
        raw_request_info: Optional[RawRequestInfo] = None,
    ) -> AsyncGenerator[Union[str, ChatCompletionResponse, ErrorResponse], None]:
        return self._handle_request(request, raw_request_info)

    async def completions(
        self,
        request: CompletionRequest,
        raw_request_info: Optional[RawRequestInfo] = None,
    ) -> AsyncGenerator[Union[str, CompletionResponse, ErrorResponse], None]:
        return self._handle_request(request, raw_request_info)

    async def embeddings(
        self,
        request: EmbeddingRequest,
        raw_request_info: Optional[RawRequestInfo] = None,
    ) -> AsyncGenerator[EmbeddingResponse, None]:
        raise NotImplementedError("Embedding is not supported for P/D disaggregation")

    @classmethod
    def get_deployment_options(
        cls, prefill_config: "LLMConfig", decode_config: "LLMConfig"
    ) -> Dict[str, Any]:
        return DEFAULT_PD_PROXY_SERVER_OPTIONS
