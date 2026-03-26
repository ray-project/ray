"""Prefill-Decode disaggregated LLM serving: decode-as-orchestrator architecture.

Replaces the former PDProxyServer (4-tier: ingress -> proxy -> prefill + decode)
with a 3-tier graph (ingress -> PDDecodeServer -> PDPrefillServer) where the
decode deployment owns a real engine and orchestrates remote prefill.
"""

import logging
import warnings
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
from ray.llm._internal.serve.core.server.llm_server import LLMServer
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


# ---------------------------------------------------------------------------
# Mixin: PD orchestration logic (used by both LLMServer and DPServer bases)
# ---------------------------------------------------------------------------


class PDOrchestratorMixin:
    """Mixin that adds prefill-decode orchestration to any LLMServer subclass.

    The decode deployment holds a handle to the prefill deployment.
    For chat/completions requests it:
      1. Sends a modified prefill request (max_tokens=1, kv_transfer_params).
      2. Receives kv_transfer_params back from the first prefill chunk.
      3. Runs decode **locally** on its own engine with those params.
    """

    # Set by __init__ of the concrete class that mixes this in.
    _prefill_handle: Optional[DeploymentHandle] = None

    # ---- request preparation (ported from former PDProxyServer) ----

    @staticmethod
    def _prepare_prefill_request(request: RequestType) -> RequestType:
        assert (
            getattr(request, "kv_transfer_params", None) is None
        ), "kv_transfer_params should be empty before orchestrator"
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

    @staticmethod
    def _prepare_decode_request(
        request: RequestType,
        prefill_chunk: Union[ChatCompletionResponse, CompletionResponse],
    ) -> RequestType:
        decode_request = request.model_copy(deep=True)
        decode_request.kv_transfer_params = prefill_chunk.kv_transfer_params
        return decode_request

    # ---- orchestrated request flow ----

    async def _pd_handle_request(
        self,
        request: RequestType,
        raw_request_info: Optional[RawRequestInfo] = None,
    ) -> AsyncGenerator[
        Union[str, ChatCompletionResponse, CompletionResponse, ErrorResponse], None
    ]:
        """Orchestrate prefill (remote) then decode (local engine)."""

        # Determine method name for the handle call
        if isinstance(request, ChatCompletionRequest):
            method = "chat"
        elif isinstance(request, CompletionRequest):
            method = "completions"
        else:
            raise ValueError(f"Unsupported request type: {type(request)}")

        # 1. Remote prefill
        prefill_request = self._prepare_prefill_request(request)
        prefill_gen = getattr(self._prefill_handle, method).remote(
            prefill_request, raw_request_info
        )
        prefill_chunk = await prefill_gen.__anext__()

        if isinstance(prefill_chunk, ErrorResponse):
            logger.error(f"Prefill returned error: {prefill_chunk}")
            yield prefill_chunk
            return

        # 2. Local decode via own engine
        decode_request = self._prepare_decode_request(request, prefill_chunk)

        # Add request id before running local decode
        request_id = get_serve_request_id()
        if request_id:
            decode_request.request_id = request_id

        # Use parent LLMServer's chat/completions which goes through the local engine
        local_gen = await getattr(super(), method)(decode_request, raw_request_info)
        async for chunk in local_gen:
            yield chunk

    # ---- optional pre-warm ----

    async def _maybe_prewarm(self) -> None:
        """If enabled, run a dummy prefill->decode round-trip for connector handshake."""
        prewarm_enabled = getattr(
            self, "_llm_config", None
        ) and self._llm_config.experimental_configs.get("_pre_warm_prefill_decode")
        if not prewarm_enabled or self._prefill_handle is None:
            return

        logger.info("Pre-warming prefill-decode connector handshake...")
        try:
            prewarm_gen = self._prefill_handle.prewarm_prefill.remote()
            prewarm_result = await prewarm_gen

            if prewarm_result and hasattr(prewarm_result, "kv_transfer_params"):
                logger.info(
                    "Pre-warm: received kv_transfer_params from prefill, "
                    "completing local decode handshake."
                )
            logger.info("Pre-warm complete.")
        except Exception:
            logger.warning(
                "Pre-warm prefill-decode handshake failed (non-fatal).",
                exc_info=True,
            )


# ---------------------------------------------------------------------------
# PDPrefillServer: real engine, serves as the prefill side
# ---------------------------------------------------------------------------


class PDPrefillServer(LLMServer):
    """Prefill-side LLM server for P/D disaggregation.

    This is a standard LLMServer with an additional ``prewarm_prefill``
    method used during the optional pre-warm handshake.
    """

    async def prewarm_prefill(self) -> Optional[Any]:
        """Run a minimal prefill for connector warm-up.

        Returns the first chunk (containing kv_transfer_params) so the
        decode side can complete the handshake.
        """
        logger.info("PDPrefillServer: prewarm_prefill called (no-op placeholder).")
        return None


# ---------------------------------------------------------------------------
# PDDecodeServer: real engine + orchestration
# ---------------------------------------------------------------------------


class PDDecodeServer(PDOrchestratorMixin, LLMServer):
    """Decode-side LLM server that orchestrates remote prefill.

    This deployment owns a real engine (decode config) and holds a handle
    to the prefill deployment.  For chat / completions it runs remote
    prefill first, then local decode.
    """

    async def __init__(
        self,
        llm_config: LLMConfig,
        *,
        prefill_server: Optional[DeploymentHandle] = None,
        **kwargs,
    ):
        self._prefill_handle = (
            prefill_server.options(stream=True) if prefill_server else None
        )
        # Initialize the real engine via LLMServer
        await super().__init__(llm_config, **kwargs)
        # Optionally pre-warm the connector
        await self._maybe_prewarm()

    async def chat(
        self,
        request: ChatCompletionRequest,
        raw_request_info: Optional[RawRequestInfo] = None,
    ) -> AsyncGenerator[Union[str, ChatCompletionResponse, ErrorResponse], None]:
        if self._prefill_handle is not None:
            return self._pd_handle_request(request, raw_request_info)
        return await super().chat(request, raw_request_info)

    async def completions(
        self,
        request: CompletionRequest,
        raw_request_info: Optional[RawRequestInfo] = None,
    ) -> AsyncGenerator[Union[str, CompletionResponse, ErrorResponse], None]:
        if self._prefill_handle is not None:
            return self._pd_handle_request(request, raw_request_info)
        return await super().completions(request, raw_request_info)


# ---------------------------------------------------------------------------
# Deprecated: PDProxyServer (kept for one release cycle)
# ---------------------------------------------------------------------------


class PDProxyServer(LLMServerProtocol):
    """Proxy between P/D LLM servers.

    .. deprecated::
        ``PDProxyServer`` is deprecated. The PD graph now uses
        ``PDDecodeServer`` (decode-as-orchestrator) instead of a
        separate proxy deployment. This class will be removed in a
        future release.
    """

    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)
        warnings.warn(
            "PDProxyServer is deprecated. Use PDDecodeServer instead.",
            DeprecationWarning,
            stacklevel=2,
        )

    async def __init__(
        self,
        prefill_server: DeploymentHandle,
        decode_server: DeploymentHandle,
    ):
        warnings.warn(
            "PDProxyServer is deprecated. The PD graph now uses "
            "PDDecodeServer (decode-as-orchestrator) instead of a "
            "separate proxy deployment. PDProxyServer will be removed "
            "in a future release.",
            DeprecationWarning,
            stacklevel=2,
        )
        self._llm_config = await prefill_server.llm_config.remote()
        self.prefill_server = prefill_server.options(stream=True)
        self.decode_server = decode_server.options(stream=True)

    async def start(self) -> None:
        pass

    async def check_health(self) -> None:
        pass

    async def reset_prefix_cache(self) -> None:
        raise NotImplementedError(
            "reset_prefix_cache is not supported for P/D disaggregation"
        )

    async def start_profile(self) -> None:
        raise NotImplementedError(
            "start_profile is not supported for P/D disaggregation"
        )

    async def stop_profile(self) -> None:
        raise NotImplementedError(
            "stop_profile is not supported for P/D disaggregation"
        )

    async def llm_config(self) -> Optional[LLMConfig]:
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
