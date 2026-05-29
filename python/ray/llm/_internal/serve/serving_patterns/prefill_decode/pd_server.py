"""Prefill-Decode disaggregated LLM serving: decode-as-orchestrator architecture.

3-tier graph (ingress -> PDDecodeServer -> PDPrefillServer) where the
decode deployment owns a real engine and orchestrates remote prefill.
"""

import asyncio
import logging
import uuid
import warnings
from typing import Any, AsyncGenerator, Dict, List, Optional, Union

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
from ray.llm._internal.serve.serving_patterns.data_parallel.dp_server import DPServer
from ray.llm._internal.serve.utils.broadcast import broadcast
from ray.llm._internal.serve.utils.server_utils import (
    get_serve_request_id,
)
from ray.serve.exceptions import DeploymentUnavailableError
from ray.serve.handle import DeploymentHandle
from ray.serve.llm import LLMConfig

logger = logging.getLogger(__name__)
RequestType = Union[ChatCompletionRequest, CompletionRequest]

# TODO(Kourosh): Deprecate in Ray 2.56, remove in Ray 2.58.
DEFAULT_PD_PROXY_SERVER_OPTIONS = {
    "max_ongoing_requests": DEFAULT_MAX_ONGOING_REQUESTS,
}

_PREWARM_PROMPT = " x"
_PREWARM_MAX_TOKENS = 1
_PREWARM_RETRY_INTERVAL_S = 5.0
_PREWARM_MAX_RETRIES = 60


# ---------------------------------------------------------------------------
# Mixin: PD Orchestration Logic
# ---------------------------------------------------------------------------


class PDOrchestratorMixin:
    """Mixin that adds prefill-decode orchestration to an LLMServer subclass.

    For chat/completions requests it:
      1. Sends a modified prefill request (max_tokens=1, kv_transfer_params).
      2. Receives kv_transfer_params back from the first prefill chunk.
      3. Runs decode locally on its own engine with those params.
    """

    # Set by __init__ of the concrete class that mixes this in.
    _prefill_handle: DeploymentHandle

    # ---- Request Preparation ----

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

    # ---- Orchestrated Request Flow ----

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

        # Use parent LLMServer's chat/completions which goes through the local engine
        local_gen = await getattr(super(), method)(decode_request, raw_request_info)
        async for chunk in local_gen:
            yield chunk

    # ---- Pre-warm ----
    #
    # KV transfer connectors (e.g. NIXL) require a handshake between each
    # prefill and decode replica before real traffic can flow.  Pre-warming
    # sends a tiny dummy request through the full prefill->decode path for
    # every prefill replica so that the connector establishes its connections
    # eagerly at startup rather than on the first user request.
    # Enable via: experimental_configs={"_prewarm_prefill_decode": True}

    def _make_dummy_request(self, model_id: str) -> CompletionRequest:
        """Build the smallest valid completion request for pre-warm."""
        return CompletionRequest(
            model=model_id,
            prompt=_PREWARM_PROMPT,
            max_tokens=_PREWARM_MAX_TOKENS,
            stream=False,
            request_id=f"prewarm-{uuid.uuid4()}",
        )

    async def _maybe_prewarm(self) -> None:
        """Run one prefill->decode round-trip per P replica to complete
        the connector handshake on both sides before traffic arrives."""
        prewarm_enabled = getattr(
            self, "_llm_config", None
        ) and self._llm_config.experimental_configs.get("_prewarm_prefill_decode")
        if not prewarm_enabled:
            return

        logger.info("[PDDecodeServer] Starting pre-warm across all P replicas.")

        model_id = self._llm_config.model_id
        dummy = self._make_dummy_request(model_id)
        prefill_req = self._prepare_prefill_request(dummy)

        # Broadcast to every live P replica; retry until they are up.
        kv_params_list: List[Any] = []
        attempt = 0
        while attempt < _PREWARM_MAX_RETRIES:
            attempt += 1
            try:
                kv_params_list = await asyncio.get_event_loop().run_in_executor(
                    None,
                    lambda: broadcast(
                        self._prefill_handle,
                        method_name="prewarm_prefill",
                        args=[prefill_req],
                    ),
                )
                break
            except DeploymentUnavailableError:
                logger.info(
                    "[PDDecodeServer] PrefillServer not available yet "
                    "(attempt %d/%d); retrying in %.0fs...",
                    attempt,
                    _PREWARM_MAX_RETRIES,
                    _PREWARM_RETRY_INTERVAL_S,
                )
                await asyncio.sleep(_PREWARM_RETRY_INTERVAL_S)
            except Exception as exc:
                logger.warning(
                    "[PDDecodeServer] broadcast() attempt %d/%d failed; "
                    "retrying in %.0fs...",
                    attempt,
                    _PREWARM_MAX_RETRIES,
                    _PREWARM_RETRY_INTERVAL_S,
                    exc_info=exc,
                )
                await asyncio.sleep(_PREWARM_RETRY_INTERVAL_S)
        else:
            raise RuntimeError(
                f"[PDDecodeServer] Pre-warm failed after {_PREWARM_MAX_RETRIES} "
                f"attempts ({_PREWARM_MAX_RETRIES * _PREWARM_RETRY_INTERVAL_S:.0f}s). "
                f"PrefillServer may be permanently unavailable."
            )

        logger.info(
            "[PDDecodeServer] broadcast() reached %d P replica(s); "
            "driving local decode to complete the handshake.",
            len(kv_params_list),
        )

        # Build one decode request per P replica result.
        decode_reqs: List[CompletionRequest] = []
        for idx, kv_params in enumerate(kv_params_list):
            if not kv_params:
                logger.warning(
                    "[PDDecodeServer] P replica %d returned empty kv_params; skipping.",
                    idx,
                )
                continue
            req = dummy.model_copy(deep=True)
            req.kv_transfer_params = kv_params
            decode_reqs.append(req)

        # Run all decode requests on the local engine concurrently to trigger
        # the connector handshake on D side for each P replica.
        async def _decode_one(req: CompletionRequest, idx: int) -> None:
            async for _ in self.engine.completions(req, None):
                pass
            logger.info(
                "[PDDecodeServer] Pre-warm handshake done for P replica %d.", idx
            )

        await asyncio.gather(*[_decode_one(r, i) for i, r in enumerate(decode_reqs)])

        logger.info("[PDDecodeServer] Pre-warm complete — all P replicas registered.")


# ---------------------------------------------------------------------------
# PDPrefillServer
# ---------------------------------------------------------------------------


class PDPrefillServer(LLMServer):
    """Prefill-side LLM server for P/D disaggregation.

    This is a standard LLMServer with an additional ``prewarm_prefill``
    method used during the pre-warm handshake.
    """

    async def prewarm_prefill(
        self, prefill_request: CompletionRequest
    ) -> Optional[dict]:
        """Run one prefill pass and return kv_transfer_params as a dict.

        Returns None on error.
        """
        async for chunk in self.engine.completions(prefill_request, None):
            if hasattr(chunk, "kv_transfer_params") and chunk.kv_transfer_params:
                return chunk.kv_transfer_params
            if isinstance(chunk, ErrorResponse):
                logger.warning("[PDPrefillServer] prewarm_prefill got error: %s", chunk)
                return None
        return None


# ---------------------------------------------------------------------------
# PDDecodeServer
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
        prefill_server: DeploymentHandle,
        engine_cls=None,
        model_downloader=None,
    ):
        self._prefill_handle = prefill_server.options(stream=True)
        await super().__init__(
            llm_config,
            engine_cls=engine_cls,
            model_downloader=model_downloader,
        )
        await self._maybe_prewarm()

    async def chat(
        self,
        request: ChatCompletionRequest,
        raw_request_info: Optional[RawRequestInfo] = None,
    ) -> AsyncGenerator[Union[str, ChatCompletionResponse, ErrorResponse], None]:
        return self._pd_handle_request(request, raw_request_info)

    async def completions(
        self,
        request: CompletionRequest,
        raw_request_info: Optional[RawRequestInfo] = None,
    ) -> AsyncGenerator[Union[str, CompletionResponse, ErrorResponse], None]:
        return self._pd_handle_request(request, raw_request_info)


# ---------------------------------------------------------------------------
# DP + PD combined servers
# ---------------------------------------------------------------------------


class DPPDPrefillServer(PDPrefillServer, DPServer):
    """PDPrefillServer with data-parallel gang scheduling.

    MRO: DPPDPrefillServer -> PDPrefillServer -> DPServer -> LLMServer
    - get_deployment_options comes from DPServer (adds gang scheduling).
    - __init__ falls through to DPServer (DP master info, bundle indices)
      then LLMServer (engine setup).
    """

    pass


class DPPDDecodeServer(PDDecodeServer, DPServer):
    """PDDecodeServer with data-parallel gang scheduling.

    MRO: DPPDDecodeServer -> PDDecodeServer -> PDOrchestratorMixin
         -> DPServer -> LLMServer
    - get_deployment_options comes from DPServer (adds gang scheduling).
    - __init__ from PDDecodeServer sets _prefill_handle, then super().__init__
      flows through DPServer (DP setup) then LLMServer (engine setup).
    """

    pass


# ---------------------------------------------------------------------------
# Deprecated: PDProxyServer
# TODO(Kourosh): Deprecate, remove in Ray 2.58.
# ---------------------------------------------------------------------------


class PDProxyServer(LLMServerProtocol):
    """Proxy between P/D LLM servers.

    .. deprecated::
        ``PDProxyServer`` is deprecated. Use ``PDDecodeServer`` instead.
        This class will be removed in a future release.
    """

    async def __init__(
        self,
        prefill_server: DeploymentHandle,
        decode_server: DeploymentHandle,
    ):
        warnings.warn(
            "PDProxyServer is deprecated and will be removed in Ray 2.58. "
            "Use PDDecodeServer (decode orchestrator) and PDPrefillServer instead.",
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
