"""Prefill-Decode disaggregated LLM serving: decode-as-orchestrator architecture.

3-tier graph (ingress -> PDDecodeServer -> PDPrefillServer) where the
decode deployment owns a real engine and orchestrates remote prefill.
"""

import asyncio
import contextlib
import logging
import uuid
import warnings
from typing import Any, AsyncGenerator, Dict, List, Optional, Union

from fastapi.routing import APIRoute
from starlette.requests import Request
from starlette.responses import JSONResponse, Response, StreamingResponse

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
from ray.llm._internal.serve.core.ingress.utils import (
    NON_STREAMING_RESPONSE_TYPES,
    _openai_json_wrapper,
    _peek_at_generator,
    _sanitize_chat_completion_request,
)
from ray.llm._internal.serve.core.protocol import LLMServerProtocol, RawRequestInfo
from ray.llm._internal.serve.core.server.llm_server import LLMServer
from ray.llm._internal.serve.engines.vllm.kv_transfer.base import BaseConnectorBackend
from ray.llm._internal.serve.serving_patterns.data_parallel.dp_server import DPServer
from ray.llm._internal.serve.utils.broadcast import broadcast
from ray.llm._internal.serve.utils.server_utils import (
    get_serve_request_id,
)
from ray.serve._private.http_util import session_id_from_headers
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
# Direct-streaming route helpers
# ---------------------------------------------------------------------------
#
# Direct streaming exposes the engine-native ASGI app directly on the LLM
# server replica (see ``LLMServer.__serve_build_asgi_app__``), eliminating the
# separate ``OpenAiIngress`` deployment. For P/D, the engine-native
# chat/completions routes would send traffic straight to the local decode
# engine and bypass remote prefill, so ``PDOrchestratorMixin`` re-points just
# those two routes at its own ``chat`` / ``completions`` (which orchestrate
# prefill then decode). Every other route stays engine-native, identical to
# non-P/D direct streaming.


def _strip_routes(app, path: str) -> None:
    """Remove the engine-native APIRoute(s) registered at ``path``."""
    app.routes[:] = [
        r for r in app.routes if not (isinstance(r, APIRoute) and r.path == path)
    ]


async def _pd_http_response(gen) -> Response:
    """Shape a P/D orchestration generator into an OpenAI HTTP response.

    Returns a JSON response when the first chunk is an error or a complete
    (non-streaming) response, otherwise an SSE stream. Uses the same response
    helpers as ``OpenAiIngress`` so the wire format matches the standard path.
    """
    first, gen = await _peek_at_generator(gen)
    if isinstance(first, list):
        first = first[0]
    if isinstance(first, ErrorResponse):
        return JSONResponse(
            content=first.model_dump(), status_code=first.error.code or 400
        )
    if isinstance(first, NON_STREAMING_RESPONSE_TYPES):
        return JSONResponse(content=first.model_dump())
    return StreamingResponse(_openai_json_wrapper(gen), media_type="text/event-stream")


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

    # ---- Connector backend resolution ----

    def _get_connector_backend(self) -> BaseConnectorBackend:
        """Return the connector backend that was set up during engine init.

        ``LLMConfig.setup_engine_backend()`` creates the backend and calls its
        ``setup()`` during engine initialization, storing it on the config. By the
        time a request reaches the orchestrator it must already be there — a
        missing backend means engine init was skipped, which is a bug (and a
        freshly-created, un-``setup()`` backend would mis-shape traffic, e.g. a
        MultiConnector whose sub-connectors are populated only in ``setup()``).
        Cached on first access since the request path calls this.
        """
        cached = getattr(self, "_connector_backend_cache", None)
        if cached is not None:
            return cached

        backend = getattr(self._llm_config, "kv_connector_backend", None)
        assert backend is not None, (
            "No KV-connector backend on the LLMConfig. setup_engine_backend() must "
            "run during engine init before the P/D orchestrator handles requests."
        )
        self._connector_backend_cache = backend
        return backend

    # ---- Request Preparation ----
    #
    # Thin instance delegates to the resolved backend's protocol so existing
    # callers/tests that reference these names keep working. The orchestrator
    # itself goes through ``backend.prepare_*`` directly.

    def _prepare_prefill_request(self, request: RequestType) -> RequestType:
        return self._get_connector_backend().prepare_prefill_request(
            request=request, peer=None
        )

    def _prepare_decode_request(
        self,
        request: RequestType,
        prefill_chunk: Union[ChatCompletionResponse, CompletionResponse],
    ) -> RequestType:
        return self._get_connector_backend().prepare_decode_request(
            request=request, peer=None, prefill_response=prefill_chunk
        )

    # ---- Orchestrated Request Flow ----

    async def _pd_handle_request(
        self,
        request: RequestType,
        raw_request_info: Optional[RawRequestInfo] = None,
    ) -> AsyncGenerator[
        Union[str, ChatCompletionResponse, CompletionResponse, ErrorResponse], None
    ]:
        """Orchestrate prefill (remote) then decode (local engine).

        Request shaping, peer addressing, and handoff discipline are delegated to
        the resolved KV-connector backend. With the default backend flags
        (``requires_peer_binding=False``, ``concurrent_handoff=False``) the
        control flow and calls are identical to the historical NIXL/default flow.
        """

        # Determine method name for the handle call
        if isinstance(request, ChatCompletionRequest):
            method = "chat"
        elif isinstance(request, CompletionRequest):
            method = "completions"
        else:
            raise ValueError(f"Unsupported request type: {type(request)}")

        backend = self._get_connector_backend()

        prefill_handle = self._prefill_handle
        if raw_request_info is not None:
            session_id = session_id_from_headers(raw_request_info.headers)
            if session_id:
                prefill_handle = prefill_handle.options(session_id=session_id)
        prefill_handle_method = getattr(prefill_handle, method)

        if backend.requires_peer_binding:
            # Connector needs to bind to the selected prefill replica *before*
            # dispatch (e.g. request-id-addressed transfers). Reserve a replica
            # via choose_replica, expose its metadata to the backend, then
            # dispatch onto that exact selection.
            async with prefill_handle_method.choose_replica(request) as selection:
                # The selected replica's published metadata (empty dict if none).
                peer = getattr(selection, "replica_metadata", {})
                prefill_request = backend.prepare_prefill_request(
                    request=request, peer=peer
                )

                if backend.concurrent_handoff:
                    # Concurrent handoff: start remote prefill and run local decode
                    # together, draining prefill before leaving the choose_replica
                    # context (so on_request_completed fires once).
                    decode_request = backend.prepare_decode_request(
                        request=request, peer=peer, prefill_response=None
                    )
                    prefill_resp = prefill_handle_method.dispatch(
                        selection, prefill_request, raw_request_info
                    )
                    # dispatch()'s completion accounting fires when its result
                    # completes, so the response must be drained to exhaustion
                    # inside the choose_replica context — never cancelled
                    # (prefill is clamped to a single token, so draining is
                    # bounded).
                    async for chunk in self._concurrent_decode(
                        method,
                        decode_request,
                        prefill_resp,
                        raw_request_info,
                        cancel_on_failure=False,
                    ):
                        yield chunk
                    return

                # Sequential handoff with peer binding: run prefill to its first
                # chunk, then drive local decode with the returned params.
                prefill_gen = prefill_handle_method.dispatch(
                    selection, prefill_request, raw_request_info
                )
                prefill_chunk = await prefill_gen.__anext__()
                # Drain the dispatched stream to exhaustion inside the
                # choose_replica context: dispatch()'s completion accounting
                # (queue-length cache decrement) fires when the result
                # completes. Prefill is clamped to a single token, so this is
                # at most one trivial extra iteration.
                async for _ in prefill_gen:
                    pass
                if isinstance(prefill_chunk, ErrorResponse):
                    logger.error(f"Prefill returned error: {prefill_chunk}")
                    yield prefill_chunk
                    return
                decode_request = backend.prepare_decode_request(
                    request=request, peer=peer, prefill_response=prefill_chunk
                )
                local_gen = await getattr(super(), method)(
                    decode_request, raw_request_info
                )
                async for chunk in local_gen:
                    yield chunk
                return

        # Default path: no pre-dispatch peer binding; dispatch prefill via the
        # standard handle path.
        prefill_request = backend.prepare_prefill_request(request=request, peer=None)

        if backend.concurrent_handoff:
            # Concurrent handoff: dispatch via remote() and run local decode
            # together.
            decode_request = backend.prepare_decode_request(
                request=request, peer=None, prefill_response=None
            )
            prefill_resp = prefill_handle_method.remote(
                prefill_request, raw_request_info
            )
            async for chunk in self._concurrent_decode(
                method, decode_request, prefill_resp, raw_request_info
            ):
                yield chunk
            return

        # 1. Remote prefill
        prefill_gen = prefill_handle_method.remote(prefill_request, raw_request_info)
        prefill_chunk = await prefill_gen.__anext__()

        if isinstance(prefill_chunk, ErrorResponse):
            logger.error(f"Prefill returned error: {prefill_chunk}")
            yield prefill_chunk
            return

        # 2. Local decode via super().chat / super().completions so the
        # standard LLMServer request pipeline (request_id, LoRA multiplex,
        # batch_output_stream) runs on the decode side.
        decode_request = backend.prepare_decode_request(
            request=request, peer=None, prefill_response=prefill_chunk
        )
        local_gen = await getattr(super(), method)(decode_request, raw_request_info)
        async for chunk in local_gen:
            yield chunk

    async def _concurrent_decode(
        self,
        method: str,
        decode_request: RequestType,
        prefill_resp: AsyncGenerator,
        raw_request_info: Optional[RawRequestInfo],
        *,
        cancel_on_failure: bool = True,
    ):
        """Run local decode while a remote prefill drains concurrently.

        While prefill is in flight, each decode chunk is raced against the
        prefill task so a prefill failure surfaces to the client as an
        ``ErrorResponse`` (instead of a hung — decode may be waiting on KV that
        will never arrive — or seemingly-successful decode stream). The
        background prefill task is always awaited so it never leaks on the
        prefill/decode engines.

        Args:
            method: The handle method name ("chat" or "completions").
            decode_request: The request to run on the local decode engine.
            prefill_resp: The in-flight remote prefill response stream.
            raw_request_info: Raw HTTP request info forwarded to the engine.
            cancel_on_failure: Whether to cancel the in-flight prefill when
                local decode does not complete. Must be False for
                ``dispatch()``-based prefill (the choose_replica path): its
                completion accounting fires when the response completes, so the
                stream must be drained to exhaustion, never abandoned. Prefill
                is clamped to a single token, so draining is bounded either way.
        """
        prefill_task = asyncio.ensure_future(_drain_prefill(prefill_resp))
        completed = False
        local_gen = None
        next_fut = None
        try:
            local_gen = await getattr(super(), method)(decode_request, raw_request_info)
            gen = local_gen.__aiter__()
            while True:
                # Surface a failed prefill as soon as it is observed.
                if prefill_task.done() and isinstance(
                    prefill_task.result(), ErrorResponse
                ):
                    err = prefill_task.result()
                    logger.error("Remote prefill returned error: %s", err)
                    yield err
                    return
                if next_fut is None:
                    next_fut = asyncio.ensure_future(gen.__anext__())
                # Race the next decode chunk against the in-flight prefill;
                # once prefill has completed (successfully), just stream.
                awaitables = {next_fut}
                if not prefill_task.done():
                    awaitables.add(prefill_task)
                done, _ = await asyncio.wait(
                    awaitables, return_when=asyncio.FIRST_COMPLETED
                )
                if next_fut in done:
                    try:
                        chunk = next_fut.result()
                    except StopAsyncIteration:
                        break
                    next_fut = None
                    yield chunk
                # else: prefill finished first; loop back to inspect it.
            completed = True
        finally:
            if next_fut is not None and not next_fut.done():
                next_fut.cancel()
                with contextlib.suppress(BaseException):
                    await next_fut
            if not completed:
                # Abort the local decode request if we bailed early.
                if local_gen is not None:
                    with contextlib.suppress(BaseException):
                        await local_gen.aclose()
                if cancel_on_failure:
                    prefill_task.cancel()
            try:
                err = await prefill_task
                if isinstance(err, ErrorResponse):
                    logger.error("Remote prefill returned error: %s", err)
            except asyncio.CancelledError:
                pass
            except Exception as exc:  # pragma: no cover - defensive
                logger.error("Remote prefill failed: %s", exc)

    # ---- Direct-streaming ASGI app ----

    async def __serve_build_asgi_app__(self):
        """Serve direct-streaming HTTP through P/D orchestration.

        Start from the engine-native app (same as non-P/D direct streaming)
        and re-point only ``/v1/chat/completions`` and ``/v1/completions`` at
        this server's ``chat`` / ``completions``, which run remote prefill
        then local decode. All other routes stay engine-native.
        """
        app = await super().__serve_build_asgi_app__()
        _strip_routes(app, "/v1/chat/completions")
        _strip_routes(app, "/v1/completions")

        @app.post("/v1/chat/completions")
        async def _pd_chat(body: ChatCompletionRequest, request: Request):
            body = _sanitize_chat_completion_request(body)
            raw_info = RawRequestInfo.from_starlette_request(request)
            return await _pd_http_response(await self.chat(body, raw_info))

        @app.post("/v1/completions")
        async def _pd_completions(body: CompletionRequest, request: Request):
            raw_info = RawRequestInfo.from_starlette_request(request)
            return await _pd_http_response(await self.completions(body, raw_info))

        return app

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
        backend = self._get_connector_backend()
        prefill_req = backend.prepare_prefill_request(request=dummy, peer=None)

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


async def _drain_prefill(prefill_resp) -> Optional[ErrorResponse]:
    """Consume a concurrent-handoff prefill response to completion.

    In concurrent (e.g. WRITE-mode) handoff the remote prefill produces no useful
    tokens — it only needs to run so the connector pushes/registers the KV. We
    drain it so the response is fully awaited before the ``choose_replica``
    context (if any) exits. Returns an ``ErrorResponse`` if one is observed.
    Handles both streaming (``DeploymentResponseGenerator``) and non-streaming
    (single ``DeploymentResponse``) results.
    """
    try:
        async for chunk in prefill_resp:
            if isinstance(chunk, ErrorResponse):
                return chunk
    except TypeError:
        result = await prefill_resp
        if isinstance(result, ErrorResponse):
            return result
    return None


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
