"""SGLang Prefill-Decode disaggregated LLM serving.

Two-deployment graph (SGLangPDPrefillServer, SGLangPDDecodeServer) where
the decode server owns a real SGLang engine in decode mode and orchestrates
remote prefill by injecting bootstrap coordination fields per request.

Unlike the vLLM PD pattern (which waits for prefill to return kv block IDs),
SGLang prefill and decode coordinate directly via a bootstrap server using a
per-request bootstrap_room ID. The bootstrap server runs on the PREFILL
worker; the decode server fetches the prefill node's bootstrap host/port at
init and stamps it onto both sides' requests so decode's KVReceiver connects
to the right place. The decode server generates the bootstrap_room upfront and
dispatches both sides simultaneously — it does not wait for a prefill response
before starting decode.
"""

import asyncio
import logging
import secrets
from typing import AsyncGenerator, Optional, Union

from ray.llm._internal.serve.core.configs.openai_api_models import (
    ChatCompletionRequest,
    ChatCompletionResponse,
    CompletionRequest,
    CompletionResponse,
    ErrorResponse,
)
from ray.llm._internal.serve.core.protocol import RawRequestInfo
from ray.llm._internal.serve.core.server.llm_server import LLMServer
from ray.llm._internal.serve.engines.sglang.sglang_engine import SGLangServer
from ray.llm._internal.serve.utils.server_utils import get_serve_request_id
from ray.serve.handle import DeploymentHandle
from ray.serve.llm import LLMConfig

logger = logging.getLogger(__name__)

RequestType = Union[ChatCompletionRequest, CompletionRequest]


def _allocate_free_port() -> int:
    """Bind to an ephemeral port and return it, releasing the socket immediately.

    Used to pick a free ``disaggregation_bootstrap_port`` before the engine
    starts, avoiding collisions when multiple replicas land on the same node.
    """
    import socket
    from contextlib import closing

    with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as s:
        s.bind(("", 0))
        return s.getsockname()[1]


class SGLangPDPrefillServer(LLMServer):
    """Prefill-side SGLang server for P/D disaggregation.

    A standard LLMServer whose engine is launched with
    ``disaggregation_mode: prefill``. SGLang's KV bootstrap server runs on the
    prefill worker: the engine binds it to ``disaggregation_bootstrap_port`` at
    startup and registers each request's KV-sender keyed by ``bootstrap_room``.

    This server owns the bootstrap address. It allocates a free bootstrap port
    before the engine starts, caches its node IP, and exposes both to the decode
    orchestrator via ``get_bootstrap_info``. The decode server injects this
    address into both prefill and decode requests so decode's KVReceiver knows
    which prefill bootstrap server to connect to.

    No orchestration logic lives here — this server is a pure executor.
    """

    # _default_engine_cls is set so that LLMServer._get_default_engine_class()
    # returns SGLangServer without falling through to the vLLM import path.
    _default_engine_cls = SGLangServer

    async def __init__(
        self,
        llm_config: LLMConfig,
        *,
        engine_cls=None,
        model_downloader=None,
    ):
        # Allocate a free bootstrap port before the engine starts unless the
        # user pinned one. SGLang's prefill engine binds its bootstrap server
        # to this port and reads it from server_args at startup.
        if "disaggregation_bootstrap_port" not in llm_config.engine_kwargs:
            llm_config = llm_config.model_copy(deep=True)
            llm_config.engine_kwargs[
                "disaggregation_bootstrap_port"
            ] = _allocate_free_port()

        await super().__init__(
            llm_config,
            engine_cls=engine_cls,
            model_downloader=model_downloader,
        )

        import ray

        self._bootstrap_host = ray.util.get_node_ip_address()
        self._bootstrap_port = llm_config.engine_kwargs["disaggregation_bootstrap_port"]

        logger.info(
            "SGLangPDPrefillServer ready: bootstrap_host=%s bootstrap_port=%s",
            self._bootstrap_host,
            self._bootstrap_port,
        )

    def get_bootstrap_info(self) -> tuple[str, int]:
        """Return this prefill node's (bootstrap_host, bootstrap_port).

        The decode server calls this at init to learn where the prefill
        bootstrap server lives, then points its KVReceiver at that address.
        """
        return self._bootstrap_host, self._bootstrap_port

    @classmethod
    def get_deployment_options(cls, llm_config: "LLMConfig"):
        # LLMServer.get_deployment_options calls get_engine_config(), which
        # unconditionally imports vLLM. The SGLang byod image uninstalls vLLM,
        # so that path fails. SGLangServer.get_deployment_options reads GPU
        # resource requirements directly from engine_kwargs (tp_size, pp_size)
        # without touching vLLM, so we delegate to it instead.
        return SGLangServer.get_deployment_options(llm_config)


class SGLangPDDecodeServer(LLMServer):
    """Decode-side SGLang server that orchestrates remote prefill.

    Owns a real SGLang engine launched with ``disaggregation_mode: decode``
    and holds a handle to the prefill deployment.

    For each chat/completions request it:
      1. Reads the PREFILL node's bootstrap_host and bootstrap_port, fetched
         from the prefill deployment at init (the bootstrap server lives there).
      2. Generates a unique bootstrap_room integer.
      3. Sends a modified prefill request carrying the prefill bootstrap
         host/port/room so the prefill engine registers its KV-sender there.
      4. Simultaneously sends the same request to its local decode engine
         carrying the same prefill bootstrap host/port/room — the decode
         KVReceiver connects to that prefill bootstrap server and blocks
         internally waiting for the KV cache to arrive.
      5. Streams the decode response back to the client.

    The proxy is not involved in the bootstrap handshake or KV transfer.
    """

    # _default_engine_cls is set so that LLMServer._get_default_engine_class()
    # returns SGLangServer without falling through to the vLLM import path.
    _default_engine_cls = SGLangServer

    async def __init__(
        self,
        llm_config: LLMConfig,
        *,
        prefill_server: DeploymentHandle,
        engine_cls=None,
        model_downloader=None,
    ):
        # TODO: Users currently need to set disaggregation_mode manually in engine_kwargs.
        # The builder should set this automatically since it already knows which
        # config is prefill and which is decode.

        self._prefill_handle = prefill_server.options(stream=True)

        # Start the decode engine. Unlike prefill, the decode engine does not
        # run a bootstrap server — its KVReceiver connects out to prefill's.
        await super().__init__(
            llm_config,
            engine_cls=engine_cls,
            model_downloader=model_downloader,
        )

        self._decode_tp_size = llm_config.engine_kwargs.get("tp_size", 1)

        # Fetch the PREFILL node's bootstrap address. The bootstrap server runs
        # on the prefill worker, so the rendezvous host/port must point there —
        # not at this decode node. Injecting decode's own address here is the
        # classic "Connection refused / NIXL KVReceiver Exception" failure.
        self._bootstrap_host, self._bootstrap_port = await prefill_server.options(
            stream=False
        ).get_bootstrap_info.remote()

        logger.info(
            "SGLangPDDecodeServer ready: prefill bootstrap_host=%s bootstrap_port=%s tp_size=%s",
            self._bootstrap_host,
            self._bootstrap_port,
            self._decode_tp_size,
        )

    @classmethod
    def get_deployment_options(cls, llm_config: "LLMConfig"):
        # LLMServer.get_deployment_options calls get_engine_config(), which
        # unconditionally imports vLLM. The SGLang byod image uninstalls vLLM,
        # so that path fails. SGLangServer.get_deployment_options reads GPU
        # resource requirements directly from engine_kwargs (tp_size, pp_size)
        # without touching vLLM, so we delegate to it instead.
        return SGLangServer.get_deployment_options(llm_config)

    def _prepare_prefill_request(
        self, request: RequestType, bootstrap_room: int
    ) -> RequestType:
        """Inject the prefill bootstrap address so its engine binds the KV-sender there."""
        prefill_request = request.model_copy(deep=True)
        object.__setattr__(prefill_request, "bootstrap_host", self._bootstrap_host)
        object.__setattr__(prefill_request, "bootstrap_port", self._bootstrap_port)
        object.__setattr__(prefill_request, "bootstrap_room", bootstrap_room)
        object.__setattr__(prefill_request, "stream", False)
        return prefill_request

    def _prepare_decode_request(
        self, request: RequestType, bootstrap_room: int
    ) -> RequestType:
        """Inject bootstrap fields so the decode engine can rendezvous with prefill.

        SGLang's decode-side handshake (tokenizer_manager.py, decode.py) reads
        bootstrap_host/bootstrap_port directly off the request to know which
        prefill bootstrap server to connect to — not just bootstrap_room.
        """
        decode_request = request.model_copy(deep=True)
        object.__setattr__(decode_request, "bootstrap_host", self._bootstrap_host)
        object.__setattr__(decode_request, "bootstrap_port", self._bootstrap_port)
        object.__setattr__(decode_request, "bootstrap_room", bootstrap_room)
        return decode_request

    async def _run_pd_request(
        self,
        request: RequestType,
        raw_request_info: Optional[RawRequestInfo],
        method: str,
    ):
        """Orchestrate SGLang prefill (remote) then decode (local engine).

        Follows the same coroutine-returns-generator pattern as
        LLMServer._run_request: this is an async function (not a generator)
        that returns the async generator from the parent's chat/completions.

        Unlike the vLLM flow, we do not wait for a prefill response before
        starting decode — the bootstrap_room is established upfront and both
        sides coordinate directly via SGLang's bootstrap server.
        """
        request_id = get_serve_request_id()
        if request_id:
            object.__setattr__(request, "request_id", request_id)

        # Generate a unique rendezvous ID for this request.
        # Both prefill and decode engines use bootstrap_room % dp_size to
        # route to matching DP workers, so this must be a fresh value per
        # request to avoid stale room collisions.
        bootstrap_room = secrets.randbits(62)

        logger.debug(
            "SGLangPD dispatch request_id=%s bootstrap_room=%s",
            request_id,
            bootstrap_room,
        )

        prefill_request = self._prepare_prefill_request(request, bootstrap_room)
        decode_request = self._prepare_decode_request(request, bootstrap_room)

        # Fire prefill and drain it in a background task.
        # Without consuming the generator, Ray's object store may GC the remote
        # call before SGLang's prefill engine finishes, causing the engine to hang.
        prefill_gen = getattr(self._prefill_handle, method).remote(
            prefill_request, raw_request_info
        )

        async def _drain_prefill(gen):
            try:
                async for _ in gen:
                    pass
            except Exception:
                pass

        asyncio.ensure_future(_drain_prefill(prefill_gen))

        # Run decode on the local engine via the parent LLMServer method.
        # super().chat / super().completions is a coroutine that returns an
        # async generator — await it to get the generator, then return it.
        return await getattr(super(), method)(decode_request, raw_request_info)

    async def chat(
        self,
        request: ChatCompletionRequest,
        raw_request_info: Optional[RawRequestInfo] = None,
    ) -> AsyncGenerator[Union[str, ChatCompletionResponse, ErrorResponse], None]:
        return await self._run_pd_request(request, raw_request_info, "chat")

    async def completions(
        self,
        request: CompletionRequest,
        raw_request_info: Optional[RawRequestInfo] = None,
    ) -> AsyncGenerator[Union[str, CompletionResponse, ErrorResponse], None]:
        return await self._run_pd_request(request, raw_request_info, "completions")

    async def _maybe_add_request_id_to_request(self, request) -> None:
        """Override LLMServer's version: SGLang's request models don't declare
        a request_id field (unlike vLLM's), so the base implementation's direct
        attribute assignment raises ValueError. Guard on field presence and use
        object.__setattr__ in case the model is frozen.
        """
        request_id = get_serve_request_id()
        if request_id and "request_id" in type(request).model_fields:
            object.__setattr__(request, "request_id", request_id)
