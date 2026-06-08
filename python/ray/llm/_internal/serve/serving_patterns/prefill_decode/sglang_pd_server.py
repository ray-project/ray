"""SGLang Prefill-Decode disaggregated LLM serving.

Two-deployment graph (SGLangPDPrefillServer, SGLangPDDecodeServer) where
the decode server owns a real SGLang engine in decode mode and orchestrates
remote prefill by injecting bootstrap coordination fields per request.

Unlike the vLLM PD pattern (which waits for prefill to return kv block IDs),
SGLang prefill and decode coordinate directly via a bootstrap server using a
per-request bootstrap_room ID. The decode server generates the bootstrap_room
upfront and dispatches both sides simultaneously — it does not wait for a
prefill response before starting decode.
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


class SGLangPDPrefillServer(LLMServer):
    """Prefill-side SGLang server for P/D disaggregation.

    A standard LLMServer whose engine is launched with
    ``disaggregation_mode: prefill``. Accepts requests that already carry
    ``bootstrap_host``, ``bootstrap_port``, and ``bootstrap_room`` fields
    injected by the decode orchestrator, and passes them through to
    SGLang's engine via ``_build_generate_kwargs``.

    No orchestration logic lives here — this server is a pure executor.
    """

    # _default_engine_cls is set so that LLMServer._get_default_engine_class()
    # returns SGLangServer without falling through to the vLLM import path.
    _default_engine_cls = SGLangServer

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
      1. Reads its own bootstrap_host and bootstrap_port (cached at init).
      2. Generates a unique bootstrap_room integer.
      3. Sends a modified prefill request with all three bootstrap fields.
      4. Simultaneously sends the same request to its local decode engine
         with only bootstrap_room set — SGLang's engine blocks internally
         waiting for the KV cache to arrive via the bootstrap server.
      5. Streams the decode response back to the client.

    The proxy is not involved in the bootstrap handshake or KV transfer.
    """

    _default_engine_cls = SGLangServer

    async def __init__(
        self,
        llm_config: LLMConfig,
        *,
        prefill_server: DeploymentHandle,
        engine_cls=None,
        model_downloader=None,
    ):
        import socket
        from contextlib import closing

        # TODO: Users currently need to set disaggregation_mode manually in engine_kwargs.
        # The builder should set this automatically since it already knows which
        # config is prefill and which is decode.

        self._prefill_handle = prefill_server.options(stream=True)

        # Allocate a free bootstrap port before the engine starts.
        # If the user specified one explicitly, respect it.
        # Otherwise find a free port to avoid collisions when multiple
        # decode replicas land on the same node.
        if "disaggregation_bootstrap_port" not in llm_config.engine_kwargs:
            with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as s:
                s.bind(("", 0))
                free_port = s.getsockname()[1]
            llm_config = llm_config.model_copy(deep=True)
            llm_config.engine_kwargs["disaggregation_bootstrap_port"] = free_port

        # Start the engine — SGLang reads disaggregation_bootstrap_port
        # from server_args at startup.
        await super().__init__(
            llm_config,
            engine_cls=engine_cls,
            model_downloader=model_downloader,
        )

        import ray

        self._bootstrap_host = ray.util.get_node_ip_address()
        self._bootstrap_port = llm_config.engine_kwargs["disaggregation_bootstrap_port"]
        self._decode_tp_size = llm_config.engine_kwargs.get("tp_size", 1)

        logger.info(
            "SGLangPDDecodeServer ready: bootstrap_host=%s bootstrap_port=%s tp_size=%s",
            self._bootstrap_host,
            self._bootstrap_port,
            self._decode_tp_size,
        )

    def _prepare_prefill_request(
        self, request: RequestType, bootstrap_room: int
    ) -> RequestType:
        """Inject bootstrap fields so the prefill engine knows where to send KV."""
        prefill_request = request.model_copy(deep=True)
        object.__setattr__(prefill_request, "bootstrap_host", self._bootstrap_host)
        object.__setattr__(prefill_request, "bootstrap_port", self._bootstrap_port)
        object.__setattr__(prefill_request, "bootstrap_room", bootstrap_room)
        object.__setattr__(prefill_request, "stream", False)
        return prefill_request

    def _prepare_decode_request(
        self, request: RequestType, bootstrap_room: int
    ) -> RequestType:
        """Inject bootstrap_room so the decode engine can rendezvous with prefill."""
        decode_request = request.model_copy(deep=True)
        object.__setattr__(decode_request, "bootstrap_room", bootstrap_room)
        return decode_request

    async def _pd_handle_request(
        self,
        request: RequestType,
        raw_request_info: Optional[RawRequestInfo] = None,
    ) -> AsyncGenerator[
        Union[str, ChatCompletionResponse, CompletionResponse, ErrorResponse], None
    ]:
        """Orchestrate SGLang prefill (remote) then decode (local engine).

        Unlike the vLLM flow, we do not wait for a prefill response before
        starting decode — the bootstrap_room is established upfront and both
        sides coordinate directly via SGLang's bootstrap server.
        """
        request_id = get_serve_request_id()
        if request_id:
            object.__setattr__(request, "request_id", request_id)

        if isinstance(request, ChatCompletionRequest):
            method = "chat"
        elif isinstance(request, CompletionRequest):
            method = "completions"
        else:
            raise ValueError(f"Unsupported request type: {type(request)}")

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

        # Run decode on the local engine. This blocks internally until the KV
        # cache arrives from prefill via the bootstrap server, then streams tokens.
        local_gen = await getattr(super(), method)(decode_request, raw_request_info)
        async for chunk in local_gen:
            yield chunk

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

    @classmethod
    def get_deployment_options(cls, llm_config: "LLMConfig"):
        return SGLangServer.get_deployment_options(llm_config)
