import asyncio
import contextvars
from typing import Dict, Optional, Type

from starlette.datastructures import Headers
from starlette.types import ASGIApp, Receive, Scope, Send
from vllm.outputs import RequestOutput
from vllm.sampling_params import RequestOutputKind
from vllm.v1.engine.async_llm import AsyncLLM

from ray import serve
from ray.llm._internal.serve.observability.logging import get_logger
from ray.llm._internal.serve.routing_policies.kv_aware.constants import (
    LIFECYCLE_EVENT_BROADCAST_TIMEOUT_S,
)
from ray.llm._internal.serve.routing_policies.kv_aware.kv_token_tracker import (
    get_llm_router_handle,
    get_worker_id,
)
from ray.llm._internal.serve.utils.server_utils import get_serve_request_id
from ray.serve._private.constants import SERVE_ROUTER_APPLICATION_NAME_HEADER
from ray.serve.handle import DeploymentHandle

logger = get_logger(__name__)

ROUTER_APPLICATION_DEPLOYMENT_NAME = "RouterApplication"
_router_application = contextvars.ContextVar("router_application", default=None)


class RouterApplicationLifecycleMiddleware:
    """Expose the originating router application to KV lifecycle tracking."""

    def __init__(self, app: ASGIApp) -> None:
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return
        token = _router_application.set(
            Headers(scope=scope).get(SERVE_ROUTER_APPLICATION_NAME_HEADER)
        )
        try:
            await self.app(scope, receive, send)
        finally:
            _router_application.reset(token)


class LifecycleEventForwarder:
    """Ordered, non-blocking bridge from the engine to KV routing replicas.

    Each batch reaches the replicas whose selection services received the
    reservation through the router-side background broadcast.

    ``report`` only enqueues locally, so generation never blocks on delivery.
    A single delivery task per replica drains the queue to the trackers,
    awaiting one broadcast at a time so events arrive everywhere in the order
    they were reported. Events that pile up during a call are sent together in
    the next one.
    """

    def __init__(
        self,
        handle: DeploymentHandle,
        worker_id: int,
        model_id: Optional[str] = None,
    ):
        self.handle = handle
        self.worker_id = worker_id
        self.model_id = model_id
        self._events: asyncio.Queue = asyncio.Queue()
        self._delivery_task: Optional[asyncio.Task] = None

    def report(self, method_name: str, *args) -> None:
        if self._delivery_task is None or self._delivery_task.done():
            self._delivery_task = asyncio.get_running_loop().create_task(
                self._deliver()
            )
        self._events.put_nowait((method_name, args))

    async def _deliver(self) -> None:
        while True:
            # Wait for the next event, then drain whatever queued up behind it
            # into the same batch.
            batch = [await self._events.get()]
            while not self._events.empty():
                batch.append(self._events.get_nowait())
            try:
                # return_exceptions so one failed replica cannot mask delivery
                # to the rest; failures are logged and dropped below.
                method_name = (
                    "on_model_lifecycle_events"
                    if self.model_id is not None
                    else "on_lifecycle_events"
                )
                args = (self.model_id, batch) if self.model_id is not None else (batch,)
                results = await self.handle.broadcast(method_name, *args).results_async(
                    timeout_s=LIFECYCLE_EVENT_BROADCAST_TIMEOUT_S,
                    return_exceptions=True,
                )
                errors = [r for r in results if isinstance(r, Exception)]
                if errors:
                    logger.warning(
                        "KV lifecycle events dropped on %d/%d ingress replicas: %s",
                        len(errors),
                        len(results),
                        errors[0],
                    )
            except Exception as e:
                # Best-effort delivery: any failure (no LLMRouter replica being
                # available during a redeploy, backpressure, a booking error) is
                # dropped so the engine's token stream is never disrupted; report()
                # restarts the delivery task on the next event.
                logger.warning("Dropping KV lifecycle events: %s", e)
            finally:
                for _ in batch:
                    self._events.task_done()

    async def flush(self) -> None:
        """Wait until every reported event has been delivered."""
        await self._events.join()

    def close(self) -> None:
        """Cancel the delivery task on engine shutdown."""
        if self._delivery_task is not None:
            self._delivery_task.cancel()
            self._delivery_task = None


class RequestTokenTracker:
    """Drives the request lifecycle hooks for one ``generate()`` stream."""

    def __init__(
        self,
        forwarder: LifecycleEventForwarder,
        request_id: str,
        report_decode_progress: bool = False,
    ):
        self._forwarder = forwarder
        self._request_id = request_id
        self._cumulative = 0
        self._prefill_marked = False
        self._finished = False
        self._report_decode_progress = report_decode_progress

    def on_output(self, output: RequestOutput) -> None:
        """Observe one engine ``RequestOutput`` (forwarded to the caller as-is).

        vLLM streams either DELTA chunks or a single FINAL_ONLY chunk; both
        carry only new tokens, so output progress simply accumulates.
        """
        step_tokens = sum(len(o.token_ids or []) for o in output.outputs)
        if step_tokens == 0:
            # No new tokens this step (e.g. a finish-only chunk).
            return
        self._cumulative += step_tokens
        if not self._prefill_marked:
            # The first output token signals prefill completion.
            self._prefill_marked = True
            self._forwarder.report("on_prefill_complete", self._request_id)
        if self._report_decode_progress:
            self._forwarder.report(
                "on_decode_progress", self._request_id, self._cumulative
            )

    def finish(self) -> None:
        """Report completion exactly once."""
        if not self._finished:
            self._finished = True
            self._forwarder.report("on_request_completed", self._request_id)


def enable_token_tracking(
    engine_cls: Type[AsyncLLM],
    report_decode_progress: bool = False,
    model_id: Optional[str] = None,
) -> Type[AsyncLLM]:
    """Decorator adding KV-router request lifecycle tracking."""

    class TokenTrackingEngine(engine_cls):
        _lifecycle_forwarders: Optional[
            Dict[Optional[str], LifecycleEventForwarder]
        ] = None
        _resolve_warned: bool = False
        _report_decode_progress: bool = report_decode_progress

        def _resolve_lifecycle_forwarder(self) -> Optional[LifecycleEventForwarder]:
            if self._lifecycle_forwarders is None:
                self._lifecycle_forwarders = {}
            router_application = _router_application.get()
            if router_application not in self._lifecycle_forwarders:
                try:
                    if router_application is None:
                        handle = get_llm_router_handle()
                        destination_model_id = None
                    else:
                        handle = serve.get_deployment_handle(
                            ROUTER_APPLICATION_DEPLOYMENT_NAME,
                            app_name=router_application,
                        )
                        destination_model_id = model_id
                    worker_id = get_worker_id(
                        serve.get_replica_context().replica_id.unique_id
                    )
                    self._lifecycle_forwarders[
                        router_application
                    ] = LifecycleEventForwarder(
                        handle,
                        worker_id,
                        model_id=destination_model_id,
                    )
                except Exception as e:
                    # Warn once: resolution is retried per request until it succeeds.
                    if not self._resolve_warned:
                        self._resolve_warned = True
                        logger.warning("KV token tracking disabled: %s", e)
            return self._lifecycle_forwarders.get(router_application)

        def shutdown(self, *args, **kwargs):
            if self._lifecycle_forwarders is not None:
                for forwarder in self._lifecycle_forwarders.values():
                    forwarder.close()
            return super().shutdown(*args, **kwargs)

        async def generate(self, prompt, sampling_params, request_id, *args, **kwargs):
            stream = super().generate(
                prompt, sampling_params, request_id, *args, **kwargs
            )
            forwarder = self._resolve_lifecycle_forwarder()
            # CUMULATIVE repeats output-so-far per chunk; our accounting sums
            # deltas, so skip it rather than over-count. vLLM's OpenAI layer only
            # uses DELTA/FINAL_ONLY (*Request.to_sampling_params):
            # https://github.com/vllm-project/vllm/tree/main/vllm/entrypoints/openai
            if forwarder is None or (
                sampling_params.output_kind == RequestOutputKind.CUMULATIVE
            ):
                async for output in stream:
                    yield output
                return

            lifecycle_request_id = get_serve_request_id() or request_id
            tracker = RequestTokenTracker(
                forwarder,
                lifecycle_request_id,
                report_decode_progress=self._report_decode_progress,
            )
            try:
                async for output in stream:
                    tracker.on_output(output)
                    yield output
            finally:
                tracker.finish()

    return TokenTrackingEngine
