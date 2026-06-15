import asyncio
from typing import Any, Mapping, Optional, Type

from vllm.outputs import RequestOutput
from vllm.sampling_params import RequestOutputKind
from vllm.v1.engine.async_llm import AsyncLLM

from ray import serve
from ray.actor import ActorHandle
from ray.llm._internal.serve.observability.logging import get_logger
from ray.llm._internal.serve.routing_policies.kv_aware.kv_aware_actor import (
    KV_ROUTER_ACTOR_NAME,
    get_worker_id,
)

logger = get_logger(__name__)


def _prompt_token_count(prompt: Any) -> int:
    """Token count of a pre-tokenized engine prompt, or ``0`` if unknown.

    vLLM's OpenAI serving layer always tokenizes before calling ``generate``,
    passing a ``TokensPrompt``-style mapping with ``prompt_token_ids``; only
    out-of-band engine calls (e.g. a raw text prompt) report an unknown count.
    """
    if isinstance(prompt, Mapping):
        token_ids = prompt.get("prompt_token_ids")
        if token_ids is not None:
            return len(token_ids)
    return 0


class KVRouterReporter:
    """Ordered, non-blocking bridge from the engine to the KV router actor.

    ``report`` only enqueues locally, so generation never blocks on the actor.
    A single pump task per replica then delivers the queued events as
    ``on_lifecycle_events`` batches with exactly one batch awaited in flight,
    which preserves submission order end to end.
    """

    def __init__(self, actor: ActorHandle, worker_id: int):
        self.actor = actor
        self.worker_id = worker_id
        self._events: asyncio.Queue = asyncio.Queue()
        self._pump_task: Optional[asyncio.Task] = None

    def report(self, method_name: str, *args) -> None:
        if self._pump_task is None or self._pump_task.done():
            self._pump_task = asyncio.get_running_loop().create_task(self._pump())
        self._events.put_nowait((method_name, args))

    async def _pump(self) -> None:
        while True:
            # Wait for the next event, then drain whatever queued up behind it
            # into the same batch.
            batch = [await self._events.get()]
            while not self._events.empty():
                batch.append(self._events.get_nowait())
            try:
                await self.actor.on_lifecycle_events.remote(batch)
            except Exception as e:
                logger.debug(
                    "Dropped a batch of %d KV lifecycle events: %s", len(batch), e
                )
            finally:
                for _ in batch:
                    self._events.task_done()

    async def flush(self) -> None:
        """Wait until every reported event has been delivered (or dropped)."""
        await self._events.join()


class RequestTokenTracker:
    """Drives the request lifecycle hooks for one ``generate()`` stream."""

    def __init__(
        self,
        reporter: KVRouterReporter,
        request_id: str,
        prompt_token_count: int,
        emits_deltas: bool,
    ):
        self._reporter = reporter
        self._request_id = request_id
        self._emits_deltas = emits_deltas
        self._cumulative = 0
        self._prefill_marked = False
        self._finished = False
        reporter.report(
            "on_request_added", request_id, reporter.worker_id, prompt_token_count
        )

    def on_output(self, output: RequestOutput) -> None:
        """Observe one engine ``RequestOutput`` (forwarded to the caller as-is)."""
        step_tokens = sum(len(o.token_ids or []) for o in output.outputs)
        cumulative = (
            self._cumulative + step_tokens
            if self._emits_deltas
            else max(self._cumulative, step_tokens)
        )
        if cumulative == self._cumulative:
            return
        self._cumulative = cumulative
        if not self._prefill_marked:
            # The first output token signals prefill completion.
            self._prefill_marked = True
            self._reporter.report("on_prefill_complete", self._request_id)
        self._reporter.report("on_decode_progress", self._request_id, cumulative)

    def finish(self) -> None:
        """Report completion exactly once."""
        if not self._finished:
            self._finished = True
            self._reporter.report("on_request_completed", self._request_id)


def enable_token_tracking(engine_cls: Type[AsyncLLM]) -> Type[AsyncLLM]:
    """Decorator adding KV-router request lifecycle tracking to vLLM's
    ``AsyncLLM`` engine client."""

    class TokenTrackingEngine(engine_cls):
        _kv_reporter: Optional[KVRouterReporter] = None

        def _resolve_kv_reporter(self) -> Optional[KVRouterReporter]:
            if self._kv_reporter is None:
                try:
                    actor = serve.get_deployment_actor(KV_ROUTER_ACTOR_NAME)
                    worker_id = get_worker_id(
                        serve.get_replica_context().replica_id.unique_id
                    )
                    self._kv_reporter = KVRouterReporter(actor, worker_id)
                except Exception as e:
                    logger.debug("KV token tracking disabled: %s", e)
            return self._kv_reporter

        async def generate(self, prompt, sampling_params, request_id, *args, **kwargs):
            stream = super().generate(
                prompt, sampling_params, request_id, *args, **kwargs
            )
            reporter = self._resolve_kv_reporter()
            if reporter is None:
                async for output in stream:
                    yield output
                return

            tracker = RequestTokenTracker(
                reporter,
                request_id,
                _prompt_token_count(prompt),
                emits_deltas=sampling_params.output_kind == RequestOutputKind.DELTA,
            )
            try:
                async for output in stream:
                    tracker.on_output(output)
                    yield output
            finally:
                tracker.finish()

    return TokenTrackingEngine
