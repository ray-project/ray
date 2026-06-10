import asyncio
from typing import Any, List, Optional, Type

from vllm.outputs import RequestOutput
from vllm.v1.engine.async_llm import AsyncLLM

from ray import serve
from ray.actor import ActorHandle
from ray.llm._internal.serve.observability.logging import get_logger
from ray.llm._internal.serve.routing_policies.kv_aware.kv_aware_actor import (
    KV_ROUTER_ACTOR_NAME,
    get_worker_id,
)

logger = get_logger(__name__)


def _get_prompt_token_ids(prompt: Any) -> List[int]:
    """The prompt's pre-tokenized token ids."""
    return list(prompt["prompt_token_ids"])


class LifecycleEventForwarder:
    """Ordered, non-blocking bridge from the engine to the KV router actor
    which maintains per-replica token load statistics.

    ``report`` only enqueues locally, so generation never blocks on the actor.
    A single delivery task per replica drains the queue to the actor, awaiting
    one ``on_lifecycle_events`` call at a time so events arrive in the order they
    were reported. Events that pile up during a call are sent together in the
    next one.
    """

    def __init__(self, actor: ActorHandle, worker_id: int):
        self.actor = actor
        self.worker_id = worker_id
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
                await self.actor.on_lifecycle_events.remote(batch)
            except Exception as e:
                logger.debug(
                    "Dropped a batch of %d KV lifecycle events: %s", len(batch), e
                )
            finally:
                for _ in batch:
                    self._events.task_done()

    async def flush(self) -> None:
        """Wait until every reported event has been delivered."""
        await self._events.join()


class RequestTokenTracker:
    """Drives the request lifecycle hooks for one ``generate()`` stream."""

    def __init__(
        self,
        forwarder: LifecycleEventForwarder,
        request_id: str,
        prompt_token_ids: List[int],
    ):
        self._forwarder = forwarder
        self._request_id = request_id
        self._cumulative = 0
        self._prefill_marked = False
        self._finished = False
        forwarder.report(
            "on_request_added", request_id, forwarder.worker_id, prompt_token_ids
        )

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
        self._forwarder.report("on_decode_progress", self._request_id, self._cumulative)

    def finish(self) -> None:
        """Report completion exactly once."""
        if not self._finished:
            self._finished = True
            self._forwarder.report("on_request_completed", self._request_id)


def enable_token_tracking(engine_cls: Type[AsyncLLM]) -> Type[AsyncLLM]:
    """Decorator adding KV-router request lifecycle tracking."""

    class TokenTrackingEngine(engine_cls):
        _lifecycle_forwarder: Optional[LifecycleEventForwarder] = None

        def _resolve_lifecycle_forwarder(self) -> Optional[LifecycleEventForwarder]:
            if self._lifecycle_forwarder is None:
                try:
                    actor = serve.get_deployment_actor(KV_ROUTER_ACTOR_NAME)
                    worker_id = get_worker_id(
                        serve.get_replica_context().replica_id.unique_id
                    )
                    self._lifecycle_forwarder = LifecycleEventForwarder(
                        actor, worker_id
                    )
                except Exception as e:
                    logger.debug("KV token tracking disabled: %s", e)
            return self._lifecycle_forwarder

        async def generate(self, prompt, sampling_params, request_id, *args, **kwargs):
            stream = super().generate(
                prompt, sampling_params, request_id, *args, **kwargs
            )
            forwarder = self._resolve_lifecycle_forwarder()
            if forwarder is None:
                async for output in stream:
                    yield output
                return

            tracker = RequestTokenTracker(
                forwarder,
                request_id,
                _get_prompt_token_ids(prompt),
            )
            try:
                async for output in stream:
                    tracker.on_output(output)
                    yield output
            finally:
                tracker.finish()

    return TokenTrackingEngine
