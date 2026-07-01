import asyncio
from typing import Any, List, Optional, Type

from vllm.outputs import RequestOutput
from vllm.sampling_params import RequestOutputKind
from vllm.v1.engine.async_llm import AsyncLLM

from ray import serve
from ray.actor import ActorHandle
from ray.exceptions import RayActorError, RayTaskError
from ray.llm._internal.serve.observability.logging import get_logger
from ray.llm._internal.serve.routing_policies.kv_aware.kv_aware_actor import (
    KV_ROUTER_ACTOR_NAME,
    get_worker_id,
)
from ray.llm._internal.serve.utils.server_utils import get_serve_request_id

logger = get_logger(__name__)


def _get_prompt_token_ids(prompt: Any) -> List[int]:
    """The prompt's pre-tokenized token ids."""
    try:
        return list(prompt["prompt_token_ids"])
    except (KeyError, TypeError) as e:
        raise ValueError(
            "KV-aware token tracking requires a pre-tokenized prompt "
            f"(dict with 'prompt_token_ids'); got {type(prompt).__name__}"
        ) from e


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
        self._drop_warned = False

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
                # Re-arm the warning so a fresh failure surfaces if the actor
                # recovers and then fails again.
                self._drop_warned = False
            except (RayActorError, RayTaskError) as e:
                if not self._drop_warned:
                    self._drop_warned = True
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
        _resolve_warned: bool = False

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
                    # Warn once: resolution is retried per request until it succeeds.
                    if not self._resolve_warned:
                        self._resolve_warned = True
                        logger.warning("KV token tracking disabled: %s", e)
            return self._lifecycle_forwarder

        def shutdown(self, *args, **kwargs):
            if self._lifecycle_forwarder is not None:
                self._lifecycle_forwarder.close()
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
                _get_prompt_token_ids(prompt),
            )
            try:
                async for output in stream:
                    tracker.on_output(output)
                    yield output
            finally:
                tracker.finish()

    return TokenTrackingEngine
