from typing import Type

from vllm.outputs import RequestOutput
from vllm.v1.engine.async_llm import AsyncLLM

from ray.llm._internal.serve.routing_policies.kv_aware.lifecycle import (
    LifecycleEvent,
    LifecyclePublisher,
)
from ray.llm._internal.serve.utils.server_utils import get_serve_request_id


class RequestTokenTracker:
    """Publishes the lifecycle events of one ``generate()`` stream."""

    def __init__(self, publisher: LifecyclePublisher, request_id: str):
        self._publisher = publisher
        self._request_id = request_id
        self._prefill_marked = False
        self._finished = False

    def on_output(self, output: RequestOutput) -> None:
        if self._prefill_marked or not any(o.token_ids for o in output.outputs):
            return
        # The first output token signals prefill completion.
        self._prefill_marked = True
        self._publisher.publish(self._request_id, LifecycleEvent.PREFILL_COMPLETE)

    def finish(self) -> None:
        """Report completion exactly once."""
        if not self._finished:
            self._finished = True
            self._publisher.publish(self._request_id, LifecycleEvent.FINISHED)


def enable_token_tracking(
    engine_cls: Type[AsyncLLM], publisher: LifecyclePublisher
) -> Type[AsyncLLM]:
    """Decorator publishing each request's lifecycle events through ``publisher``."""

    class TokenTrackingEngine(engine_cls):
        async def generate(self, prompt, sampling_params, request_id, *args, **kwargs):
            stream = super().generate(
                prompt, sampling_params, request_id, *args, **kwargs
            )
            # Routers book requests under their Serve request id. Requests no
            # router booked (e.g. HAProxy's fallback path) publish too. Routers
            # have no booking for them, so the selection service keeps each such
            # event for 30 s in case a booking arrives, then drops it.
            # TODO (jeffreywang): Track per request, not per generate call. Beam
            # search runs many generate calls, so the first one frees the
            # request's load early.
            tracker = RequestTokenTracker(
                publisher, get_serve_request_id() or request_id
            )
            try:
                async for output in stream:
                    tracker.on_output(output)
                    yield output
            finally:
                tracker.finish()

    return TokenTrackingEngine
