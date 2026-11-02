from typing import Optional, Type

from vllm.outputs import RequestOutput
from vllm.sampling_params import RequestOutputKind
from vllm.v1.engine.async_llm import AsyncLLM

from ray.llm._internal.serve.routing_policies.kv_aware.lifecycle import (
    LifecycleEventForwarder,
    get_request_lifecycle_owner,
)
from ray.llm._internal.serve.utils.server_utils import get_serve_request_id


class RequestTokenTracker:
    """Drives the request lifecycle hooks for one ``generate()`` stream."""

    def __init__(
        self,
        forwarder: LifecycleEventForwarder,
        request_id: str,
        owner: str,
        report_decode_progress: bool = False,
    ):
        self._forwarder = forwarder
        self._request_id = request_id
        # Lifecycle endpoint of the router that selected this request.
        self._owner = owner
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
            self._forwarder.report(self._owner, "on_prefill_complete", self._request_id)
        if self._report_decode_progress:
            self._forwarder.report(
                self._owner,
                "on_decode_progress",
                self._request_id,
                self._cumulative,
            )

    def finish(self) -> None:
        """Report completion exactly once."""
        if not self._finished:
            self._finished = True
            self._forwarder.report(
                self._owner, "on_request_completed", self._request_id
            )


def enable_token_tracking(
    engine_cls: Type[AsyncLLM], report_decode_progress: bool = False
) -> Type[AsyncLLM]:
    """Decorator adding KV-router request lifecycle tracking."""

    class TokenTrackingEngine(engine_cls):
        _lifecycle_forwarder: Optional[LifecycleEventForwarder] = None
        _report_decode_progress: bool = report_decode_progress

        def shutdown(self, *args, **kwargs):
            if self._lifecycle_forwarder is not None:
                self._lifecycle_forwarder.close()
            return super().shutdown(*args, **kwargs)

        async def generate(self, prompt, sampling_params, request_id, *args, **kwargs):
            stream = super().generate(
                prompt, sampling_params, request_id, *args, **kwargs
            )
            # No owner: no KV router selected this request (e.g. HAProxy's
            # fallback path), so there is nothing to report.
            owner = get_request_lifecycle_owner()
            # CUMULATIVE repeats output-so-far per chunk; our accounting sums
            # deltas, so skip it rather than over-count. vLLM's OpenAI layer only
            # uses DELTA/FINAL_ONLY (*Request.to_sampling_params):
            # https://github.com/vllm-project/vllm/tree/main/vllm/entrypoints/openai
            if owner is None or (
                sampling_params.output_kind == RequestOutputKind.CUMULATIVE
            ):
                async for output in stream:
                    yield output
                return

            if self._lifecycle_forwarder is None:
                self._lifecycle_forwarder = LifecycleEventForwarder()
            lifecycle_request_id = get_serve_request_id() or request_id
            tracker = RequestTokenTracker(
                self._lifecycle_forwarder,
                lifecycle_request_id,
                owner,
                report_decode_progress=self._report_decode_progress,
            )
            try:
                async for output in stream:
                    tracker.on_output(output)
                    yield output
            finally:
                tracker.finish()

    return TokenTrackingEngine
