"""Decode-stage reuse of prefill's prompt token ids (P/D tokenize-once).

In prefill/decode disaggregation a chat prompt is otherwise tokenized twice (once
per stage). This wraps ``BaseRenderer.tokenize_prompts_async`` to inject the token
ids prefill already produced (published per-task via a contextvar by
``reuse_prompt_token_ids``) into the rendered prompt. vLLM's tokenizer skips the
encode when ``prompt_token_ids`` is already present, so the redundant tokenize is
avoided while the rest of the pipeline (multimodal data, detokenization,
truncation/validation, response shaping) runs unchanged. No vLLM source change, no
new request field. ``install()`` is idempotent, called once per decode replica.

Relies on decode tokenization running within the reuse block -- on the same async
task or a task spawned from it (true today). If vLLM moved tokenization onto an
unrelated thread/executor the contextvar would not propagate and this degrades to
normal tokenization (slow, never wrong).
"""

import contextlib
import contextvars
import functools
import logging

logger = logging.getLogger(__name__)

# Per-async-task reused prompt token ids. contextvars propagate across ``await``
# within the same task (and to tasks spawned from it), so concurrent requests on
# other tasks don't cross-contaminate.
_reused_token_ids: contextvars.ContextVar = contextvars.ContextVar(
    "pd_reused_prompt_token_ids", default=None
)


@contextlib.contextmanager
def reuse_prompt_token_ids(token_ids):
    """Reuse ``token_ids`` for the chat render inside this block (skip tokenize).

    No-op when ``token_ids`` is falsy (feature disabled, or the prefill stage did
    not return token ids), so callers don't need a separate enabled check.
    """
    if not token_ids:
        yield
        return
    _reused_token_ids.set(list(token_ids))
    try:
        yield
    finally:
        # Clear with set(None), not reset(token): this block spans the decode
        # generator's yields, so the finally may run in a different asyncio Context
        # than the enter (async-gen finalization after a streaming disconnect, or GC
        # of a peeked-then-abandoned generator). ContextVar.reset() raises across
        # Contexts; set() does not, and reuse never nests so there is no prior value
        # to restore.
        _reused_token_ids.set(None)


def install() -> bool:
    """Wrap ``BaseRenderer.tokenize_prompts_async`` to honor the contextvar.

    Idempotent (guarded by an attribute on the wrapped method) and resilient: any
    failure (e.g. a vLLM version whose renderer differs) leaves tokenization
    untouched. Returns True if the wrap is active.
    """
    try:
        from vllm.renderers.base import BaseRenderer
    except Exception as e:  # pragma: no cover - defensive
        logger.debug("pd-tokenize-once: vLLM renderer unavailable (%s)", e)
        return False

    orig = BaseRenderer.tokenize_prompts_async
    if getattr(orig, "_pd_tokonce_wrapped", False):
        return True

    @functools.wraps(orig)
    async def tokenize_prompts_async(self, prompts, params, *args, **kwargs):
        ids = _reused_token_ids.get()
        # Inject the reused ids into the single rendered prompt and delegate to the
        # real tokenizer: vLLM skips the encode when prompt_token_ids is already
        # present but still preserves multi_modal_data and runs detokenization +
        # truncation/validation. Anything else (batched, embeds, encoder-decoder,
        # or already tokenized) falls through untouched. ``ids`` is a private copy.
        if (
            ids
            and len(prompts) == 1
            and isinstance(prompts[0], dict)
            and "prompt_token_ids" not in prompts[0]
            and "prompt_embeds" not in prompts[0]
            and "encoder_prompt" not in prompts[0]
        ):
            prompts = [{**prompts[0], "prompt_token_ids": ids}]
        return await orig(self, prompts, params, *args, **kwargs)

    tokenize_prompts_async._pd_tokonce_wrapped = True
    BaseRenderer.tokenize_prompts_async = tokenize_prompts_async
    logger.info(
        "pd-tokenize-once: wrapped BaseRenderer.tokenize_prompts_async "
        "(decode stage will reuse prefill's prompt token ids)"
    )
    return True
