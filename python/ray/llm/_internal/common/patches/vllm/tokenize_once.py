"""Decode-stage reuse of prefill's prompt token ids (P/D tokenize-once).

A P/D chat prompt is otherwise tokenized once per stage. ``install()`` wraps
``BaseRenderer.tokenize_prompts_async`` to inject the ids prefill already produced.
``reuse_prompt_token_ids`` publishes those ids per async task. vLLM skips the encode
when ``prompt_token_ids`` is present, so the rest of the pipeline runs unchanged.
``install()`` is idempotent and fails safe.

Decode tokenization must run within the reuse block, on the same async task or one
spawned from it, so the contextvar reaches it.

Temporary patch. The intended end state is native pre-tokenized input on the
chat-completions path (a ``prompt_token_ids`` field on ``ChatCompletionRequest``),
at which point decode passes the ids through a request field and this wrap is
deleted. See vllm-project/vllm#22817 (token-in/token-out) for the upstream
direction.
"""

import contextlib
import contextvars
import functools
import logging

logger = logging.getLogger(__name__)

# Per-async-task reused prompt token ids. contextvars don't leak across tasks, so
# concurrent requests can't cross-contaminate.
_reused_token_ids: contextvars.ContextVar = contextvars.ContextVar(
    "pd_reused_prompt_token_ids", default=None
)


@contextlib.contextmanager
def reuse_prompt_token_ids(token_ids):
    """Publish ``token_ids`` so the chat render inside this block skips tokenize.

    No-op when ``token_ids`` is falsy, so callers need no separate enabled check.
    """
    if not token_ids:
        yield
        return
    _reused_token_ids.set(list(token_ids))
    try:
        yield
    finally:
        # Use set(None), not reset(token). The finally may run in a different
        # Context than the enter during off-task generator finalization, where
        # reset() would raise.
        _reused_token_ids.set(None)


def install() -> bool:
    """Wrap ``BaseRenderer.tokenize_prompts_async`` to honor the contextvar.

    Idempotent and fails safe. Returns False and leaves tokenization untouched when
    vLLM's renderer is missing or differs, so it never crashes startup.
    """
    try:
        from vllm.renderers.base import BaseRenderer

        orig = getattr(BaseRenderer, "tokenize_prompts_async", None)
        if orig is None:
            logger.debug("pd-tokenize-once: BaseRenderer.tokenize_prompts_async absent")
            return False
        if getattr(orig, "_pd_tokonce_wrapped", False):
            return True

        @functools.wraps(orig)
        async def tokenize_prompts_async(self, prompts, params, *args, **kwargs):
            ids = _reused_token_ids.get()
            # Inject the reused ids into the lone rendered prompt so vLLM skips the
            # encode but still preserves multi_modal_data and runs detok/validation.
            # Anything else falls through to a normal tokenize.
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
    except Exception as e:  # pragma: no cover - defensive
        logger.debug("pd-tokenize-once: install failed (%s)", e)
        return False

    logger.info(
        "pd-tokenize-once: wrapped BaseRenderer.tokenize_prompts_async "
        "(decode stage will reuse prefill's prompt token ids)"
    )
    return True
