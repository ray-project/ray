"""Unit tests for the P/D tokenize-once renderer wrap (no vLLM/GPU required)."""
import asyncio
import contextvars
import sys
import types

import pytest

from ray.llm._internal.serve.serving_patterns.prefill_decode._tokenize_once import (
    _reused_token_ids,
    install,
    reuse_prompt_token_ids,
)


def test_reuse_is_noop_on_falsy():
    for falsy in (None, [], 0):
        with reuse_prompt_token_ids(falsy):
            assert _reused_token_ids.get() is None


def test_reuse_sets_private_copy_then_clears():
    src = [1, 2, 3]
    with reuse_prompt_token_ids(src):
        got = _reused_token_ids.get()
        assert got == [1, 2, 3]
        assert got is not src  # a private per-task copy, not the caller's list
    assert _reused_token_ids.get() is None


def test_reuse_teardown_is_cross_context_safe():
    # Enter the block in one asyncio Context and exit it in another, as happens
    # when a peeked-then-abandoned decode generator is finalized off-task. The old
    # reset(token) teardown raised "Token was created in a different Context" here;
    # the set(None) teardown must not.
    cm = reuse_prompt_token_ids([1, 2, 3])
    contextvars.copy_context().run(cm.__enter__)
    cm.__exit__(None, None, None)
    assert _reused_token_ids.get() is None


@pytest.fixture
def fake_renderer(monkeypatch):
    """Install the wrap onto a stub BaseRenderer whose orig echoes its args."""

    class FakeBaseRenderer:
        async def tokenize_prompts_async(self, prompts, params):
            return ("orig", prompts, params)

    base = types.ModuleType("vllm.renderers.base")
    base.BaseRenderer = FakeBaseRenderer
    monkeypatch.setitem(sys.modules, "vllm", types.ModuleType("vllm"))
    monkeypatch.setitem(
        sys.modules, "vllm.renderers", types.ModuleType("vllm.renderers")
    )
    monkeypatch.setitem(sys.modules, "vllm.renderers.base", base)
    return FakeBaseRenderer


def test_install_is_idempotent(fake_renderer):
    assert install() is True
    assert install() is True
    assert getattr(fake_renderer.tokenize_prompts_async, "_pd_tokonce_wrapped", False)


def test_install_returns_false_without_vllm(monkeypatch):
    monkeypatch.setitem(sys.modules, "vllm.renderers.base", None)
    assert install() is False


def _tokenize(renderer_cls, prompts, ids):
    async def run():
        with reuse_prompt_token_ids(ids):
            return await renderer_cls.tokenize_prompts_async(
                renderer_cls(), prompts, "PARAMS"
            )

    return asyncio.run(run())


def test_injects_ids_and_preserves_other_fields(fake_renderer):
    install()
    prompt = {"prompt": "hi", "multi_modal_data": {"image": object()}}
    tag, seen, params = _tokenize(fake_renderer, [dict(prompt)], [7, 8, 9])
    assert tag == "orig" and params == "PARAMS"
    # ids injected so vLLM skips the encode; multi_modal_data preserved.
    assert seen == [{**prompt, "prompt_token_ids": [7, 8, 9]}]


@pytest.mark.parametrize(
    "prompts, ids",
    [
        ([{"prompt": "hi"}], None),  # reuse not set
        ([{"prompt": "a"}, {"prompt": "b"}], [1, 2]),  # batched
        ([{"prompt_token_ids": [1]}], [9]),  # already tokenized
        ([{"prompt_embeds": object()}], [9]),  # embeds
        ([{"encoder_prompt": {}}], [9]),  # encoder-decoder
    ],
)
def test_falls_through_untouched(fake_renderer, prompts, ids):
    install()
    expected = [dict(p) for p in prompts]
    _tag, seen, _params = _tokenize(fake_renderer, prompts, ids)
    assert seen == expected


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
