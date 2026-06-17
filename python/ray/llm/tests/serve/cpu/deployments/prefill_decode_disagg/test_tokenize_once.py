"""Unit tests for the P/D tokenize-once renderer wrap.

vLLM is optional: the real-renderer and orchestrator checks skip when it (or the
Ray serve import) is unavailable; the rest run with a stubbed renderer.
"""
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
    # Enter in one asyncio Context and exit in another, as when a peeked-then-
    # abandoned decode generator is finalized off-task. reset(token) would raise
    # "Token was created in a different Context". set(None) must not.
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


def test_install_returns_false_when_method_missing(monkeypatch):
    # A vLLM whose BaseRenderer lacks tokenize_prompts_async must fail safe
    # (return False), not raise AttributeError and crash replica startup.
    class RendererWithoutMethod:
        pass

    base = types.ModuleType("vllm.renderers.base")
    base.BaseRenderer = RendererWithoutMethod
    monkeypatch.setitem(sys.modules, "vllm", types.ModuleType("vllm"))
    monkeypatch.setitem(
        sys.modules, "vllm.renderers", types.ModuleType("vllm.renderers")
    )
    monkeypatch.setitem(sys.modules, "vllm.renderers.base", base)
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


def test_concurrent_tasks_do_not_cross_contaminate(fake_renderer):
    # Two requests run on separate asyncio tasks with different reuse ids and
    # interleave at the await. contextvars are task-local, so each injection must
    # see only its own ids (this is the property the design relies on for safety).
    install()
    Renderer = fake_renderer

    async def one(ids):
        with reuse_prompt_token_ids(ids):
            await asyncio.sleep(0)  # yield so the two tasks interleave
            _tag, seen, _params = await Renderer.tokenize_prompts_async(
                Renderer(), [{"prompt": "hi"}], "PARAMS"
            )
            return seen

    async def run():
        return await asyncio.gather(one([1, 1, 1]), one([2, 2, 2]))

    a, b = asyncio.run(run())
    assert a == [{"prompt": "hi", "prompt_token_ids": [1, 1, 1]}]
    assert b == [{"prompt": "hi", "prompt_token_ids": [2, 2, 2]}]


def test_patch_applies_to_real_vllm_renderer():
    # Confirms install() wraps the *real* vLLM BaseRenderer (correct import path
    # and method name for this vLLM version), and that with reuse ids set the
    # rendered text prompt reaches per-prompt tokenization carrying
    # ``prompt_token_ids`` so vLLM's skip-check avoids the encode. This is the
    # guard against a vLLM bump silently moving the seam the wrap depends on.
    vbase = pytest.importorskip("vllm.renderers.base")
    original = vbase.BaseRenderer.tokenize_prompts_async
    try:
        assert install() is True
        assert getattr(
            vbase.BaseRenderer.tokenize_prompts_async, "_pd_tokonce_wrapped", False
        )
        assert install() is True  # idempotent against the real class

        seen = []

        class StubRenderer:
            # The real tokenize_prompts_async fans out to tokenize_prompt_async
            # per prompt; that is the only hook we need to observe injection.
            async def tokenize_prompt_async(self, prompt, params):
                seen.append(prompt)
                return prompt

        async def drive(ids):
            seen.clear()
            with reuse_prompt_token_ids(ids):
                await vbase.BaseRenderer.tokenize_prompts_async(
                    StubRenderer(), [{"prompt": "hi"}], "PARAMS"
                )

        asyncio.run(drive([11, 22, 33]))
        assert seen == [{"prompt": "hi", "prompt_token_ids": [11, 22, 33]}]

        asyncio.run(drive(None))  # no reuse -> untouched, real encode would run
        assert seen == [{"prompt": "hi"}]
    finally:
        vbase.BaseRenderer.tokenize_prompts_async = original


def _orchestrator(pd_tokenize_once):
    """A PDOrchestratorMixin instance with only the reuse flag set.

    Skips when the Ray serve LLM stack (or vLLM) is unavailable.
    """
    pd_server = pytest.importorskip(
        "ray.llm._internal.serve.serving_patterns.prefill_decode.pd_server"
    )
    obj = pd_server.PDOrchestratorMixin.__new__(pd_server.PDOrchestratorMixin)
    obj._pd_tokenize_once = pd_tokenize_once
    return obj


@pytest.mark.parametrize(
    "enabled, chunk, expected",
    [
        # chat: ids echoed top-level on the response
        (True, types.SimpleNamespace(prompt_token_ids=[1, 2, 3]), [1, 2, 3]),
        # completions: ids echoed on the first choice
        (
            True,
            types.SimpleNamespace(
                prompt_token_ids=None,
                choices=[types.SimpleNamespace(prompt_token_ids=[4, 5])],
            ),
            [4, 5],
        ),
        # disabled: never reuse, even when prefill echoed ids
        (False, types.SimpleNamespace(prompt_token_ids=[1, 2, 3]), None),
    ],
)
def test_decode_reuse_ids(enabled, chunk, expected):
    assert _orchestrator(enabled)._decode_reuse_ids(chunk) == expected


@pytest.mark.parametrize(
    "enabled, expected",
    [(True, True), (False, False)],  # only request the echo when enabled
)
def test_request_prefill_token_ids_gating(enabled, expected):
    req = types.SimpleNamespace(return_token_ids=False)
    _orchestrator(enabled)._request_prefill_token_ids(req)
    assert req.return_token_ids is expected


def test_request_prefill_token_ids_noop_when_field_absent():
    # An older vLLM request without return_token_ids must not crash.
    req = types.SimpleNamespace()
    _orchestrator(True)._request_prefill_token_ids(req)
    assert not hasattr(req, "return_token_ids")


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
