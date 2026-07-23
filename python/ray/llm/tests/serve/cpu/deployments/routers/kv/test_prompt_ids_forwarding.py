"""The engine consumes the x-kv-prompt-ids header the ingress rode to it,
staging the ids into ``kv_transfer_params`` for vLLM's renderer reuse branch
(vllm-project/vllm#48145)."""

import sys
from types import SimpleNamespace

import pytest

from ray.llm._internal.serve.engines.vllm.vllm_engine import (
    _install_prompt_ids_forwarding,
)
from ray.llm._internal.serve.routing_policies.kv_aware.constants import (
    KV_PROMPT_IDS_HEADER,
)


class _FakeServingChat:
    def __init__(self):
        self.calls = []

    async def create_chat_completion(self, request, raw_request=None, **kwargs):
        self.calls.append((request, raw_request))
        return "ok"


def _wrapped_serving_chat():
    serving_chat = _FakeServingChat()
    _install_prompt_ids_forwarding(serving_chat)
    return serving_chat


def _raw_request(headers):
    return SimpleNamespace(headers=headers)


@pytest.mark.asyncio
async def test_header_ids_staged_into_kv_transfer_params():
    serving_chat = _wrapped_serving_chat()
    request = SimpleNamespace(kv_transfer_params={"do_remote_decode": True})

    result = await serving_chat.create_chat_completion(
        request, _raw_request({KV_PROMPT_IDS_HEADER: "5,6,7"})
    )

    assert result == "ok"
    assert request.kv_transfer_params == {
        "do_remote_decode": True,
        "prompt_token_ids": [5, 6, 7],
    }


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "headers",
    [{}, {KV_PROMPT_IDS_HEADER: ""}, {KV_PROMPT_IDS_HEADER: "5,x,7"}],
    ids=["absent", "empty", "malformed"],
)
async def test_unusable_header_leaves_request_untouched(headers):
    serving_chat = _wrapped_serving_chat()
    request = SimpleNamespace(kv_transfer_params=None)

    await serving_chat.create_chat_completion(request, _raw_request(headers))

    assert request.kv_transfer_params is None


@pytest.mark.asyncio
async def test_no_raw_request_is_a_noop():
    serving_chat = _wrapped_serving_chat()
    request = SimpleNamespace(kv_transfer_params=None)

    await serving_chat.create_chat_completion(request)

    assert request.kv_transfer_params is None
    assert serving_chat.calls == [(request, None)]


@pytest.mark.asyncio
async def test_staged_ids_match_renderer_reuse_contract():
    """What we stage is exactly what vLLM's patched renderer consumes."""
    try:
        from vllm.renderers.online_renderer import _reused_prompt_token_ids
    except ImportError:
        pytest.skip("vLLM without the #48145 token-reuse patch")

    serving_chat = _wrapped_serving_chat()
    request = SimpleNamespace(kv_transfer_params=None)
    await serving_chat.create_chat_completion(
        request, _raw_request({KV_PROMPT_IDS_HEADER: "1,2,3"})
    )

    assert _reused_prompt_token_ids(request) == [1, 2, 3]
    # The renderer pops the key so the ids stay out of sampling metadata.
    assert "prompt_token_ids" not in request.kv_transfer_params


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
