from types import SimpleNamespace
import zlib

import numpy as np
import pytest
from starlette.datastructures import Headers

from ray.llm._internal.serve.engines.vllm.vllm_engine import (
    _PromptTokenSideChannelStore,
    _inject_prompt_ids_from_side_channel,
    _install_prompt_ids_forwarding,
)
from ray.llm._internal.serve.routing_policies.kv_aware.constants import (
    KV_PROMPT_TOKEN_CRC32_HEADER,
    KV_PROMPT_TOKEN_KEY_HEADER,
    KV_PROMPT_TOKEN_LEN_HEADER,
)


def _payload(ids):
    payload = np.asarray(ids, dtype="<u4").tobytes()
    return payload, f"{zlib.crc32(payload) & 0xFFFFFFFF:08x}"


def _raw_request(key, ids):
    payload, crc32 = _payload(ids)
    return SimpleNamespace(
        headers=Headers(
            {
                KV_PROMPT_TOKEN_KEY_HEADER: key,
                KV_PROMPT_TOKEN_LEN_HEADER: str(len(ids)),
                KV_PROMPT_TOKEN_CRC32_HEADER: crc32,
            }
        )
    )


@pytest.mark.asyncio
async def test_inject_prompt_ids_from_staged_binary_payload():
    ids = [1, 2, 3, 65536]
    payload, crc32 = _payload(ids)
    store = _PromptTokenSideChannelStore()
    await store.put("k1", payload=payload, token_count=len(ids), crc32=crc32)

    request = SimpleNamespace(kv_transfer_params=None)
    await _inject_prompt_ids_from_side_channel(request, _raw_request("k1", ids), store)

    prompt_ids = request.kv_transfer_params["prompt_token_ids"]
    assert prompt_ids == ids
    assert await store.pop("k1") is None


@pytest.mark.asyncio
async def test_missing_side_channel_key_falls_back_to_tokenization():
    store = _PromptTokenSideChannelStore()
    request = SimpleNamespace(kv_transfer_params=None)

    await _inject_prompt_ids_from_side_channel(
        request, _raw_request("missing", [1, 2, 3]), store
    )

    assert request.kv_transfer_params is None


@pytest.mark.asyncio
async def test_installed_wrapper_injects_before_serving_method_runs():
    ids = [7, 8, 9]
    payload, crc32 = _payload(ids)
    store = _PromptTokenSideChannelStore()
    await store.put("k2", payload=payload, token_count=len(ids), crc32=crc32)
    captured = {}

    class Serving:
        async def create_completion(self, request, raw_request=None):
            captured["kv_transfer_params"] = request.kv_transfer_params
            return "ok"

    serving = Serving()
    _install_prompt_ids_forwarding(serving, "create_completion", store)
    request = SimpleNamespace(kv_transfer_params={})

    result = await serving.create_completion(request, _raw_request("k2", ids))

    assert result == "ok"
    prompt_ids = captured["kv_transfer_params"]["prompt_token_ids"]
    assert prompt_ids == ids
