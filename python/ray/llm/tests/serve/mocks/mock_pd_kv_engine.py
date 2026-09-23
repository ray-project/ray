"""Controllable engines for P/D routing integration tests."""

import asyncio
import hashlib
import json
import os
import time
from unittest.mock import patch

import msgspec
import zmq

from ray import serve
from ray._common.network_utils import find_free_port
from ray.llm._internal.serve.constants import RAY_SERVE_LLM_ENABLE_DECODE_BLOCK_PROGRESS
from ray.llm._internal.serve.core.configs.openai_api_models import CompletionResponse
from ray.llm._internal.serve.core.ingress.pd_router import LLMPDRouter
from ray.llm._internal.serve.routing_policies.kv_aware.constants import (
    KV_TOKEN_METADATA_KEY,
    ROUTING_REQUEST_ID_CONTEXT,
)
from ray.llm._internal.serve.routing_policies.kv_aware.kv_token_tracker import (
    get_llm_router_handle,
    get_worker_id,
)
from ray.llm._internal.serve.routing_policies.kv_aware.token_channel import (
    TokenReceiver,
    TokenStore,
)
from ray.llm._internal.serve.routing_policies.kv_aware.vllm.prompt_token_forwarding import (
    inject_prompt_token_ids,
)
from ray.llm._internal.serve.routing_policies.kv_aware.vllm.token_tracking import (
    LifecycleEventForwarder,
)
from ray.llm.tests.serve.mocks.mock_vllm_engine import MockVLLMEngine


def _completion(request, text, kv_transfer_params=None):
    return CompletionResponse(
        id=request.request_id,
        created=int(time.time()),
        model=request.model,
        choices=[{"index": 0, "text": text, "finish_reason": "length"}],
        usage={"prompt_tokens": 32, "completion_tokens": 1, "total_tokens": 33},
        kv_transfer_params=kv_transfer_params,
    )


class MockPDKVEngine(MockVLLMEngine):
    async def start(self):
        await super().start()
        self._publisher = zmq.Context.instance().socket(zmq.PUB)
        port = self._publisher.bind_to_random_port("tcp://127.0.0.1")
        self._endpoint = f"tcp://127.0.0.1:{port}"
        self._token_store = TokenStore()
        self._token_endpoint = f"tcp://127.0.0.1:{find_free_port()}"
        self._token_receiver = TokenReceiver(
            bind_endpoint=self._token_endpoint, store=self._token_store
        )
        await self._token_receiver.start()
        self._event_sequence = 0
        rc = serve.get_replica_context()
        self.replica_id = rc.replica_id.to_full_id_str()
        self._forwarder = LifecycleEventForwarder(
            get_llm_router_handle(),
            get_worker_id(rc.replica_id.unique_id),
            deployment_id=rc.replica_id.deployment_id,
        )

    def routing_stats(self):
        return {
            KV_TOKEN_METADATA_KEY: {"endpoint": self._token_endpoint},
            "kv_event_metadata": {
                "endpoint": self._endpoint,
                "block_size": 16,
                "max_num_batched_tokens": 8192,
                "dp_rank": 0,
            },
        }

    async def completions(self, request, raw_request_info=None):
        key = ROUTING_REQUEST_ID_CONTEXT.get()
        assert key and key == request.request_id
        params = request.kv_transfer_params
        assert params
        prompt = getattr(request, "prompt", "chat")
        if not isinstance(prompt, str):
            prompt = "chat"
        prefill = self.llm_config.experimental_configs["pd_routing_stage"] == "prefill"
        staged_tokens = False
        if not prefill:
            inject_prompt_token_ids(request, raw_request_info, self._token_store)
            staged_tokens = "prompt_token_ids" in params
            params.setdefault("prompt_token_ids", [ord(c) for c in prompt])
            assert params["metadata_probe"] == 'quotes " braces {} and unicode: \u2603'
        try:
            if prompt.startswith("concurrent"):
                await asyncio.sleep(0.1)
            if prefill:
                assert params.get("prompt_token_ids")
                if prompt.startswith("fail"):
                    raise RuntimeError("injected prefill failure")
                if prompt.startswith("slow-prefill"):
                    await asyncio.sleep(2)
                if prompt.startswith("cache-affinity"):
                    self._publish_cache(params["prompt_token_ids"])
                self._forwarder.report("on_prefill_complete", key)
                params = {
                    "remote_engine_id": self.replica_id,
                    "prefill_done_at": time.time(),
                    "metadata_probe": 'quotes " braces {} and unicode: \u2603',
                }
                if prompt == "oversized-metadata":
                    params["padding"] = "x" * (512 * 1024)
                yield _completion(request, "prefill", params)
                return
            self._forwarder.report("on_prefill_complete", key)
            info = {
                "prefill": params["remote_engine_id"],
                "decode": self.replica_id,
                "prefill_done_at": params["prefill_done_at"],
                "decode_started_at": time.time(),
                "routing_id": key,
                "tokens": params["prompt_token_ids"],
                "staged_tokens": staged_tokens,
            }
            if request.stream:
                yield "data: " + json.dumps(info) + "\n\n"
                for count in range(1, request.max_tokens or 4):
                    await asyncio.sleep(0.05)
                    if RAY_SERVE_LLM_ENABLE_DECODE_BLOCK_PROGRESS:
                        self._forwarder.report("on_decode_progress", key, count)
                    yield "data: " + json.dumps({"token": count}) + "\n\n"
                yield "data: [DONE]\n\n"
            else:
                yield _completion(request, json.dumps(info))
        finally:
            self._forwarder.report("on_request_completed", key)

    async def chat(self, request, raw_request_info=None):
        async for chunk in self.completions(request, raw_request_info):
            yield chunk

    def _publish_cache(self, token_ids):
        from vllm.distributed.kv_events import BlockStored, KVEventBatch

        full_blocks = len(token_ids) // 16
        hashes = [
            int.from_bytes(
                hashlib.blake2b(
                    json.dumps(token_ids[: 16 * i]).encode(),
                    digest_size=8,
                ).digest(),
                "big",
            )
            for i in range(1, full_blocks + 1)
        ]
        event = BlockStored(
            block_hashes=hashes,
            parent_block_hash=None,
            token_ids=token_ids[: full_blocks * 16],
            block_size=16,
            lora_id=None,
            medium="GPU",
            lora_name=None,
        )
        self._publisher.send_multipart(
            (
                b"",
                self._event_sequence.to_bytes(8, "big"),
                msgspec.msgpack.encode(
                    KVEventBatch(ts=time.time(), events=[event], data_parallel_rank=0)
                ),
            )
        )
        self._event_sequence += 1

    async def shutdown(self):
        self._forwarder.close()
        self._publisher.close(linger=0)
        await self._token_receiver.close()
        await super().shutdown()


class _Tokenizer:
    def __init__(self, config):
        pass

    async def tokenize(self, payload):
        return [ord(c) for c in payload.get("prompt", "chat")]


class MockLLMPDRouter(LLMPDRouter):
    async def __init__(self, *args, **kwargs):
        with patch(
            "ray.llm._internal.serve.routing_policies.kv_aware.vllm.tokenizer.Tokenizer",
            _Tokenizer,
        ):
            await super().__init__(*args, **kwargs)

    def push_prompt_tokens(self, *, request_token_ids, **kwargs):
        if request_token_ids[:11] == [ord(c) for c in "drop-tokens"]:
            return None
        return super().push_prompt_tokens(request_token_ids=request_token_ids, **kwargs)

    async def exit(self):
        os._exit(0)
