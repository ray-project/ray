import asyncio
import json
import sys

import pytest
import requests

import ray
from ray import serve
from ray.llm._internal.serve.core.ingress.builder import (
    _build_direct_streaming_llm_deployment,
)
from ray.llm._internal.serve.routing_policies.kv_aware.kv_aware_actor import (
    KV_ROUTER_ACTOR_NAME,
    KVRouterActor,
)
from ray.llm._internal.serve.routing_policies.kv_aware.kv_events import (
    derive_kv_event_block_size,
)
from ray.serve.config import DeploymentActorConfig
from ray.serve.llm import LLMConfig, ModelLoadingConfig

from utils import discover_deployment_actor

MODEL_ID = "Qwen/Qwen3-0.6B"
APP_NAME = "lifecycle_tracking_gpu_test"
REQUEST_ID = "gpu-req-1"
MAX_TOKENS = 48
PROMPT_TEXT = "Describe the water cycle in detail, stage by stage."
# vLLM adopts the X-Request-Id header as the engine request id, prefixed per
# endpoint; the engine-level id is what token tracking reports to the actor.
ENGINE_REQUEST_ID = f"chatcmpl-{REQUEST_ID}"


@ray.remote(num_cpus=0)
class RecordingKVRouterActor(KVRouterActor):
    """The real KVRouterActor, additionally recording every lifecycle event it
    receives so the test can assert reporting order and arguments (the hooks
    themselves are no-ops on this branch)."""

    def __init__(self, block_size):
        super().__init__(block_size=block_size)
        self._event_log = []

    async def on_lifecycle_events(self, events):
        self._event_log.extend(events)
        await super().on_lifecycle_events(events)

    def get_event_log(self):
        return self._event_log


class TestLifecycleTracking:
    @pytest.fixture(scope="class")
    def deployed_handle(self):
        """Deploy a direct-streaming LLMServer with the recording actor."""
        if not ray.is_initialized():
            ray.init(address="auto")
        serve.shutdown()  # ensure no prior app is holding GPU memory

        engine_kwargs = dict(
            max_model_len=2048,
            enforce_eager=True,
            gpu_memory_utilization=0.4,
            use_tqdm_on_load=False,
        )
        llm_config = LLMConfig(
            model_loading_config=ModelLoadingConfig(
                model_id=MODEL_ID,
                model_source=MODEL_ID,
            ),
            deployment_config=dict(
                autoscaling_config=dict(min_replicas=1, max_replicas=1),
                deployment_actors=[
                    DeploymentActorConfig(
                        name=KV_ROUTER_ACTOR_NAME,
                        actor_class=RecordingKVRouterActor,
                        init_kwargs={
                            "block_size": derive_kv_event_block_size(engine_kwargs)
                        },
                        actor_options={"num_cpus": 0},
                    )
                ],
            ),
            engine_kwargs=engine_kwargs,
            placement_group_config={"bundles": [{"GPU": 1}]},
            # The replica worker needs the vLLM compile-cache flag at import
            # time; direct ingress/streaming come from the cluster runtime_env.
            runtime_env=dict(env_vars={"VLLM_DISABLE_COMPILE_CACHE": "1"}),
            log_engine_metrics=False,
        )

        app = _build_direct_streaming_llm_deployment(llm_config)
        handle = serve.run(app, name=APP_NAME)
        yield handle
        serve.shutdown()

    async def _backend_endpoint(self, handle):
        """Poll for the replica's backend HTTP (host, port); it is reported
        asynchronously after the replica starts its direct-ingress server."""
        for _ in range(60):
            async with handle.choose_replica() as selection:
                endpoint = selection._replica.backend_http_endpoint
            if endpoint is not None:
                return endpoint
            await asyncio.sleep(1.0)
        raise AssertionError("replica backend HTTP endpoint never became available")

    @pytest.mark.asyncio
    async def test_lifecycle_events_reported_in_order(self, deployed_handle):
        actor = discover_deployment_actor(
            APP_NAME, deployed_handle.deployment_name, KV_ROUTER_ACTOR_NAME
        )
        assert actor is not None, "KV router actor was not discoverable"

        # X-Request-Id pins the engine request id; include_usage returns the
        # engine's own token counts as ground truth.
        host, port = await self._backend_endpoint(deployed_handle)
        url = f"http://{host}:{port}/v1/chat/completions"
        payload = {
            "model": MODEL_ID,
            "messages": [{"role": "user", "content": PROMPT_TEXT}],
            "stream": True,
            "stream_options": {"include_usage": True},
            "max_tokens": MAX_TOKENS,
            "temperature": 0.0,
        }
        headers = {"X-Request-Id": REQUEST_ID}

        usage = None
        with requests.post(
            url, json=payload, headers=headers, stream=True, timeout=120
        ) as resp:
            assert resp.status_code == 200, resp.text
            for raw in resp.iter_lines():
                if not raw:
                    continue
                line = raw.decode("utf-8")
                if not line.startswith("data:"):
                    continue
                data = line[len("data:") :].strip()
                if data == "[DONE]":
                    break
                chunk = json.loads(data)
                if chunk.get("usage"):
                    usage = chunk["usage"]

        assert usage is not None, "expected a final usage chunk"

        # Events are delivered asynchronously; wait for the terminal completion.
        def request_events():
            return [
                (name, args)
                for name, args in ray.get(actor.get_event_log.remote())
                if args[0] == ENGINE_REQUEST_ID
            ]

        for _ in range(75):
            events = request_events()
            if events and events[-1][0] == "on_request_completed":
                break
            await asyncio.sleep(0.2)
        assert events and events[-1][0] == "on_request_completed"

        names = [name for name, _ in events]
        assert names[0] == "on_request_added"
        assert names[1] == "on_prefill_complete"
        assert names[2:-1] == ["on_decode_progress"] * (len(names) - 3)

        # on_request_added reports the prompt's token ids; decode progress is
        # strictly increasing and ends at the engine's completion-token count.
        assert len(events[0][1][2]) == usage["prompt_tokens"]
        decode_counts = [
            args[1] for name, args in events if name == "on_decode_progress"
        ]
        assert decode_counts == sorted(set(decode_counts))
        assert decode_counts[-1] == usage["completion_tokens"]


if __name__ == "__main__":
    if not ray.is_initialized():
        ray.init(address="auto")
    sys.exit(pytest.main(["-v", "-s", __file__]))
