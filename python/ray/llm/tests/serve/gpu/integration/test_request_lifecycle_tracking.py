"""GPU end-to-end test for KV-router request-lifecycle tracking.

Deploys a direct-streaming LLM app on a real vLLM engine, with a recording
subclass of the deployment-scoped ``KVRouterActor`` attached, streams a real
chat completion, and asserts the lifecycle hooks fired in order with token
counts exactly matching the engine's reported usage.

``KVAwareRouter`` replica selection lands in a later branch, so the actor is
attached directly via ``deployment_actors`` and requests route with the
default router; the engine-client token tracking is router-independent.
"""

import asyncio
import json
import math
import os
import sys
import tempfile

import pytest
import requests

# Direct ingress gives each replica its own backend HTTP server so the test
# can hit the replica's vLLM ASGI app directly.
os.environ["RAY_SERVE_LLM_ENABLE_DIRECT_STREAMING"] = "1"
os.environ["RAY_SERVE_ENABLE_DIRECT_INGRESS"] = "1"

import ray  # noqa: E402
from ray import serve  # noqa: E402
from ray.llm._internal.serve.core.ingress.builder import (  # noqa: E402
    _build_direct_streaming_llm_deployment,
)
from ray.llm._internal.serve.routing_policies.kv_aware.kv_aware_actor import (  # noqa: E402
    KV_ROUTER_ACTOR_NAME,
    KVRouterActor,
    derive_kv_block_size,
)
from ray.serve._private.constants import (  # noqa: E402
    SERVE_DEPLOYMENT_ACTOR_PREFIX,
    SERVE_NAMESPACE,
)
from ray.serve.config import DeploymentActorConfig  # noqa: E402
from ray.serve.llm import LLMConfig, ModelLoadingConfig  # noqa: E402

MODEL_ID = "Qwen/Qwen2.5-0.5B-Instruct"
APP_NAME = "lifecycle_tracking_gpu_test"
REQUEST_ID = "gpu-req-1"
MAX_TOKENS = 32
# vLLM adopts the X-Request-Id header as the engine request id, prefixed per
# endpoint; the engine-level id is what token tracking reports to the actor.
ENGINE_REQUEST_ID = f"chatcmpl-{REQUEST_ID}"


@ray.remote(num_cpus=0)
class RecordingKVRouterActor(KVRouterActor.__ray_actor_class__):
    """The real KVRouterActor, additionally recording every event it applies.

    Eviction on completion makes the hook calls unobservable from state alone;
    the event log preserves them for assertion.
    """

    def __init__(self, block_size):
        super().__init__(block_size=block_size)
        self._event_log = []

    async def on_lifecycle_events(self, events):
        self._event_log.extend(events)
        await super().on_lifecycle_events(events)

    def get_event_log(self):
        return self._event_log


def discover_deployment_actor(app_name, deployment_name, actor_name):
    """Resolve a deployment-scoped actor by its registered name.

    The test driver isn't a replica, so ``get_deployment_actor`` is
    unavailable; match the name's stable prefix/suffix instead (the middle
    embeds an opaque code_version).
    """
    prefix = f"{SERVE_DEPLOYMENT_ACTOR_PREFIX}{app_name}::{deployment_name}::"
    suffix = f"::{actor_name}"
    for entry in ray.util.list_named_actors(all_namespaces=True):
        name = entry.get("name") or ""
        if (
            entry.get("namespace") == SERVE_NAMESPACE
            and name.startswith(prefix)
            and name.endswith(suffix)
        ):
            return ray.get_actor(name, namespace=SERVE_NAMESPACE)
    return None


class TestLifecycleTrackingGPU:
    @pytest.fixture(scope="class")
    def deployed_handle(self):
        """Deploy a direct-streaming LLMServer with the recording actor."""
        if not ray.is_initialized():
            # An empty working_dir keeps the runtime-env package tiny; the
            # repo root would exceed the upload size limit.
            ray.init(
                address="auto",
                runtime_env={"working_dir": tempfile.mkdtemp(prefix="kv_lc_wd_")},
            )
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
                        init_kwargs={"block_size": derive_kv_block_size(engine_kwargs)},
                        actor_options={"num_cpus": 0},
                    )
                ],
            ),
            engine_kwargs=engine_kwargs,
            placement_group_config={"bundles": [{"GPU": 1}]},
            # The replica worker process reads these constants at import time.
            runtime_env=dict(
                env_vars={
                    "VLLM_DISABLE_COMPILE_CACHE": "1",
                    "RAY_SERVE_ENABLE_DIRECT_INGRESS": "1",
                    "RAY_SERVE_LLM_ENABLE_DIRECT_STREAMING": "1",
                },
            ),
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
    async def test_exact_lifecycle_tracking(self, deployed_handle):
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
            "messages": [
                {"role": "user", "content": "Count slowly from one to twenty."}
            ],
            "stream": True,
            "stream_options": {"include_usage": True},
            "max_tokens": MAX_TOKENS,
            "temperature": 0.0,
        }
        headers = {"X-Request-Id": REQUEST_ID}

        # Snapshot the actor's view after every streamed chunk: completion
        # evicts the request, so state is only observable while streaming.
        usage = None
        snapshots = []
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
                snapshot = await actor.get_request_lifecycle.remote(ENGINE_REQUEST_ID)
                if snapshot is not None:
                    snapshots.append(snapshot)

        assert usage is not None, "expected a final usage chunk"
        assert snapshots, "request was never observed in flight on the actor"

        # Every in-flight snapshot upholds exact token and block accounting.
        block_size = await actor.get_block_size.remote()
        prompt_blocks = math.ceil(usage["prompt_tokens"] / block_size)
        previous_output_tokens = 0
        for snapshot in snapshots:
            assert snapshot["prompt_tokens"] == usage["prompt_tokens"]
            assert previous_output_tokens <= snapshot["output_tokens"]
            assert snapshot["output_tokens"] <= usage["completion_tokens"]
            previous_output_tokens = snapshot["output_tokens"]
            total_blocks = math.ceil(
                (usage["prompt_tokens"] + snapshot["output_tokens"]) / block_size
            )
            assert snapshot["total_blocks"] == total_blocks
            assert snapshot["output_blocks"] == total_blocks - prompt_blocks
            if snapshot["output_tokens"] > 0:
                assert snapshot["prefill_completed"] is True

        # Load was attributed to a worker id that membership tracking also sees.
        worker_id = snapshots[-1]["worker_id"]
        assert worker_id in await actor.get_candidate_worker_ids.remote()

        # Completion evicts the request from the actor's active-load view.
        for _ in range(50):
            lifecycle = await actor.get_request_lifecycle.remote(ENGINE_REQUEST_ID)
            if lifecycle is None:
                break
            await asyncio.sleep(0.2)
        assert lifecycle is None, "completed request was not evicted"
        assert await actor.get_active_request_ids.remote() == []

        events = [
            (name, args)
            for name, args in await actor.get_event_log.remote()
            if args[0] == ENGINE_REQUEST_ID
        ]
        names = [name for name, _ in events]
        assert names[0] == "on_request_added"
        assert names[1] == "on_prefill_complete"
        assert names[-1] == "on_request_completed"
        assert names[2:-1] == ["on_decode_progress"] * (len(names) - 3)

        added_args = events[0][1]
        assert added_args[1] == worker_id
        assert added_args[2] == usage["prompt_tokens"]
        decode_counts = [
            args[1] for name, args in events if name == "on_decode_progress"
        ]
        assert decode_counts == sorted(set(decode_counts))  # strictly increasing
        assert decode_counts[-1] == usage["completion_tokens"]


if __name__ == "__main__":
    if not ray.is_initialized():
        ray.init(address="auto")
    sys.exit(pytest.main(["-v", "-s", __file__]))
