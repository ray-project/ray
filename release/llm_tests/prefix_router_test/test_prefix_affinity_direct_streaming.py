"""GPU release test: PrefixCacheAffinityRouter under direct streaming.

Reproduces the bug fixed by https://github.com/ray-project/ray/pull/66489.
With direct streaming, LLMRouter picks a replica via
``handle.choose_replica(_reserve=False)``. Before the fix, that path never
called the request router's ``on_request_routed`` hook, so
PrefixCacheAffinityRouter's prefix tree stayed empty and every request fell
back to ``get_smallest_tenants()``, which returns every replica tied at 0
characters in the same order every time -- so every request went to the same
replica. This deploys a real direct-streaming cluster with
PrefixCacheAffinityRouter and asserts the two behaviors that regressed:
unrelated prompts spread across replicas, and repeated prompts pin to one.
"""

import asyncio
import time
import uuid
from collections import Counter
from types import SimpleNamespace

import pytest

import ray
from ray import serve
from ray.serve.config import RequestRouterConfig
from ray.serve.llm import LLMConfig, ModelLoadingConfig, build_openai_app
from ray.serve.llm.request_router import PrefixCacheAffinityRouter

MODEL_ID = "qwen3-0.6b"
MODEL_SOURCE = "Qwen/Qwen3-0.6B"
APP_NAME = "prefix_affinity_direct_streaming_test"
NUM_REPLICAS = 4

NUM_UNIQUE_PROMPTS = 60
# The bug's signature was 100% of requests on one replica. A cap well below
# that still fails hard on a regression while tolerating normal
# length-driven skew in the smallest-tenant tie-break.
MAX_SHARE_PER_REPLICA = 0.5

NUM_PREFIX_GROUPS = 4
REPEATS_PER_GROUP = 5


def _chat_payload(text: str) -> SimpleNamespace:
    """Same shape ``LLMRouter._pick_replica`` passes: a parsed JSON body."""
    return SimpleNamespace(messages=[{"role": "user", "content": text}])


def unique_prompt(i: int) -> str:
    """A prompt that shares no prefix with any other prompt in this test.

    A fresh uuid at position 0 keeps the match rate against every other
    prompt at ~0, so routing must use the smallest-tenant tie-break, not a
    prefix match.
    """
    return f"{uuid.uuid4().hex} unrelated request body #{i}."


def prefix_group_text(group_id: str) -> str:
    """One fixed string per group. Every repeat in a group sends this exact
    text, so a repeat's match rate against its own group is 1.0."""
    return (
        f"{group_id} shares this long common preamble across every repeat "
        "in its conversation, establishing context. "
    ) * 4


async def wait_for_replicas(handle, expected_replicas, timeout_s=120):
    """Block until this handle's local router has picked ``expected_replicas``
    distinct replicas. The controller's replica-set update reaches this
    handle's router by long polling, so it can lag ``serve.run()`` returning
    by a beat."""
    seen = set()
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        async with handle.choose_replica() as selection:
            seen.add(selection._replica.replica_id.to_full_id_str())
        if len(seen) >= expected_replicas:
            return
        await asyncio.sleep(0.5)
    raise AssertionError(
        f"Expected {expected_replicas} replicas, saw {len(seen)}: {seen}"
    )


class TestPrefixAffinityDirectStreaming:
    @pytest.fixture(scope="class")
    def deployed_handle(self):
        """Deploy direct-streaming LLMServer replicas routed by
        PrefixCacheAffinityRouter, one dedicated GPU each."""
        if not ray.is_initialized():
            ray.init(address="auto")
        serve.shutdown()

        llm_config = LLMConfig(
            model_loading_config=ModelLoadingConfig(
                model_id=MODEL_ID,
                model_source=MODEL_SOURCE,
            ),
            deployment_config=dict(
                autoscaling_config=dict(
                    min_replicas=NUM_REPLICAS, max_replicas=NUM_REPLICAS
                ),
                request_router_config=RequestRouterConfig(
                    request_router_class=PrefixCacheAffinityRouter
                ),
            ),
            engine_kwargs=dict(
                max_model_len=2048,
                enforce_eager=True,
                gpu_memory_utilization=0.4,
            ),
            placement_group_config={"bundles": [{"GPU": 1}]},
        )
        app = build_openai_app({"llm_configs": [llm_config]})
        handle = serve.run(app, name=APP_NAME)
        yield handle
        serve.shutdown()

    @pytest.mark.asyncio
    @pytest.mark.timeout(600)
    async def test_new_prompts_spread_across_replicas(self, deployed_handle):
        """With an empty prefix tree, unrelated prompts must spread across
        every replica. Before the fix, ``on_request_routed`` never ran on
        the pick-only path, so the tree stayed empty forever and
        ``get_smallest_tenants()`` returned every replica tied at 0 chars in
        the same order every time: 100% of requests went to one replica."""
        await wait_for_replicas(deployed_handle, NUM_REPLICAS)

        picked = []
        for i in range(NUM_UNIQUE_PROMPTS):
            payload = _chat_payload(unique_prompt(i))
            async with deployed_handle.choose_replica(
                payload, _reserve=False
            ) as selection:
                picked.append(selection._replica.replica_id.to_full_id_str())

        counts = Counter(picked)
        assert len(counts) == NUM_REPLICAS, (
            f"Expected all {NUM_REPLICAS} replicas to receive at least one "
            f"of {NUM_UNIQUE_PROMPTS} unrelated prompts, got {dict(counts)}"
        )
        busiest = counts.most_common(1)[0]
        assert busiest[1] <= NUM_UNIQUE_PROMPTS * MAX_SHARE_PER_REPLICA, (
            f"Replica {busiest[0]} got {busiest[1]}/{NUM_UNIQUE_PROMPTS} "
            f"requests -- looks like the all-to-one-replica regression: {dict(counts)}"
        )

    @pytest.mark.asyncio
    @pytest.mark.timeout(600)
    async def test_repeated_prompt_pins_to_one_replica(self, deployed_handle):
        """Repeats of the same prompt must land on the same replica every
        time. This only works if the first pick's ``on_request_routed`` call
        inserted the prompt into the prefix tree, so the repeat's prefix
        match rate (1.0) beats ``match_rate_threshold`` and the router
        returns the matched tenant instead of falling back."""
        await wait_for_replicas(deployed_handle, NUM_REPLICAS)

        groups = [f"group-{uuid.uuid4().hex}" for _ in range(NUM_PREFIX_GROUPS)]
        group_replicas = {g: [] for g in groups}

        # Interleave groups (round-robin) rather than finishing one group
        # before starting the next, so affinity is proven against
        # concurrent unrelated inserts from sibling groups, not just a
        # quiet tree.
        for _ in range(REPEATS_PER_GROUP):
            for group_id in groups:
                payload = _chat_payload(prefix_group_text(group_id))
                async with deployed_handle.choose_replica(
                    payload, _reserve=False
                ) as selection:
                    group_replicas[group_id].append(
                        selection._replica.replica_id.to_full_id_str()
                    )

        for group_id, replicas in group_replicas.items():
            assert len(set(replicas)) == 1, (
                f"{group_id}'s {REPEATS_PER_GROUP} repeats landed on "
                f"{len(set(replicas))} different replicas, not one: {replicas}"
            )


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-s", __file__]))
