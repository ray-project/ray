import asyncio
import time

import pytest

import ray
from ray._common.utils import get_or_create_event_loop
from ray.llm._internal.serve.routing_policies.prefix_aware.prefix_aware_router import (
    PrefixCacheAffinityRouter,
)
from ray.llm._internal.serve.routing_policies.prefix_aware.prefix_tree import (
    PrefixTreeActor,
)
from ray.serve._private.common import (
    DeploymentHandleSource,
    DeploymentID,
    RequestMetadata,
)
from ray.serve._private.request_router.common import PendingRequest
from ray.serve._private.test_utils import MockTimer
from ray.serve._private.utils import generate_request_id
from ray.serve.tests.unit.test_pow_2_request_router import (
    FakeRunningReplica,
)  # Reuse the FakeRunningReplica from the Pow2 test

TIMER = MockTimer()
DEFAULT_MAX_ONGOING_REQUESTS = 10


# === Fixtures ===


@pytest.fixture
def tree_actor():
    """Create a fresh PrefixTreeActor instance."""
    actor = PrefixTreeActor.options(name="PrefixTreeActor").remote()
    yield actor
    ray.kill(actor)


@pytest.fixture
def prefix_request_router(tree_actor, request):
    """Create a fresh PrefixCacheAffinityRouter with connected tree_actor."""
    params = getattr(request, "param", {})

    async def construct_request_router(loop: asyncio.AbstractEventLoop):
        request_router = PrefixCacheAffinityRouter(
            deployment_id=DeploymentID(name="TEST_DEPLOYMENT"),
            handle_source=DeploymentHandleSource.REPLICA,
            use_replica_queue_len_cache=False,
            get_curr_time_s=TIMER.time,
        )
        return request_router

    request_router = asyncio.new_event_loop().run_until_complete(
        construct_request_router(get_or_create_event_loop())
    )
    request_router.initialize_state(
        imbalanced_threshold=params.get("imbalanced_threshold", 10),
        match_rate_threshold=params.get("match_rate_threshold", 0.1),
        do_eviction=params.get("do_eviction", False),
        eviction_threshold_chars=params.get("eviction_threshold_chars"),
        eviction_target_chars=params.get("eviction_target_chars"),
        eviction_interval_secs=params.get("eviction_interval_secs"),
        tree_actor=tree_actor,
    )

    yield request_router
    assert request_router.curr_num_routing_tasks == 0
    assert request_router.num_pending_requests == 0


# === Helpers ===


class PromptRequest:
    def __init__(self, prompt: str):
        self.prompt = prompt


class ChatRequest:
    def __init__(self, messages):
        self.messages = messages


def fake_pending_request(prompt=None, messages=None) -> PendingRequest:
    if prompt is not None:
        args = [PromptRequest(prompt)]
    elif messages is not None:
        args = [ChatRequest(messages)]
    else:
        args = []

    return PendingRequest(
        args=args,
        kwargs={},
        metadata=RequestMetadata(
            request_id=generate_request_id(),
            internal_request_id=generate_request_id(),
            multiplexed_model_id="",
        ),
        created_at=time.time(),
    )


# === Tests ===
class TestPow2FallbackBehavior:
    """Tests fallback to Pow2 when prefix-aware logic should be skipped."""

    @pytest.mark.asyncio
    async def test_fallback_when_no_prompt(self, prefix_request_router):
        """No args → prefix logic skipped → falls back to least busy replica."""
        r1 = FakeRunningReplica("r1")
        r1.set_queue_len_response(0)
        r2 = FakeRunningReplica("r2")
        r2.set_queue_len_response(5)
        prefix_request_router.update_replicas([r1, r2])

        tenant_to_char_count = ray.get(
            prefix_request_router._tree_actor.getattr.remote("tenant_to_char_count")
        )
        assert tenant_to_char_count == {
            r1.replica_id.to_full_id_str(): 0,
            r2.replica_id.to_full_id_str(): 0,
        }

        req = fake_pending_request()
        for _ in range(10):
            chosen = await prefix_request_router._choose_replica_for_request(req)
            assert chosen == r1

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "prefix_request_router", [{"imbalanced_threshold": 2}], indirect=True
    )
    async def test_fallback_when_imbalanced(self, prefix_request_router):
        """If load is imbalanced beyond threshold, prefix matching is skipped."""
        r1 = FakeRunningReplica("r1")
        r1.set_queue_len_response(0)
        r2 = FakeRunningReplica("r2")
        r2.set_queue_len_response(10)
        prefix_request_router.update_replicas([r1, r2])

        ray.get(
            prefix_request_router._tree_actor.insert.remote(
                "hello world", r2.replica_id.to_full_id_str(), time.time()
            )
        )

        tenant_to_char_count = ray.get(
            prefix_request_router._tree_actor.getattr.remote("tenant_to_char_count")
        )
        assert tenant_to_char_count == {
            r1.replica_id.to_full_id_str(): 0,
            r2.replica_id.to_full_id_str(): 11,
        }

        matched_text, matched_tenants = ray.get(
            prefix_request_router._tree_actor.prefix_match.remote("hello world")
        )
        assert matched_text == "hello world"
        assert matched_tenants == [r2.replica_id.to_full_id_str()]

        req = fake_pending_request(prompt="hello world")
        for _ in range(10):
            chosen = await prefix_request_router._choose_replica_for_request(req)
            # Even though r2 has a higher match rate, it is not chosen because the load is imbalanced
            assert chosen == r1


class TestPrefixAwareLogic:
    """Tests that exercise actual prefix-aware request routing logic."""

    @pytest.mark.asyncio
    async def test_high_match_rate_selects_matching_replica(
        self, prefix_request_router
    ):
        """High match rate → use matched replica instead of Pow2."""
        r1 = FakeRunningReplica("r1")
        r1.set_queue_len_response(0)
        r2 = FakeRunningReplica("r2")
        r2.set_queue_len_response(0)
        prefix_request_router.update_replicas([r1, r2])
        ray.get(
            prefix_request_router._tree_actor.insert.remote(
                "Hello", r2.replica_id.to_full_id_str(), time.time()
            )
        )
        # Verify prefix match and smallest tenants
        matched_text, matched_tenants = ray.get(
            prefix_request_router._tree_actor.prefix_match.remote("Hello world")
        )
        assert matched_text == "Hello"
        assert matched_tenants == [r2.replica_id.to_full_id_str()]

        tenant_counts = ray.get(
            prefix_request_router._tree_actor.getattr.remote("tenant_to_char_count")
        )
        assert tenant_counts[r1.replica_id.to_full_id_str()] == 0
        assert tenant_counts[r2.replica_id.to_full_id_str()] == 5

        prompt_req = fake_pending_request(prompt="Hello world")
        for _ in range(10):
            chosen = await prefix_request_router._choose_replica_for_request(prompt_req)
            assert chosen == r2
        chat_req = fake_pending_request(
            messages=[{"content": "Hello"}, {"content": " world"}]
        )
        for _ in range(10):
            chosen = await prefix_request_router._choose_replica_for_request(chat_req)
            assert chosen == r2

    @pytest.mark.asyncio
    async def test_low_match_rate_uses_smallest_tree(self, prefix_request_router):
        """Low match rate → use replica with least total inserted characters."""
        r1 = FakeRunningReplica("r1")
        r1.set_queue_len_response(0)
        r2 = FakeRunningReplica("r2")
        r2.set_queue_len_response(0)
        prefix_request_router.update_replicas([r1, r2])

        # Make r2 "bigger" tenant
        ray.get(
            prefix_request_router._tree_actor.insert.remote(
                "hi", r1.replica_id.to_full_id_str(), time.time()
            )
        )
        ray.get(
            prefix_request_router._tree_actor.insert.remote(
                "longtext", r2.replica_id.to_full_id_str(), time.time()
            )
        )

        # Verify tenant character counts
        tenant_counts = ray.get(
            prefix_request_router._tree_actor.getattr.remote("tenant_to_char_count")
        )
        assert tenant_counts[r1.replica_id.to_full_id_str()] == 2  # "hi"
        assert tenant_counts[r2.replica_id.to_full_id_str()] == 8  # "longtext"

        prompt_req = fake_pending_request(prompt="z")
        for _ in range(10):
            # Both tenants have 0% match rate, so the smaller tenant (r1) is chosen
            assert (
                await prefix_request_router._choose_replica_for_request(prompt_req)
                == r1
            )

        chat_req = fake_pending_request(messages=[{"content": "z"}])
        for _ in range(10):
            # Both tenants have 0% match rate, so the smaller tenant (r1) is chosen
            assert (
                await prefix_request_router._choose_replica_for_request(chat_req) == r1
            )


class TestEvictionBehavior:
    """Tests for prefix tree eviction behavior."""

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "prefix_request_router",
        [
            {
                "do_eviction": True,
                "eviction_threshold_chars": 10,
                "eviction_target_chars": 5,
                "eviction_interval_secs": 1.0,
            }
        ],
        indirect=True,
    )
    async def test_eviction_task_creation(self, prefix_request_router):
        """Test that eviction task is only created after update_replicas."""
        # Before update_replicas
        assert not prefix_request_router._eviction_loop_running

        # After update_replicas
        r1 = FakeRunningReplica("r1")
        prefix_request_router.update_replicas([r1])
        assert prefix_request_router._eviction_loop_running

        # After stop_eviction_loop
        ray.get(prefix_request_router._tree_actor.stop_eviction_loop.remote())
        await asyncio.sleep(0.1)


class TestPromptNormalization:
    """Tests for input normalization in the prefix-aware router."""

    def test_normalize_prompt_string(self, prefix_request_router):
        req = fake_pending_request(prompt="Hello world")
        normalized = prefix_request_router._extract_text_from_request(req)
        assert normalized == "Hello world"

    def test_normalize_messages_list_of_strings(self, prefix_request_router):
        req = fake_pending_request(messages=["Hello", " ", "world"])
        normalized = prefix_request_router._extract_text_from_request(req)
        assert normalized == "Hello world"

    def test_normalize_messages_dict_content_string(self, prefix_request_router):
        req = fake_pending_request(
            messages=[
                {"content": "Hello"},
                {"content": " world"},
            ]
        )
        normalized = prefix_request_router._extract_text_from_request(req)
        assert normalized == "Hello world"

    def test_normalize_messages_dict_content_list_of_dicts_text(
        self, prefix_request_router
    ):
        req = fake_pending_request(
            messages=[
                {
                    "content": [
                        {"type": "text", "text": "Hello"},
                        {"type": "text", "text": " world"},
                    ]
                }
            ]
        )
        normalized = prefix_request_router._extract_text_from_request(req)
        assert normalized == "Hello world"

    def test_normalize_messages_dict_content_list_of_strings(
        self, prefix_request_router
    ):
        req = fake_pending_request(messages=[{"content": ["Hello", " ", "world"]}])
        normalized = prefix_request_router._extract_text_from_request(req)
        assert normalized == "Hello world"

    def test_normalize_unsupported_returns_empty(self, prefix_request_router):
        # For now, unsupported multimodal parts should be ignored, resulting in empty string
        req = fake_pending_request(
            messages=[
                {
                    "content": [
                        {
                            "type": "image_url",
                            "image_url": {"url": "http://example.com"},
                        },
                    ]
                }
            ]
        )
        normalized = prefix_request_router._extract_text_from_request(req)
        assert normalized == ""

    def test_extract_raises_when_no_prompt_or_messages(self, prefix_request_router):
        with pytest.raises(ValueError):
            _ = prefix_request_router._extract_text_from_request(fake_pending_request())

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "prefix_request_router",
        [
            {
                "do_eviction": True,
                "eviction_threshold_chars": 10,
                "eviction_target_chars": 5,
                "eviction_interval_secs": 1.0,
            }
        ],
        indirect=True,
    )
    async def test_eviction_threshold_behavior(self, prefix_request_router):
        """Test that eviction reduces tree size below threshold after interval."""
        r1 = FakeRunningReplica("r1")
        prefix_request_router.update_replicas([r1])

        # Insert text that exceeds eviction_threshold_chars
        ray.get(
            prefix_request_router._tree_actor.insert.remote(
                "verylongtext", r1.replica_id.to_full_id_str(), time.time()
            )
        )
        ray.get(
            prefix_request_router._tree_actor.insert.remote(
                "anotherlongtext", r1.replica_id.to_full_id_str(), time.time()
            )
        )

        # Verify initial size exceeds eviction_threshold_chars
        tenant_counts = ray.get(
            prefix_request_router._tree_actor.getattr.remote("tenant_to_char_count")
        )
        assert tenant_counts[r1.replica_id.to_full_id_str()] > 10

        # Wait for eviction interval
        await asyncio.sleep(1.1)

        # Verify size is reduced below eviction_target_chars
        tenant_counts = ray.get(
            prefix_request_router._tree_actor.getattr.remote("tenant_to_char_count")
        )
        assert tenant_counts[r1.replica_id.to_full_id_str()] <= 5

        ray.get(prefix_request_router._tree_actor.stop_eviction_loop.remote())
        await asyncio.sleep(0.1)


class TestMultiDeploymentIsolation:
    """Tests that multiple deployments get isolated prefix tree actors."""

    @pytest.mark.asyncio
    async def test_two_deployments_get_separate_tree_actors(self):
        """Verify that two deployments using PrefixCacheAffinityRouter get
        deployment-specific prefix tree actors to avoid replica ID conflicts."""

        # Create two routers for different deployments (e.g., Prefill and Decode in PD setup)
        async def construct_router(deployment_name: str):
            router = PrefixCacheAffinityRouter(
                deployment_id=DeploymentID(name=deployment_name),
                handle_source=DeploymentHandleSource.REPLICA,
                use_replica_queue_len_cache=False,
                get_curr_time_s=TIMER.time,
            )
            router.initialize_state()
            return router

        prefill_router = await construct_router("Prefill:deepseek")
        decode_router = await construct_router("Decode:deepseek")

        # Create replicas for each deployment
        prefill_r1 = FakeRunningReplica("prefill_r1")
        prefill_r1.set_queue_len_response(0)
        prefill_r2 = FakeRunningReplica("prefill_r2")
        prefill_r2.set_queue_len_response(0)

        decode_r1 = FakeRunningReplica("decode_r1")
        decode_r1.set_queue_len_response(0)
        decode_r2 = FakeRunningReplica("decode_r2")
        decode_r2.set_queue_len_response(0)

        # Update replicas for each router
        prefill_router.update_replicas([prefill_r1, prefill_r2])
        decode_router.update_replicas([decode_r1, decode_r2])

        # Verify replicas are tracked independently in each tree
        prefill_tenants = ray.get(
            prefill_router._tree_actor.getattr.remote("tenant_to_char_count")
        )
        decode_tenants = ray.get(
            decode_router._tree_actor.getattr.remote("tenant_to_char_count")
        )

        # Each tree should only know about its own replicas
        assert set(prefill_tenants.keys()) == {
            prefill_r1.replica_id.to_full_id_str(),
            prefill_r2.replica_id.to_full_id_str(),
        }
        assert set(decode_tenants.keys()) == {
            decode_r1.replica_id.to_full_id_str(),
            decode_r2.replica_id.to_full_id_str(),
        }

        # Insert text into prefill tree
        ray.get(
            prefill_router._tree_actor.insert.remote(
                "prefill text", prefill_r1.replica_id.to_full_id_str(), time.time()
            )
        )

        # Insert text into decode tree
        ray.get(
            decode_router._tree_actor.insert.remote(
                "decode text", decode_r1.replica_id.to_full_id_str(), time.time()
            )
        )

        # Verify routing works correctly for both deployments without KeyErrors
        prefill_req = fake_pending_request(prompt="prefill text continued")
        chosen_prefill = await prefill_router._choose_replica_for_request(prefill_req)
        assert chosen_prefill == prefill_r1

        decode_req = fake_pending_request(prompt="decode text continued")
        chosen_decode = await decode_router._choose_replica_for_request(decode_req)
        assert chosen_decode == decode_r1

        # Verify trees remain isolated
        prefill_tenants_after = ray.get(
            prefill_router._tree_actor.getattr.remote("tenant_to_char_count")
        )
        decode_tenants_after = ray.get(
            decode_router._tree_actor.getattr.remote("tenant_to_char_count")
        )

        assert prefill_tenants_after[prefill_r1.replica_id.to_full_id_str()] > 0
        assert prefill_tenants_after[prefill_r2.replica_id.to_full_id_str()] == 0
        assert decode_tenants_after[decode_r1.replica_id.to_full_id_str()] > 0
        assert decode_tenants_after[decode_r2.replica_id.to_full_id_str()] == 0

        # Cleanup
        ray.kill(prefill_router._tree_actor)
        ray.kill(decode_router._tree_actor)


if __name__ == "__main__":
    import sys

    exit_code = pytest.main(["-vs", __file__])
    sys.exit(exit_code)
