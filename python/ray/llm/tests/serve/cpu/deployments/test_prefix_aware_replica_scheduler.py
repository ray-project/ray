# test_prefix_aware_replica_scheduler.py

import pytest
import ray
from ray._common.utils import get_or_create_event_loop
import asyncio
import time

from test_pow_2_replica_scheduler import (
    FakeRunningReplica,
)  # Reuse the FakeRunningReplica from the Pow2 test

from ray.serve._private.replica_scheduler.prefix_aware_scheduler import (
    PrefixAwareReplicaScheduler,
)
from ray.llm._internal.serve.replica_scheduler.prefix_aware.prefix_tree import (
    PrefixTreeActor,
)
from ray.serve._private.common import (
    DeploymentHandleSource,
    DeploymentID,
    RequestMetadata,
)
from ray.serve._private.replica_scheduler.common import PendingRequest
from ray.serve._private.utils import generate_request_id
from ray.serve._private.test_utils import MockTimer

TIMER = MockTimer()
DEFAULT_MAX_ONGOING_REQUESTS = 10


# === Fixtures ===


@pytest.fixture
async def tree_actor():
    """Create a fresh PrefixTreeActor instance."""
    # ray.init(ignore_reinit_error=True)
    actor = PrefixTreeActor.options(name=None).remote()
    yield actor
    # await asyncio.sleep(0.1)
    # ray.shutdown()


@pytest.fixture
def prefix_scheduler(tree_actor, request):
    """Create a fresh PrefixAwareReplicaScheduler with connected tree_actor."""
    params = getattr(request, "param", {})

    async def construct_scheduler(loop: asyncio.AbstractEventLoop):
        scheduler = PrefixAwareReplicaScheduler(
            deployment_id=DeploymentID(name="TEST_DEPLOYMENT"),
            handle_source=DeploymentHandleSource.REPLICA,
            # prefer_local_node=params.get("prefer_local_node", False),
            # prefer_local_az=params.get("prefer_local_az", False),
            # self_node_id="node",
            # self_actor_id="actor",
            # self_actor_handle=None,
            # self_availability_zone="az",
            use_replica_queue_len_cache=False,
            imbalanced_threshold=params.get("imbalanced_threshold", 10),
            match_rate_threshold=params.get("match_rate_threshold", 0.1),
            do_eviction=params.get("do_eviction", False),
            eviction_threshold_chars=params.get("eviction_threshold_chars"),
            eviction_target_chars=params.get("eviction_target_chars"),
            eviction_interval_secs=params.get("eviction_interval_secs"),
            get_curr_time_s=TIMER.time,
            tree_actor=tree_actor,
        )
        return scheduler

    s = asyncio.new_event_loop().run_until_complete(
        construct_scheduler(get_or_create_event_loop())
    )

    yield s
    assert s.curr_num_scheduling_tasks == 0
    assert s.num_pending_requests == 0


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
    async def test_fallback_when_no_prompt(self, prefix_scheduler):
        """No args → prefix logic skipped → falls back to least busy replica."""
        r1 = FakeRunningReplica("r1")
        r1.set_queue_len_response(0)
        r2 = FakeRunningReplica("r2")
        r2.set_queue_len_response(5)
        prefix_scheduler.update_replicas([r1, r2])

        tenant_to_char_count = ray.get(
            prefix_scheduler._tree_actor.getattr.remote("tenant_to_char_count")
        )
        assert tenant_to_char_count == {
            r1.replica_id.to_full_id_str(): 0,
            r2.replica_id.to_full_id_str(): 0,
        }

        req = fake_pending_request()
        for _ in range(10):
            chosen = await prefix_scheduler.choose_replica_for_request(req)
            assert chosen == r1

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "prefix_scheduler", [{"imbalanced_threshold": 2}], indirect=True
    )
    async def test_fallback_when_imbalanced(self, prefix_scheduler):
        """If load is imbalanced beyond threshold, prefix matching is skipped."""
        r1 = FakeRunningReplica("r1")
        r1.set_queue_len_response(0)
        r2 = FakeRunningReplica("r2")
        r2.set_queue_len_response(10)
        prefix_scheduler.update_replicas([r1, r2])

        ray.get(
            prefix_scheduler._tree_actor.insert.remote(
                "hello world", r2.replica_id.to_full_id_str(), time.time()
            )
        )

        tenant_to_char_count = ray.get(
            prefix_scheduler._tree_actor.getattr.remote("tenant_to_char_count")
        )
        assert tenant_to_char_count == {
            r1.replica_id.to_full_id_str(): 0,
            r2.replica_id.to_full_id_str(): 11,
        }

        matched_text, matched_tenants = ray.get(
            prefix_scheduler._tree_actor.prefix_match.remote("hello world")
        )
        assert matched_text == "hello world"
        assert matched_tenants == [r2.replica_id.to_full_id_str()]

        req = fake_pending_request(prompt="hello world")
        for _ in range(10):
            chosen = await prefix_scheduler.choose_replica_for_request(req)
            # Even though r2 has a higher match rate, it is not chosen because the load is imbalanced
            assert chosen == r1


class TestPrefixAwareLogic:
    """Tests that exercise actual prefix-aware scheduling logic."""

    @pytest.mark.asyncio
    async def test_high_match_rate_selects_matching_replica(self, prefix_scheduler):
        """High match rate → use matched replica instead of Pow2."""
        r1 = FakeRunningReplica("r1")
        r1.set_queue_len_response(0)
        r2 = FakeRunningReplica("r2")
        r2.set_queue_len_response(0)
        prefix_scheduler.update_replicas([r1, r2])
        ray.get(
            prefix_scheduler._tree_actor.insert.remote(
                "Hello", r2.replica_id.to_full_id_str(), time.time()
            )
        )
        # Verify prefix match and smallest tenants
        matched_text, matched_tenants = ray.get(
            prefix_scheduler._tree_actor.prefix_match.remote("Hello world")
        )
        assert matched_text == "Hello"
        assert matched_tenants == [r2.replica_id.to_full_id_str()]

        tenant_counts = ray.get(
            prefix_scheduler._tree_actor.getattr.remote("tenant_to_char_count")
        )
        assert tenant_counts[r1.replica_id.to_full_id_str()] == 0
        assert tenant_counts[r2.replica_id.to_full_id_str()] == 5

        prompt_req = fake_pending_request(prompt="Hello world")
        for _ in range(10):
            chosen = await prefix_scheduler.choose_replica_for_request(prompt_req)
            assert chosen == r2
        chat_req = fake_pending_request(
            messages=[{"content": "Hello"}, {"content": " world"}]
        )
        for _ in range(10):
            chosen = await prefix_scheduler.choose_replica_for_request(chat_req)
            assert chosen == r2

    @pytest.mark.asyncio
    async def test_low_match_rate_uses_smallest_tree(self, prefix_scheduler):
        """Low match rate → use replica with least total inserted characters."""
        r1 = FakeRunningReplica("r1")
        r1.set_queue_len_response(0)
        r2 = FakeRunningReplica("r2")
        r2.set_queue_len_response(0)
        prefix_scheduler.update_replicas([r1, r2])

        # Make r2 "bigger" tenant
        ray.get(
            prefix_scheduler._tree_actor.insert.remote(
                "hi", r1.replica_id.to_full_id_str(), time.time()
            )
        )
        ray.get(
            prefix_scheduler._tree_actor.insert.remote(
                "longtext", r2.replica_id.to_full_id_str(), time.time()
            )
        )

        # Verify tenant character counts
        tenant_counts = ray.get(
            prefix_scheduler._tree_actor.getattr.remote("tenant_to_char_count")
        )
        assert tenant_counts[r1.replica_id.to_full_id_str()] == 2  # "hi"
        assert tenant_counts[r2.replica_id.to_full_id_str()] == 8  # "longtext"

        prompt_req = fake_pending_request(prompt="z")
        for _ in range(10):
            # Both tenants have 0% match rate, so the smaller tenant (r1) is chosen
            assert await prefix_scheduler.choose_replica_for_request(prompt_req) == r1

        chat_req = fake_pending_request(messages=[{"content": "z"}])
        for _ in range(10):
            # Both tenants have 0% match rate, so the smaller tenant (r1) is chosen
            assert await prefix_scheduler.choose_replica_for_request(chat_req) == r1


class TestEvictionBehavior:
    """Tests for prefix tree eviction behavior."""

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "prefix_scheduler",
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
    async def test_eviction_task_creation(self, prefix_scheduler):
        """Test that eviction task is only created after update_replicas."""
        # Before update_replicas
        assert not prefix_scheduler._eviction_loop_running

        # After update_replicas
        r1 = FakeRunningReplica("r1")
        prefix_scheduler.update_replicas([r1])
        assert prefix_scheduler._eviction_loop_running

        # After stop_eviction_loop
        ray.get(prefix_scheduler._tree_actor.stop_eviction_loop.remote())
        await asyncio.sleep(0.1)

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "prefix_scheduler",
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
    async def test_eviction_threshold_behavior(self, prefix_scheduler):
        """Test that eviction reduces tree size below threshold after interval."""
        r1 = FakeRunningReplica("r1")
        prefix_scheduler.update_replicas([r1])

        # Insert text that exceeds eviction_threshold_chars
        ray.get(
            prefix_scheduler._tree_actor.insert.remote(
                "verylongtext", r1.replica_id.to_full_id_str(), time.time()
            )
        )
        ray.get(
            prefix_scheduler._tree_actor.insert.remote(
                "anotherlongtext", r1.replica_id.to_full_id_str(), time.time()
            )
        )

        # Verify initial size exceeds eviction_threshold_chars
        tenant_counts = ray.get(
            prefix_scheduler._tree_actor.getattr.remote("tenant_to_char_count")
        )
        assert tenant_counts[r1.replica_id.to_full_id_str()] > 10

        # Wait for eviction interval
        await asyncio.sleep(1.1)

        # Verify size is reduced below eviction_target_chars
        tenant_counts = ray.get(
            prefix_scheduler._tree_actor.getattr.remote("tenant_to_char_count")
        )
        assert tenant_counts[r1.replica_id.to_full_id_str()] <= 5

        ray.get(prefix_scheduler._tree_actor.stop_eviction_loop.remote())
        await asyncio.sleep(0.1)
