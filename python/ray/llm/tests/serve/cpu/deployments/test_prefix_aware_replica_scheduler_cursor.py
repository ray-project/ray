import asyncio
import pytest

from ray._common.utils import get_or_create_event_loop
from ray.serve._private.replica_scheduler import PendingRequest
from ray.serve._private.replica_scheduler.prefix_aware_scheduler import (
    PrefixAwareReplicaScheduler,
)
from ray.llm.tests.serve.cpu.deployments.test_pow_2_replica_scheduler import (
    FakeRunningReplica,
    TIMER,
    SCHEDULER_NODE_ID,
)

# Import all tests from pow2 scheduler to reuse them


@pytest.fixture
def prefix_aware_scheduler(request) -> PrefixAwareReplicaScheduler:
    if not hasattr(request, "param"):
        request.param = {}

    # In order to prevent issues like https://github.com/ray-project/ray/issues/40631,
    # construct the scheduler on a different loop to mimic the deployment handle path.
    async def construct_scheduler(loop: asyncio.AbstractEventLoop):
        scheduler = PrefixAwareReplicaScheduler(
            deployment_id=request.param.get("deployment_id", "TEST_DEPLOYMENT"),
            handle_source=request.param.get(
                "handle_source", DeploymentHandleSource.REPLICA
            ),
            prefer_local_node_routing=request.param.get("prefer_local_node", False),
            prefer_local_az_routing=request.param.get("prefer_local_az", False),
            self_node_id=SCHEDULER_NODE_ID,
            self_actor_id="fake-actor-id",
            self_actor_handle=None,
            self_availability_zone=request.param.get("az", None),
            use_replica_queue_len_cache=request.param.get(
                "use_replica_queue_len_cache", False
            ),
            get_curr_time_s=TIMER.time,
            imbalanced_threshold=request.param.get("imbalanced_threshold", 10),
            match_rate_threshold=request.param.get("match_rate_threshold", 0.1),
            do_eviction=request.param.get("do_eviction", False),
            eviction_threshold_chars=request.param.get(
                "eviction_threshold_chars", 400_000
            ),
            eviction_target_chars=request.param.get("eviction_target_chars", 360_000),
            eviction_interval_secs=request.param.get("eviction_interval_secs", 10),
        )
        scheduler.backoff_sequence_s = request.param.get(
            "backoff_sequence_s",
            [0, 0.001, 0.001, 0.001, 0.001, 0.001, 0.001],
        )
        return scheduler

    s = asyncio.new_event_loop().run_until_complete(
        construct_scheduler(get_or_create_event_loop())
    )

    # Reset mock timer to avoid state leakage.
    TIMER.reset()

    yield s

    # Always verify that all scheduling tasks exit once all queries are satisfied.
    assert s.curr_num_scheduling_tasks == 0
    assert s.num_pending_requests == 0


class TestPrefixAwareFallbackToPow2:
    """Tests for when prefix aware scheduler falls back to pow2 behavior."""

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "prefix_aware_scheduler",
        [
            {
                "imbalanced_threshold": 2,  # Low threshold for testing
                "match_rate_threshold": 0.1,
            }
        ],
        indirect=True,
    )
    async def test_fallback_when_system_imbalanced(self, prefix_aware_scheduler):
        """Test that when system is imbalanced, we fall back to pow2 behavior."""
        s = prefix_aware_scheduler
        loop = get_or_create_event_loop()

        # Create two replicas with very different queue lengths
        r1 = FakeRunningReplica("r1")
        r2 = FakeRunningReplica("r2")
        r1.set_queue_len_response(0)  # Very low load
        r2.set_queue_len_response(11)  # Very high load
        s.update_replicas([r1, r2])

        # Insert text into both replicas' trees
        await s._tree_actor.insert.remote("Hello world", "r1", 1)
        await s._tree_actor.insert.remote("Hello world", "r2", 2)

        # Create a request that would match both replicas
        class CompletionRequest:
            def __init__(self, prompt):
                self.prompt = prompt

        request = PendingRequest(
            args=[CompletionRequest("Hello world")],
            kwargs={},
            metadata=RequestMetadata(
                request_id=generate_request_id(),
                internal_request_id=generate_request_id(),
            ),
        )

        # Send multiple requests to ensure consistent behavior
        async def choose_replicas():
            tasks = []
            for _ in range(10):
                tasks.append(loop.create_task(s.choose_replica_for_request(request)))
            return await asyncio.gather(*tasks)

        # All requests should go to r1 since it has lower queue length
        # (system is imbalanced, so we fall back to pow2 behavior)
        assert all(replica == r1 for replica in await choose_replicas())

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "prefix_aware_scheduler",
        [
            {
                "imbalanced_threshold": 2,
                "match_rate_threshold": 0.1,
            }
        ],
        indirect=True,
    )
    async def test_fallback_when_no_text_extracted(self, prefix_aware_scheduler):
        """Test that when no text can be extracted from request, we fall back to pow2 behavior."""
        s = prefix_aware_scheduler
        loop = get_or_create_event_loop()

        # Create two replicas with different queue lengths
        r1 = FakeRunningReplica("r1")
        r2 = FakeRunningReplica("r2")
        r1.set_queue_len_response(0)
        r2.set_queue_len_response(1)
        s.update_replicas([r1, r2])

        # Insert text into both replicas' trees
        await s._tree_actor.insert.remote("Hello world", "r1", 1)
        await s._tree_actor.insert.remote("Hello world", "r2", 2)

        # Create a request with no text to extract
        request = PendingRequest(
            args=[],  # Empty args means no text to extract
            kwargs={},
            metadata=RequestMetadata(
                request_id=generate_request_id(),
                internal_request_id=generate_request_id(),
            ),
        )

        # Send multiple requests to ensure consistent behavior
        async def choose_replicas():
            tasks = []
            for _ in range(10):
                tasks.append(loop.create_task(s.choose_replica_for_request(request)))
            return await asyncio.gather(*tasks)

        # All requests should go to r1 since it has lower queue length
        # (no text to extract, so we fall back to pow2 behavior)
        assert all(replica == r1 for replica in await choose_replicas())


class TestPrefixAwareBehavior:
    """Tests for when prefix aware scheduler uses its own logic."""

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "prefix_aware_scheduler",
        [
            {
                "imbalanced_threshold": 2,
                "match_rate_threshold": 0.1,
            }
        ],
        indirect=True,
    )
    async def test_choose_highest_match_rate(self, prefix_aware_scheduler):
        """Test that when system is balanced, we choose replica with highest match rate."""
        s = prefix_aware_scheduler
        loop = get_or_create_event_loop()

        # Create two replicas with similar queue lengths
        r1 = FakeRunningReplica("r1")
        r2 = FakeRunningReplica("r2")
        r1.set_queue_len_response(0)
        r2.set_queue_len_response(1)  # Only 1 difference, system is balanced
        s.update_replicas([r1, r2])

        # Insert different text into each replica's tree
        await s._tree_actor.insert.remote("Hello", "r1", 1)
        await s._tree_actor.insert.remote("Hello world", "r2", 2)

        # Create a request that matches r2 better
        class CompletionRequest:
            def __init__(self, prompt):
                self.prompt = prompt

        request = PendingRequest(
            args=[CompletionRequest("Hello world")],
            kwargs={},
            metadata=RequestMetadata(
                request_id=generate_request_id(),
                internal_request_id=generate_request_id(),
            ),
        )

        # Send multiple requests to ensure consistent behavior
        async def choose_replicas():
            tasks = []
            for _ in range(10):
                tasks.append(loop.create_task(s.choose_replica_for_request(request)))
            return await asyncio.gather(*tasks)

        # All requests should go to r2 since it has higher match rate
        # (system is balanced, so we use prefix aware behavior)
        assert all(replica == r2 for replica in await choose_replicas())

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "prefix_aware_scheduler",
        [
            {
                "imbalanced_threshold": 2,
                "match_rate_threshold": 0.1,
            }
        ],
        indirect=True,
    )
    async def test_choose_smallest_tree_when_match_rate_low(
        self, prefix_aware_scheduler
    ):
        """Test that when match rate is below threshold, we choose replica with smallest tree."""
        s = prefix_aware_scheduler
        loop = get_or_create_event_loop()

        # Create two replicas with similar queue lengths
        r1 = FakeRunningReplica("r1")
        r2 = FakeRunningReplica("r2")
        r1.set_queue_len_response(0)
        r2.set_queue_len_response(1)  # Only 1 difference, system is balanced
        s.update_replicas([r1, r2])

        # Insert different text into each replica's tree
        await s._tree_actor.insert.remote("Hello", "r1", 1)
        await s._tree_actor.insert.remote("Hello world", "r2", 2)

        # Create a request that matches poorly with both
        class CompletionRequest:
            def __init__(self, prompt):
                self.prompt = prompt

        request = PendingRequest(
            args=[CompletionRequest("Goodbye")],
            kwargs={},
            metadata=RequestMetadata(
                request_id=generate_request_id(),
                internal_request_id=generate_request_id(),
            ),
        )

        # Send multiple requests to ensure consistent behavior
        async def choose_replicas():
            tasks = []
            for _ in range(10):
                tasks.append(loop.create_task(s.choose_replica_for_request(request)))
            return await asyncio.gather(*tasks)

        # All requests should go to r1 since it has smaller tree
        # (match rate is low, so we choose smallest tree)
        assert all(replica == r1 for replica in await choose_replicas())

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "prefix_aware_scheduler",
        [
            {
                "imbalanced_threshold": 2,
                "match_rate_threshold": 0.1,
            }
        ],
        indirect=True,
    )
    async def test_random_choice_when_same_tree_size(self, prefix_aware_scheduler):
        """Test that when multiple replicas have same tree size, we randomly choose between them."""
        s = prefix_aware_scheduler
        loop = get_or_create_event_loop()

        # Create three replicas with similar queue lengths
        r1 = FakeRunningReplica("r1")
        r2 = FakeRunningReplica("r2")
        r3 = FakeRunningReplica("r3")
        r1.set_queue_len_response(0)
        r2.set_queue_len_response(1)
        r3.set_queue_len_response(1)
        s.update_replicas([r1, r2, r3])

        # Insert same text into each replica's tree
        await s._tree_actor.insert.remote("Hello", "r1", 1)
        await s._tree_actor.insert.remote("Hello", "r2", 2)
        await s._tree_actor.insert.remote("Hello", "r3", 3)

        # Create a request that matches poorly with all
        class CompletionRequest:
            def __init__(self, prompt):
                self.prompt = prompt

        request = PendingRequest(
            args=[CompletionRequest("Goodbye")],
            kwargs={},
            metadata=RequestMetadata(
                request_id=generate_request_id(),
                internal_request_id=generate_request_id(),
            ),
        )

        # Send many requests to ensure we see random distribution
        async def choose_replicas():
            tasks = []
            for _ in range(30):  # More requests to ensure good distribution
                tasks.append(loop.create_task(s.choose_replica_for_request(request)))
            return await asyncio.gather(*tasks)

        # All requests should go to one of the replicas with same tree size
        # (r2 or r3, since r1 has different queue length)
        replicas = await choose_replicas()
        assert all(replica in {r2, r3} for replica in replicas)
        # Verify we got a mix of both replicas
        assert len(set(replicas)) == 2

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "prefix_aware_scheduler",
        [
            {
                "imbalanced_threshold": 2,
                "match_rate_threshold": 0.1,
            }
        ],
        indirect=True,
    )
    async def test_chat_completion_request(self, prefix_aware_scheduler):
        """Test that we handle chat completion requests correctly."""
        s = prefix_aware_scheduler
        loop = get_or_create_event_loop()

        # Create two replicas with similar queue lengths
        r1 = FakeRunningReplica("r1")
        r2 = FakeRunningReplica("r2")
        r1.set_queue_len_response(0)
        r2.set_queue_len_response(1)
        s.update_replicas([r1, r2])

        # Insert different text into each replica's tree
        await s._tree_actor.insert.remote("Hello", "r1", 1)
        await s._tree_actor.insert.remote("Hello world", "r2", 2)

        # Create a chat completion request
        class ChatCompletionRequest:
            def __init__(self, messages):
                self.messages = messages

        request = PendingRequest(
            args=[ChatCompletionRequest([{"role": "user", "content": "Hello world"}])],
            kwargs={},
            metadata=RequestMetadata(
                request_id=generate_request_id(),
                internal_request_id=generate_request_id(),
            ),
        )

        # Send multiple requests to ensure consistent behavior
        async def choose_replicas():
            tasks = []
            for _ in range(10):
                tasks.append(loop.create_task(s.choose_replica_for_request(request)))
            return await asyncio.gather(*tasks)

        # All requests should go to r2 since it has higher match rate
        assert all(replica == r2 for replica in await choose_replicas())
