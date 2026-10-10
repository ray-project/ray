import asyncio
import time

import pytest

import ray
from ray._common.test_utils import SignalActor, async_wait_for_condition
from ray._common.utils import get_or_create_event_loop
from ray.llm._internal.serve.routing_policies.prefix_aware.prefix_aware_router import (
    PrefixCacheAffinityRouter,
)
from ray.llm._internal.serve.routing_policies.prefix_aware.prefix_tree import (
    PrefixTree,
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
        imbalanced_threshold=params.get("imbalanced_threshold", float("inf")),
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


@ray.remote
class SlowPrefixTreeActor(PrefixTree):
    """Prefix tree actor whose lookups and inserts each take delay_s."""

    def __init__(self, delay_s: float):
        super().__init__()
        self._delay_s = delay_s

    def prefix_match(self, *args, **kwargs):
        time.sleep(self._delay_s)
        return super().prefix_match(*args, **kwargs)

    def insert(self, *args, **kwargs):
        time.sleep(self._delay_s)
        return super().insert(*args, **kwargs)

    def get_tenant_to_char_count(self):
        return self.tenant_to_char_count


@ray.remote
class BlockingPrefixTreeActor(PrefixTree):
    """Prefix tree actor whose prefix matches wait until a signal is sent."""

    def __init__(self, signal):
        super().__init__()
        self._signal = signal

    def prefix_match(self, *args, **kwargs):
        ray.get(self._signal.wait.remote())
        return super().prefix_match(*args, **kwargs)


@ray.remote
class LookupCountingPrefixTreeActor(PrefixTree):
    """Prefix tree actor that counts the router's lookups."""

    def __init__(self):
        super().__init__()
        self._num_lookups = 0

    def prefix_match_or_smallest_tenants(self, *args, **kwargs):
        self._num_lookups += 1
        return super().prefix_match_or_smallest_tenants(*args, **kwargs)

    def get_num_lookups(self) -> int:
        return self._num_lookups


def make_router(
    tree_actor, use_replica_queue_len_cache: bool = False, **state_kwargs
) -> PrefixCacheAffinityRouter:
    router = PrefixCacheAffinityRouter(
        deployment_id=DeploymentID(name="TEST_DEPLOYMENT"),
        handle_source=DeploymentHandleSource.REPLICA,
        use_replica_queue_len_cache=use_replica_queue_len_cache,
        get_curr_time_s=TIMER.time,
    )
    router.initialize_state(tree_actor=tree_actor, **state_kwargs)
    return router


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

    @pytest.mark.parametrize(
        "prefix_request_router", [{"imbalanced_threshold": 2}], indirect=True
    )
    @pytest.mark.asyncio
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


class TestNonBlockingTreeCalls:
    """Tests that the router doesn't block its event loop on the tree actor."""

    @pytest.mark.asyncio
    async def test_tree_lookup_does_not_block_event_loop(self):
        tree_actor = SlowPrefixTreeActor.remote(delay_s=0.5)
        router = make_router(tree_actor)
        r1 = FakeRunningReplica("r1")
        r1.set_queue_len_response(0)
        router.update_replicas([r1])

        routing = asyncio.ensure_future(
            router._choose_replica_for_request(fake_pending_request(prompt="hello"))
        )
        # Keep ticking the loop while the lookup is in flight. Blocking on the
        # actor would stall the loop for the whole lookup.
        max_gap_s = 0.0
        last = time.monotonic()
        while not routing.done():
            await asyncio.sleep(0.01)
            now = time.monotonic()
            max_gap_s = max(max_gap_s, now - last)
            last = now
        assert await routing == r1
        assert max_gap_s < 0.25
        ray.kill(tree_actor)

    @pytest.mark.asyncio
    async def test_on_request_routed_does_not_wait_for_insert(self):
        tree_actor = SlowPrefixTreeActor.remote(delay_s=0.5)
        router = make_router(tree_actor)
        r1 = FakeRunningReplica("r1")
        router.update_replicas([r1])

        start = time.monotonic()
        router.on_request_routed(
            fake_pending_request(prompt="hello"), r1.replica_id, result=None
        )
        assert time.monotonic() - start < 0.25
        # The insert still lands before later calls from this process.
        tenant_to_char_count = ray.get(tree_actor.get_tenant_to_char_count.remote())
        assert tenant_to_char_count[r1.replica_id.to_full_id_str()] == 5
        ray.kill(tree_actor)

    @pytest.mark.asyncio
    async def test_replica_removed_during_tree_lookup_is_not_chosen(self):
        signal = SignalActor.remote()
        tree_actor = BlockingPrefixTreeActor.remote(signal)
        router = make_router(tree_actor)
        r1 = FakeRunningReplica("r1")
        r1.set_queue_len_response(0)
        r2 = FakeRunningReplica("r2")
        r2.set_queue_len_response(0)
        router.update_replicas([r1, r2])
        ray.get(tree_actor.insert.remote("hello", r2.replica_id.to_full_id_str(), 0.0))

        routing = asyncio.ensure_future(
            router._choose_replica_for_request(
                fake_pending_request(prompt="hello world")
            )
        )

        # Hold the lookup, which will match r2, until r2 is removed.
        async def lookup_waiting():
            return await signal.cur_num_waiters.remote() == 1

        await async_wait_for_condition(lookup_waiting)
        router.update_replicas([r1])
        await signal.send.remote()
        assert await asyncio.wait_for(routing, timeout=10) == r1
        ray.kill(tree_actor)
        ray.kill(signal)


class TestLoadCheck:
    """Tests that the load check doesn't do work whose result isn't used."""

    @pytest.mark.asyncio
    async def test_default_threshold_skips_queue_length_probes(self):
        """Load can't be imbalanced at the default threshold, so prefix matching
        doesn't probe queue lengths."""
        tree_actor = LookupCountingPrefixTreeActor.remote()
        router = make_router(tree_actor)
        r1 = FakeRunningReplica("r1")
        r2 = FakeRunningReplica("r2")
        router.update_replicas([r1, r2])
        probed = []

        async def probe_queue_lens(replicas, backoff_index):
            probed.extend(replicas)
            return [(r, 0) for r in replicas]

        router._probe_queue_lens = probe_queue_lens

        chosen = await router._prefix_match_best_replicas(
            fake_pending_request(prompt="hello"), [r1, r2]
        )
        assert probed == []
        assert ray.get(tree_actor.get_num_lookups.remote()) == 1
        # Neither replica has cached text, so both are the smallest tenants.
        assert set(chosen[0]) == {r1, r2}
        ray.kill(tree_actor)

    @pytest.mark.parametrize("r2_queue_len, num_lookups", [(100, 0), (5, 1)])
    @pytest.mark.asyncio
    async def test_cached_imbalance_skips_tree_lookup(self, r2_queue_len, num_lookups):
        """If cached queue lengths already differ by more than the threshold, the
        router doesn't send a tree lookup whose result it would discard."""
        tree_actor = LookupCountingPrefixTreeActor.remote()
        router = make_router(
            tree_actor, use_replica_queue_len_cache=True, imbalanced_threshold=10
        )
        r1 = FakeRunningReplica("r1", max_ongoing_requests=1000)
        r2 = FakeRunningReplica("r2", max_ongoing_requests=1000)
        router.update_replicas([r1, r2])
        router._replica_queue_len_cache.update(r1.replica_id, 0)
        router._replica_queue_len_cache.update(r2.replica_id, r2_queue_len)

        chosen = await router._prefix_match_best_replicas(
            fake_pending_request(prompt="hello"), [r1, r2]
        )
        assert ray.get(tree_actor.get_num_lookups.remote()) == num_lookups
        # Imbalanced load skips prefix matching; balanced load picks the smallest
        # tenants.
        assert bool(chosen[0]) == bool(num_lookups)
        ray.kill(tree_actor)


class TestEvictionBehavior:
    """Tests for prefix tree eviction behavior."""

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
    @pytest.mark.asyncio
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
    @pytest.mark.asyncio
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

        # Create separate tree actors for each deployment
        prefill_tree = PrefixTreeActor.options(name="PrefillTree").remote()
        decode_tree = PrefixTreeActor.options(name="DecodeTree").remote()

        # Create two routers for different deployments (e.g., Prefill and Decode in PD setup)
        async def construct_router(deployment_name: str, tree_actor):
            router = PrefixCacheAffinityRouter(
                deployment_id=DeploymentID(name=deployment_name),
                handle_source=DeploymentHandleSource.REPLICA,
                use_replica_queue_len_cache=False,
                get_curr_time_s=TIMER.time,
            )
            router.initialize_state(tree_actor=tree_actor)
            return router

        prefill_router = await construct_router("Prefill:deepseek", prefill_tree)
        decode_router = await construct_router("Decode:deepseek", decode_tree)

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
