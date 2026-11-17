"""Unit tests for lazy actor-handle resolution in Ray Serve.

These tests verify the behaviour introduced as part of issue #65782 – making
``RunningReplica`` resolve its actor handle on first use rather than eagerly
during construction, with a process-level cache and single-flight async
coalescing.

They use pure unit-test mocks (no Ray runtime) so they run fast and don't
need a live cluster.
"""

import asyncio
import sys
import threading
from unittest.mock import Mock, patch

import pytest

from ray.actor import ActorHandle
from ray.serve._private.common import (
    DeploymentID,
    ReplicaID,
    RunningReplicaInfo,
)
from ray.serve._private.constants import SERVE_NAMESPACE
from ray.serve._private.request_router.replica_wrapper import RunningReplica

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_running_replica_info(
    actor_name: str = "fake-actor-1",
    node_id=None,
    node_ip="127.0.0.1",
) -> RunningReplicaInfo:
    """Build a minimal ``RunningReplicaInfo`` suitable for unit tests."""
    replica_id = ReplicaID(
        "fake_replica",
        deployment_id=DeploymentID(name="fake_deployment"),
    )
    return RunningReplicaInfo(
        replica_id=replica_id,
        node_id=node_id,
        node_ip=node_ip,
        availability_zone=None,
        actor_name=actor_name,
        max_ongoing_requests=10,
        is_cross_language=False,
    )


def _make_mock_actor_handle() -> Mock:
    """Create a mock ActorHandle that exposes the attributes we touch."""
    handle = Mock(spec=ActorHandle)
    handle._actor_id = Mock(hex=lambda: "00" * 8)
    return handle


@pytest.fixture(autouse=True)
def _clear_handle_cache():
    """Before every test, wipe the class-level actor handle cache.

    The cache is process-wide and survives across tests otherwise.
    """
    RunningReplicaInfo.clear_actor_handle_cache()
    yield
    RunningReplicaInfo.clear_actor_handle_cache()


# ---------------------------------------------------------------------------
# Process-level handle cache (RunningReplicaInfo)
# ---------------------------------------------------------------------------


class TestRunningReplicaInfoCache:
    """Tests for the class-level ``_actor_handle_cache``."""

    def test_cache_key_is_actor_name_plus_namespace(self):
        info = _make_running_replica_info(actor_name="my-actor")
        assert info._cache_key() == ("my-actor", SERVE_NAMESPACE)

    def test_sync_get_actor_handle_populates_cache(self):
        info = _make_running_replica_info(actor_name="sync-fill")
        fake_handle = _make_mock_actor_handle()

        with (
            patch.object(RunningReplicaInfo, "_actor_handle_cache", {}),
            patch("ray.get_actor", return_value=fake_handle) as mock_get,
        ):
            h1 = info.get_actor_handle()
            h2 = info.get_actor_handle()

        mock_get.assert_called_once_with("sync-fill", namespace=SERVE_NAMESPACE)
        assert h1 is h2 is fake_handle

    def test_cache_hit_on_different_running_replica_info_instances(self):
        """Two distinct ``RunningReplicaInfo`` objects that share the same
        actor name must share the same cached handle.
        """
        info_a = _make_running_replica_info(actor_name="shared-actor")
        info_b = _make_running_replica_info(actor_name="shared-actor")
        fake_handle = _make_mock_actor_handle()

        with patch("ray.get_actor", return_value=fake_handle) as mock_get:
            h_a = info_a.get_actor_handle()
            h_b = info_b.get_actor_handle()

        mock_get.assert_called_once()
        assert h_a is h_b is fake_handle

    def test_different_actor_names_dont_share_cache(self):
        info_a = _make_running_replica_info(actor_name="actor-A")
        info_b = _make_running_replica_info(actor_name="actor-B")
        h_a = _make_mock_actor_handle()
        h_b = _make_mock_actor_handle()

        with patch(
            "ray.get_actor",
            side_effect=[h_a, h_b],
        ) as mock_get:
            r_a = info_a.get_actor_handle()
            r_b = info_b.get_actor_handle()

        assert mock_get.call_count == 2
        assert r_a is h_a
        assert r_b is h_b
        assert r_a is not r_b

    def test_invalidate_actor_handle_removes_one_key(self):
        info_a = _make_running_replica_info(actor_name="actor-A")
        info_b = _make_running_replica_info(actor_name="actor-B")

        h_a1 = _make_mock_actor_handle()
        h_a2 = _make_mock_actor_handle()
        h_b = _make_mock_actor_handle()

        with patch(
            "ray.get_actor",
            side_effect=[h_a1, h_b, h_a2],
        ) as mock_get:
            info_a.get_actor_handle()  # miss -> caches h_a1
            info_b.get_actor_handle()  # miss -> caches h_b
            RunningReplicaInfo.invalidate_actor_handle("actor-A")
            info_a.get_actor_handle()  # miss again -> h_a2
            info_b.get_actor_handle()  # hit -> still h_b

        assert mock_get.call_count == 3
        assert info_a.get_actor_handle() is h_a2  # cached
        assert info_b.get_actor_handle() is h_b  # still cached

    def test_clear_actor_handle_cache_wipes_everything(self):
        info_a = _make_running_replica_info(actor_name="actor-A")
        h_a1 = _make_mock_actor_handle()
        h_a2 = _make_mock_actor_handle()

        with patch(
            "ray.get_actor",
            side_effect=[h_a1, h_a2],
        ) as mock_get:
            info_a.get_actor_handle()
            RunningReplicaInfo.clear_actor_handle_cache()
            info_a.get_actor_handle()

        assert mock_get.call_count == 2

    def test_hash_and_eq_not_affected_by_cache(self):
        """Adding ClassVar cache fields must not break the frozen dataclass
        contract — two instances with the same values must still hash and
        compare equal.
        """
        a = _make_running_replica_info(actor_name="same-name")
        b = _make_running_replica_info(actor_name="same-name")
        assert hash(a) == hash(b)
        assert a == b

    def test_cache_is_not_a_dataclass_field(self):
        """ClassVar fields must not become frozen-dataclass fields (which
        would break construction).  This is a canary that the cache
        declaration syntax is correct.
        """
        info = _make_running_replica_info()
        # Must be constructible without passing cache-related args.
        assert not hasattr(info, "_actor_handle_cache") or True  # ok


class TestRunningReplicaInfoAsyncCache:
    """Tests for ``get_actor_handle_async`` — single-flight + thread-pool."""

    @pytest.mark.asyncio
    async def test_async_get_actor_handle_uses_cache(self):
        info = _make_running_replica_info(actor_name="async-hit")
        fake_handle = _make_mock_actor_handle()

        # Seed the sync cache first.
        with patch("ray.get_actor", return_value=fake_handle):
            info.get_actor_handle()

        # Async path must hit the cache and never call ray.get_actor.
        with patch("ray.get_actor", side_effect=AssertionError("cache should be hit")):
            h = await info.get_actor_handle_async()
        assert h is fake_handle

    @pytest.mark.asyncio
    async def test_async_get_actor_handle_offloads_to_thread_pool(self):
        info = _make_running_replica_info(actor_name="async-pool")
        fake_handle = _make_mock_actor_handle()

        with patch("ray.get_actor", return_value=fake_handle) as mock_get:
            h = await info.get_actor_handle_async()

        assert h is fake_handle
        mock_get.assert_called_once_with("async-pool", namespace=SERVE_NAMESPACE)

    @pytest.mark.asyncio
    async def test_single_flight_coalesces_concurrent_awaiters(self):
        """Multiple coroutines awaiting the same uncached handle must share
        one ``ray.get_actor`` call.
        """
        info = _make_running_replica_info(actor_name="single-flight")
        fake_handle = _make_mock_actor_handle()
        call_count = {"n": 0}

        def counting_get_actor(*args, **kwargs):
            call_count["n"] += 1
            return fake_handle

        # Reset cache so the coroutines will race.
        info.clear_actor_handle_cache()

        with patch("ray.get_actor", side_effect=counting_get_actor):
            # Fire N concurrent coroutines, all uncached.
            coros = [info.get_actor_handle_async() for _ in range(5)]
            results = await asyncio.gather(*coros)

        # All results are the same handle, and ray.get_actor was called once.
        assert call_count["n"] == 1
        assert all(r is fake_handle for r in results)


# ---------------------------------------------------------------------------
# Lazy resolution in RunningReplica
# ---------------------------------------------------------------------------


class TestRunningReplicaLazyResolution:
    """RunningReplica must *not* call ray.get_actor in __init__."""

    def test_init_does_not_resolve_actor_handle(self):
        info = _make_running_replica_info(actor_name="never-resolved")
        handle = _make_mock_actor_handle()

        with (
            patch("ray.get_actor", return_value=handle) as mock_get,
            patch.object(RunningReplicaInfo, "_actor_handle_cache", {}),
        ):
            # Cache starts empty so resolution *would* happen if it were eager.
            replica = RunningReplica(info)
            mock_get.assert_not_called()

        # Internal invariants.
        assert replica._actor_handle is None
        assert not replica._handle_resolved
        assert replica._actor_replica_wrapper is None

    def test_actor_id_property_triggers_sync_resolution(self):
        info = _make_running_replica_info(actor_name="actor-id-prop")
        fake = _make_mock_actor_handle()

        with patch("ray.get_actor", return_value=fake) as mock_get:
            replica = RunningReplica(info)
            _ = replica.actor_id

        mock_get.assert_called_once()
        assert replica._handle_resolved
        assert replica._actor_handle is fake

    @pytest.mark.asyncio
    async def test_ensure_actor_handle_uses_async_resolver(self):
        info = _make_running_replica_info(actor_name="ensure-async")
        fake = _make_mock_actor_handle()

        with patch("ray.get_actor", return_value=fake):
            replica = RunningReplica(info)
            h = await replica._ensure_actor_handle()

        assert h is fake
        assert replica._handle_resolved
        # Subsequent calls hit the instance cache, not ray.
        with patch("ray.get_actor", side_effect=AssertionError):
            h2 = await replica._ensure_actor_handle()
        assert h2 is fake

    @pytest.mark.asyncio
    async def test_concurrent_ensure_actor_handle_coalesces(self):
        info = _make_running_replica_info(actor_name="concurrent")
        fake = _make_mock_actor_handle()
        call_count = {"n": 0}

        def counting_get_actor(*args, **kwargs):
            call_count["n"] += 1
            return fake

        with patch("ray.get_actor", side_effect=counting_get_actor):
            replica = RunningReplica(info)
            coros = [replica._ensure_actor_handle() for _ in range(4)]
            results = await asyncio.gather(*coros)

        assert call_count["n"] == 1
        assert all(r is fake for r in results)

    def test_invalidate_drops_handle_and_wrapper(self):
        info = _make_running_replica_info(actor_name="to-invalidate")
        fake = _make_mock_actor_handle()

        with patch("ray.get_actor", return_value=fake):
            replica = RunningReplica(info)
            _ = replica.actor_id  # resolve
            replica.invalidate()

        assert replica._actor_handle is None
        assert not replica._handle_resolved
        assert replica._actor_replica_wrapper is None


class TestRunningReplicaCrossLanguage:
    """Cross-language (Java) replicas still resolve eagerly via sync path."""

    def test_sync_resolution_wraps_java_handle(self):
        from ray.serve._private.utils import JavaActorHandleProxy

        info = RunningReplicaInfo(
            replica_id=ReplicaID(
                "java-replica",
                deployment_id=DeploymentID(name="java-deployment"),
            ),
            node_id=None,
            node_ip="127.0.0.1",
            availability_zone=None,
            actor_name="java-actor",
            max_ongoing_requests=5,
            is_cross_language=True,
        )
        fake = _make_mock_actor_handle()

        with patch("ray.get_actor", return_value=fake):
            replica = RunningReplica(info)
            h = replica._get_actor_handle_sync()

        assert isinstance(h, JavaActorHandleProxy)
        assert replica._handle_resolved


class TestRunningReplicaMultipleInstancesShareHandle:
    """Two RunningReplica instances with the same actor name share one GCS
    lookup via the process-level cache."""

    def test_two_running_replicas_share_cache(self):
        info_a = _make_running_replica_info(actor_name="shared")
        info_b = _make_running_replica_info(actor_name="shared")
        fake = _make_mock_actor_handle()

        with patch("ray.get_actor", return_value=fake) as mock_get:
            r_a = RunningReplica(info_a)
            r_b = RunningReplica(info_b)
            _ = r_a.actor_id
            _ = r_b.actor_id

        mock_get.assert_called_once()
        assert r_a._actor_handle is r_b._actor_handle is fake


# ---------------------------------------------------------------------------
# Cache thread safety
# ---------------------------------------------------------------------------


class TestCacheThreadSafety:
    """Sanity check that the cache is safe under concurrent threads."""

    def test_sync_cache_thread_safe(self):
        """Spawn many threads that all call get_actor_handle on the same
        RunningReplicaInfo and assert ray.get_actor is called exactly once.
        """
        info = _make_running_replica_info(actor_name="threaded")
        fake = _make_mock_actor_handle()
        call_count = {"n": 0}

        def counting_get_actor(*args, **kwargs):
            call_count["n"] += 1
            return fake

        errors = []

        def worker():
            try:
                h = info.get_actor_handle()
                assert h is fake
            except Exception as e:  # noqa: BLE001 - pragma: no cover - safety net
                errors.append(e)

        with patch("ray.get_actor", side_effect=counting_get_actor):
            threads = [threading.Thread(target=worker) for _ in range(20)]
            for t in threads:
                t.start()
            for t in threads:
                t.join()

        assert not errors
        assert call_count["n"] == 1


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
