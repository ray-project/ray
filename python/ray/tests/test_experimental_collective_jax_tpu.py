import sys
from unittest.mock import MagicMock, patch

import pytest

import ray
import ray.experimental.collective
from ray.util.collective.types import Backend


@ray.remote
class FakeTPUActor:
    def __init__(self, device_ids):
        self.device_ids = device_ids

    def get_device_ids(self):
        return self.device_ids


def test_jax_tpu_cache_lifecycle(ray_start_regular_shared):
    """Tests the lifecycle of the TPU device ID cache for JAX_TPU backend."""
    world_size = 2
    actors = [FakeTPUActor.remote([i]) for i in range(world_size)]
    actor_ids = [actor._actor_id.hex() for actor in actors]

    # Mock _populate_tpu_cache to simulate TPU device ID population without real JAX.
    with patch(
        "ray.experimental.collective.collective._populate_tpu_cache"
    ) as mock_populate:

        def side_effect(workers):
            from ray.experimental.collective.collective import (
                _actor_to_device_ids_cache,
                _cache_lock,
            )

            with _cache_lock:
                for i, worker in enumerate(workers):
                    _actor_to_device_ids_cache[worker._actor_id.hex()] = [i]

        mock_populate.side_effect = side_effect

        group_name = "test_jax_tpu"
        group = ray.experimental.collective.create_collective_group(
            actors, backend="JAX_TPU", name=group_name
        )

        assert group is not None
        assert group.name == group_name
        assert group.backend == Backend.JAX_TPU

        # Verify cache is populated.
        from ray.experimental.collective.collective import _actor_to_device_ids_cache

        for i, actor_id in enumerate(actor_ids):
            assert _actor_to_device_ids_cache[actor_id] == [i]

        # Verify it's in the manager.
        manager = ray.experimental.collective.collective.RemoteCommunicatorManager.get()
        assert group_name in manager._remote_communicators

        # Destroy the group.
        ray.experimental.collective.destroy_collective_group(group)

        # Verify cache is cleared.
        for actor_id in actor_ids:
            assert actor_id not in _actor_to_device_ids_cache

        # Verify it's removed from the manager.
        assert group_name not in manager._remote_communicators


def test_actor_device_id_getters_success():
    """Tests the successful retrieval of TPU device IDs from the actor cache."""
    actor_id = "test_actor_id"
    mock_actor = MagicMock()
    mock_actor._actor_id.hex.return_value = actor_id

    device_ids = [100, 200]

    # Populate the cache
    from ray.experimental.collective.collective import (
        _actor_to_device_ids_cache,
        _cache_lock,
    )

    with _cache_lock:
        _actor_to_device_ids_cache[actor_id] = device_ids

    try:
        # Test get_all_local_device_ids_from_actor
        ids = ray.experimental.collective.get_all_local_device_ids_from_actor(
            mock_actor
        )
        assert ids == device_ids

        # Test get_global_device_id_from_actor
        first_id = ray.experimental.collective.get_global_device_id_from_actor(
            mock_actor
        )
        assert first_id == device_ids[0]
    finally:
        # Clean up cache
        with _cache_lock:
            if actor_id in _actor_to_device_ids_cache:
                del _actor_to_device_ids_cache[actor_id]


def test_actor_device_id_getters_missing_actor():
    """Tests that a KeyError is raised if the actor is missing from the cache."""
    mock_actor = MagicMock()
    mock_actor._actor_id.hex.return_value = "missing_actor"

    with pytest.raises(KeyError):
        ray.experimental.collective.get_all_local_device_ids_from_actor(mock_actor)

    with pytest.raises(KeyError):
        ray.experimental.collective.get_global_device_id_from_actor(mock_actor)


def test_get_global_device_id_from_actor_empty_list():
    """Tests that an IndexError is raised if the cache contains an empty list for the actor."""
    actor_id = "empty_list_actor"
    mock_actor = MagicMock()
    mock_actor._actor_id.hex.return_value = actor_id

    from ray.experimental.collective.collective import (
        _actor_to_device_ids_cache,
        _cache_lock,
    )

    with _cache_lock:
        _actor_to_device_ids_cache[actor_id] = []

    try:
        with pytest.raises(IndexError):
            ray.experimental.collective.get_global_device_id_from_actor(mock_actor)
    finally:
        # Clean up cache
        with _cache_lock:
            if actor_id in _actor_to_device_ids_cache:
                del _actor_to_device_ids_cache[actor_id]


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
