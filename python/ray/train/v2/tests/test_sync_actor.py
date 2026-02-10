import pytest

import ray
from ray.train.v2._internal.constants import DEFAULT_COLLECTIVE_TIMEOUT_S
from ray.train.v2._internal.exceptions import BroadcastCollectiveTimeoutError
from ray.train.v2._internal.execution.checkpoint.sync_actor import SynchronizationActor


@pytest.fixture(autouse=True, scope="module")
def ray_start_4_cpus():
    ray.init(num_cpus=4)
    yield
    ray.shutdown()


@pytest.mark.parametrize("world_size", [1, 10, 1000])
def test_broadcast_from_rank_0(world_size):
    """Check that rank 0 can broadcast data to all other workers.
    Every worker sends data with a string "data-{rank}" that is unique
    to the worker. Everyone should receive the data from rank 0, which is "data-0".
    Also assert that the actor state is reset after the broadcast function returns.
    """
    sync_actor = SynchronizationActor.remote()
    # Test broadcast_from_rank_zero with a world size of 10
    remote_tasks = []
    for rank in range(world_size):
        remote_tasks.append(
            sync_actor.broadcast_from_rank_zero.remote(
                world_rank=rank,
                world_size=world_size,
                data=f"data-{rank}",
                caller_method_name="broadcast_from_rank_zero",
            )
        )
    # Ensure that all workers have the same consensus data same as rank 0
    assert all([each == "data-0" for each in ray.get(remote_tasks)])
    # Ensure al the states are cleared after the broadcast function returns
    assert ray.get(sync_actor.get_counter.remote()) == 0
    assert ray.get(sync_actor.get_world_size.remote()) == 0
    assert ray.get(sync_actor.get_reduced_data.remote()) is None


def test_hang_with_timeout():
    """The test checks if the workers are blocked and hang when the world size
    is greater than the number of workers. The workers should block and hang
    until the barrier is lifted.
    """
    sync_actor = SynchronizationActor.remote(timeout_s=1, warn_interval_s=0.2)
    # Test broadcast_from_rank_zero with a world size of 10. But
    # only 9 workers data, the workers should block and hang
    remote_tasks = []
    for rank in range(9):
        remote_tasks.append(
            sync_actor.broadcast_from_rank_zero.remote(
                world_rank=rank,
                world_size=10,
                data=f"data-{rank}",
                caller_method_name="broadcast_from_rank_zero",
            )
        )
    # Ensure that the workers are blocked and raise BroadcastCollectiveTimeoutError
    # after 1 second
    with pytest.raises(BroadcastCollectiveTimeoutError) as excinfo:
        ray.get(remote_tasks)
    assert "The following ranks have not joined the collective operation: [9]" in str(
        excinfo.value
    )


def test_hang_without_timeout():
    """Test the default behavior of running with no collective timeout."""
    assert DEFAULT_COLLECTIVE_TIMEOUT_S == -1

    sync_actor = SynchronizationActor.remote()
    remote_tasks = []
    for rank in range(9):
        remote_tasks.append(
            sync_actor.broadcast_from_rank_zero.remote(
                world_rank=rank,
                world_size=10,
                data=f"data-{rank}",
                caller_method_name="broadcast_from_rank_zero",
            )
        )

    # Just check for a short timeout to ensure the test doesn't error out.
    done, _ = ray.wait(remote_tasks, num_returns=len(remote_tasks), timeout=2)
    assert not done, "All tasks should be hanging, but some are done."

    # Finish up once the last worker joins.
    remote_tasks.append(
        sync_actor.broadcast_from_rank_zero.remote(
            world_rank=9,
            world_size=10,
            data="data-9",
            caller_method_name="broadcast_from_rank_zero",
        )
    )
    ray.get(remote_tasks)


def test_world_size_mismatch():
    """The test checks if the workers are blocked and raise an value error
    when the world size is different. The workers should block and raise
    a ValueError.
    """
    sync_actor = SynchronizationActor.remote()
    remote_tasks = []

    # All workers pass use a world size of 10, except for one.
    for rank in range(9):
        remote_tasks.append(
            sync_actor.broadcast_from_rank_zero.remote(
                world_rank=rank,
                world_size=10,
                data=f"data-{rank}",
                caller_method_name="broadcast_from_rank_zero",
            )
        )

    # The last worker calls broadcast with a different world size.
    # This task should raise an error immediately.
    mismatch_task = sync_actor.broadcast_from_rank_zero.remote(
        world_rank=9,
        world_size=11,
        data="data-9",
        caller_method_name="broadcast_from_rank_zero",
    )
    with pytest.raises(ValueError, match="same world size"):
        ray.get(mismatch_task)


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-x", __file__]))
