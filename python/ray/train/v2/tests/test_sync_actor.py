import pytest

import ray
from ray.train.v2._internal.exceptions import BroadcastCollectiveTimeoutError
from ray.train.v2._internal.execution.checkpoint.sync_actor import SynchronizationActor


@pytest.fixture(autouse=True, scope="module")
def ray_start_4_cpus():
    ray.init(num_cpus=4)
    yield
    ray.shutdown()


@pytest.mark.parametrize("world_size", [1, 10, 1000])
def test_broadcast_from_rank_0(world_size):
    """The test checks if all workers can reach a consensus on a data.
    Every worker sends data with a string "data-{rank}" that is unique
    to the worker. Expected to get a consensus data of "data-0".
    Also checks if the counter is reset to 0 after all workers have data.
    """
    sync_actor = SynchronizationActor.remote()
    # Test broadcast_from_rank_zero with a world size of 10
    remote_tasks = []
    for rank in range(world_size):
        remote_tasks.append(
            sync_actor.broadcast_from_rank_zero.remote(
                world_rank=rank, world_size=world_size, data=f"data-{rank}"
            )
        )
    # Ensure that all workers have the same consensus data same as rank 0
    assert all([each == "data-0" for each in ray.get(remote_tasks)])
    # Ensure al the states are cleared after the broadcast function returns
    assert ray.get(sync_actor.get_counter.remote()) == 0
    assert ray.get(sync_actor.get_world_size.remote()) == 0
    assert ray.get(sync_actor.get_reduced_data.remote()) is None


def test_hang():
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
                world_rank=rank, world_size=10, data=f"data-{rank}"
            )
        )
    # Ensure that the workers are blocked and raise BroadcastCollectiveTimeoutError
    # after 1 second
    with pytest.raises(BroadcastCollectiveTimeoutError) as excinfo:
        ray.get(remote_tasks)
    # Ensure that "rank 9 workers not reaching barrier" included in the error message
    assert (
        "The following ranks are not reaching the collective "
        "operation: [9]\n" in str(excinfo.value)
    )


def test_world_size_mismatch():
    """The test checks if the workers are blocked and raise an value error
    when the world size is different. The workers should block and raise
    an ValueError.
    """
    sync_actor = SynchronizationActor.remote()
    # Test broadcast_from_rank_zero with a world size of 10. But
    # one of the worker reports a different world size, raise an assertion error.
    remote_tasks = []
    for rank in range(9):
        remote_tasks.append(
            sync_actor.broadcast_from_rank_zero.remote(
                world_rank=rank, world_size=10, data=f"data-{rank}"
            )
        )
    # The last worker datas with a different world size
    remote_tasks.append(
        sync_actor.broadcast_from_rank_zero.remote(
            world_rank=9, world_size=11, data="data-9"
        )
    )
    # Ensure that the workers are blocked and getting GetTimeoutError after 5 seconds
    with pytest.raises(ValueError) as excinfo:
        ray.get(remote_tasks, timeout=1)
    assert "ValueError" in str(excinfo.type)


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-x", __file__]))
