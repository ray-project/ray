import pytest

import ray
from ray.exceptions import RayTaskError
from ray.train.v2._internal.constants import DEFAULT_COLLECTIVE_TIMEOUT_S
from ray.train.v2._internal.exceptions import BroadcastCollectiveTimeoutError
from ray.train.v2._internal.execution.checkpoint.sync_actor import (
    SynchronizationActor,
)


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
    # Ensure all the states are cleared after the broadcast function returns
    assert ray.get(sync_actor.get_counter.remote()) == 0
    assert ray.get(sync_actor.get_world_size.remote()) == 0
    assert ray.get(sync_actor.get_reduced_data.remote()) is None


@pytest.mark.parametrize("world_size", [1, 4, 10])
def test_collective_all_gather(world_size):
    """Every rank receives every rank's value, ordered by world rank."""
    sync_actor = SynchronizationActor.remote()
    values = [f"rank-{rank}" for rank in range(world_size)]
    remote_tasks = [
        sync_actor.collective_all_gather.remote(
            world_rank=rank,
            world_size=world_size,
            data=value,
            caller_method_name="collective_all_gather",
        )
        for rank, value in enumerate(values)
    ]

    assert ray.get(remote_tasks) == [values] * world_size
    # State is fully released once everyone has left.
    assert ray.get(sync_actor.get_counter.remote()) == 0
    assert ray.get(sync_actor.get_world_size.remote()) == 0
    assert ray.get(sync_actor.get_reduced_data.remote()) is None


def test_collective_all_gather_preserves_none():
    """A rank contributing None is distinguishable from a missing contribution."""
    sync_actor = SynchronizationActor.remote()
    remote_tasks = [
        sync_actor.collective_all_gather.remote(
            world_rank=rank,
            world_size=3,
            data=None if rank != 1 else "only-rank-1",
            caller_method_name="collective_all_gather",
        )
        for rank in range(3)
    ]
    assert ray.get(remote_tasks) == [[None, "only-rank-1", None]] * 3


@pytest.mark.parametrize("world_size", [2, 4])
def test_fast_worker_cannot_join_the_collective_it_just_left(world_size):
    """Alternating collective kinds across rounds must not interfere.

    ``get_preemption_info()`` (all_gather) and ``report()`` (broadcast) alternate
    every training step, and ``_clear_states`` only releases the previous round's
    world size and reduce op once the *last* worker has drained. This guards the
    window in between: each rank issues its second call as soon as its first
    returns, so the fastest rank runs ahead while its peers are still leaving.

    This documents required behavior rather than a fixed bug -- the window was
    not reachable in testing, because the returning worker's next RPC round trip
    is far slower than a waiting worker's resumption.
    """
    sync_actor = SynchronizationActor.remote()
    for round_index in range(10):
        pending = {
            sync_actor.collective_all_gather.remote(
                world_rank=rank,
                world_size=world_size,
                data=f"round{round_index}-gather-{rank}",
                caller_method_name="ray.train.get_preemption_info",
            ): rank
            for rank in range(world_size)
        }
        expected_gather = [f"round{round_index}-gather-{r}" for r in range(world_size)]
        second_round = []
        # As each rank returns, it immediately runs ahead into the next
        # collective while its peers are still draining.
        while pending:
            done, _ = ray.wait(list(pending), num_returns=1, timeout=30)
            assert done, "collective did not make progress"
            rank = pending.pop(done[0])
            assert ray.get(done[0]) == expected_gather
            second_round.append(
                sync_actor.broadcast_from_rank_zero.remote(
                    world_rank=rank,
                    world_size=world_size,
                    data=f"round{round_index}-broadcast",
                    caller_method_name="ray.train.report",
                )
            )
        assert ray.get(second_round) == [f"round{round_index}-broadcast"] * world_size


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
    assert DEFAULT_COLLECTIVE_TIMEOUT_S is None

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


def test_collective_operation_mismatch():
    """Ranks disagreeing on the collective kind within one round is an error."""
    sync_actor = SynchronizationActor.remote()
    waiting_task = sync_actor.collective_all_gather.remote(
        world_rank=0,
        world_size=2,
        data="data-0",
        caller_method_name="collective_all_gather",
    )
    # Give rank 0 time to join so rank 1 validates against it.
    ray.wait([waiting_task], timeout=2)

    mismatch_task = sync_actor.broadcast_from_rank_zero.remote(
        world_rank=1,
        world_size=2,
        data="data-1",
        caller_method_name="broadcast_from_rank_zero",
    )
    with pytest.raises(RayTaskError, match="same collective operation"):
        ray.get(mismatch_task)

    ray.get(sync_actor.reset.remote())
    with pytest.raises(RayTaskError):
        ray.get(waiting_task)


def test_reset_releases_state_for_dead_workers():
    """reset() must leave the barrier immediately usable.

    It is called when workers have died, so it cannot wait for the current
    callers to leave -- they never will.
    """
    sync_actor = SynchronizationActor.remote()
    stuck = sync_actor.collective_all_gather.remote(
        world_rank=0,
        world_size=2,
        data="doomed",
        caller_method_name="collective_all_gather",
    )
    ray.wait([stuck], timeout=2)
    ray.get(sync_actor.reset.remote())
    with pytest.raises(RayTaskError):
        ray.get(stuck)

    assert ray.get(sync_actor.get_counter.remote()) == 0
    assert ray.get(sync_actor.get_world_size.remote()) == 0

    # A fresh collective at a different world size must work right away.
    tasks = [
        sync_actor.broadcast_from_rank_zero.remote(
            world_rank=rank,
            world_size=3,
            data="after-reset",
            caller_method_name="broadcast_from_rank_zero",
        )
        for rank in range(3)
    ]
    assert ray.get(tasks) == ["after-reset"] * 3


def test_reset():
    """Test that calling reset() unblocks all waiting workers with
    SynchronizationBarrierResetError and leaves the actor usable for
    subsequent barriers.
    """
    sync_actor = SynchronizationActor.remote()

    # 9 out of 10 workers enter the barrier — they should all block.
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

    # Verify all tasks are blocking.
    done, _ = ray.wait(remote_tasks, num_returns=len(remote_tasks), timeout=2)
    assert not done, "All tasks should be hanging, but some are done."

    # Reset the actor — this should unblock all 9 workers.
    ray.get(sync_actor.reset.remote())

    # All 9 tasks should raise SynchronizationBarrierResetError.
    with pytest.raises(RayTaskError) as excinfo:
        ray.get(remote_tasks)
    assert "SynchronizationBarrierResetError" in str(excinfo.value)

    # Actor state should be fully cleaned up.
    assert ray.get(sync_actor.get_counter.remote()) == 0
    assert ray.get(sync_actor.get_world_size.remote()) == 0
    assert ray.get(sync_actor.get_reduced_data.remote()) is None

    # The actor should still be usable for a subsequent barrier.
    remote_tasks = []
    for rank in range(10):
        remote_tasks.append(
            sync_actor.broadcast_from_rank_zero.remote(
                world_rank=rank,
                world_size=10,
                data=f"data-{rank}",
                caller_method_name="broadcast_from_rank_zero",
            )
        )
    assert all([each == "data-0" for each in ray.get(remote_tasks)])


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-x", __file__]))
