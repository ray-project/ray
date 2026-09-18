import time

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


def _broadcast(sync_actor, rank, world_size, seq=1, relaxable=True):
    """Enter collective `seq` as `rank`, carrying that rank's own data.

    Defaults to a relaxable collective, since that is what the relaxation
    tests exercise; pass relaxable=False for the public-API contract.
    """
    return sync_actor.broadcast_from_rank_zero.remote(
        world_rank=rank,
        world_size=world_size,
        data=f"data-{rank}",
        caller_method_name="broadcast_from_rank_zero",
        collective_seq=seq,
        relaxable=relaxable,
    )


def _wait_until_parked(sync_actor, count, timeout_s=30):
    """Block until `count` workers are inside the barrier.

    Submitting a broadcast only queues an actor task, so without this the
    arrival order is not deterministic.
    """
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        if ray.get(sync_actor.get_counter.remote()) == count:
            return
        time.sleep(0.05)
    raise AssertionError(f"only {ray.get(sync_actor.get_counter.remote())} parked")


@pytest.mark.parametrize("world_size", [1, 10, 1000])
def test_broadcast_from_rank_0(world_size):
    """Check that rank 0 can broadcast data to all other workers.
    Every worker sends data with a string "data-{rank}" that is unique
    to the worker. Everyone should receive the data from rank 0, which is "data-0".
    Also assert that the actor state is reset after the broadcast function returns.
    """
    sync_actor = SynchronizationActor.remote()
    remote_tasks = [
        _broadcast(sync_actor, rank, world_size) for rank in range(world_size)
    ]
    # Ensure that all workers have the same consensus data same as rank 0
    assert all([each == "data-0" for each in ray.get(remote_tasks)])
    # Ensure all the states are cleared after the broadcast function returns
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
    remote_tasks = [_broadcast(sync_actor, rank, 10) for rank in range(9)]
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
    remote_tasks = [_broadcast(sync_actor, rank, 10) for rank in range(9)]

    # Just check for a short timeout to ensure the test doesn't error out.
    done, _ = ray.wait(remote_tasks, num_returns=len(remote_tasks), timeout=2)
    assert not done, "All tasks should be hanging, but some are done."

    # Finish up once the last worker joins.
    remote_tasks.append(_broadcast(sync_actor, 9, 10))
    ray.get(remote_tasks)


def test_world_size_mismatch():
    """The test checks if the workers are blocked and raise an value error
    when the world size is different. The workers should block and raise
    a ValueError.
    """
    sync_actor = SynchronizationActor.remote()

    # All workers pass use a world size of 10, except for one.
    remote_tasks = [_broadcast(sync_actor, rank, 10) for rank in range(9)]
    # Wait until they are all inside the barrier before the mismatching rank
    # arrives. Submitting a broadcast only queues an actor task, and this is an
    # async actor, so without this the mismatching rank can be the one that
    # establishes the world size -- and then these nine raise instead of it.
    _wait_until_parked(sync_actor, len(remote_tasks))

    # The last worker calls broadcast with a different world size.
    # This task should raise an error immediately.
    mismatch_task = _broadcast(sync_actor, 9, 11)
    with pytest.raises(ValueError, match="same world size"):
        ray.get(mismatch_task)


def test_reset():
    """Test that calling reset() unblocks all waiting workers with
    SynchronizationBarrierResetError and leaves the actor usable for
    subsequent barriers.
    """
    sync_actor = SynchronizationActor.remote()

    # 9 out of 10 workers enter the barrier — they should all block.
    remote_tasks = [_broadcast(sync_actor, rank, 10) for rank in range(9)]

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
    remote_tasks = [_broadcast(sync_actor, rank, 10) for rank in range(10)]
    assert all([each == "data-0" for each in ray.get(remote_tasks)])


def test_set_expected_ranks_releases_blocked_workers():
    """Relaxing the barrier releases ranks already waiting on a lost rank.

    This is the ordering that matters in practice: the healthy ranks reach the
    barrier first and only then does the controller notice the preemption.
    """
    sync_actor = SynchronizationActor.remote()
    tasks = [_broadcast(sync_actor, rank, 4) for rank in range(3)]

    # Rank 3 is gone, so the strict barrier cannot release.
    done, _ = ray.wait(tasks, num_returns=len(tasks), timeout=3)
    assert not done, "Tasks should be hanging until the barrier is relaxed."

    assert ray.get(sync_actor.set_expected_ranks.remote([0, 1, 2]))
    assert ray.get(tasks) == ["data-0"] * 3
    assert ray.get(sync_actor.get_expected_ranks.remote()) == [0, 1, 2]


def test_set_expected_ranks_before_arrival():
    """A barrier entered after relaxation releases on the expected ranks."""
    sync_actor = SynchronizationActor.remote()
    assert ray.get(sync_actor.set_expected_ranks.remote([0, 1]))
    assert ray.get([_broadcast(sync_actor, r, 4) for r in (0, 1)]) == ["data-0"] * 2


def test_expected_ranks_persist_across_barriers():
    """One preemption window spans several barriers, so the set must persist.

    `ray.train.get_preemption_info()` broadcasts, then `report()` broadcasts
    the checkpoint dir name. Both must be relaxed by the single set the
    controller installs on `PreemptingState` entry.
    """
    sync_actor = SynchronizationActor.remote()
    ray.get(sync_actor.set_expected_ranks.remote([0, 1]))
    for _ in range(3):
        assert ray.get([_broadcast(sync_actor, r, 4) for r in (0, 1)]) == ["data-0"] * 2
    assert ray.get(sync_actor.get_expected_ranks.remote()) == [0, 1]


def test_set_expected_ranks_rejects_set_without_rank_0():
    """Rank 0 is the sole writer of the payload, so it can never be dropped."""
    sync_actor = SynchronizationActor.remote()
    assert not ray.get(sync_actor.set_expected_ranks.remote([1, 2, 3]))
    assert ray.get(sync_actor.get_expected_ranks.remote()) is None

    # The barrier stayed strict, so ranks 1-3 still wait for rank 0.
    tasks = [_broadcast(sync_actor, rank, 4) for rank in (1, 2, 3)]
    done, _ = ray.wait(tasks, num_returns=len(tasks), timeout=3)
    assert not done, "Tasks should be hanging: the relaxation was rejected."


def test_set_expected_ranks_none_restores_strict_barrier():
    sync_actor = SynchronizationActor.remote()
    ray.get(sync_actor.set_expected_ranks.remote([0, 1]))
    ray.get(sync_actor.set_expected_ranks.remote(None))
    assert ray.get(sync_actor.get_expected_ranks.remote()) is None

    tasks = [_broadcast(sync_actor, rank, 4) for rank in (0, 1)]
    done, _ = ray.wait(tasks, num_returns=len(tasks), timeout=3)
    assert not done, "Tasks should be hanging: the strict barrier is back."


def test_non_relaxable_collective_still_requires_every_rank():
    """`ray.train.collective.*` keeps its all-rank contract while relaxed.

    Only Ray Train's own preemption and checkpoint collectives opt in, so a
    user barrier taken during a drain still waits for every worker.
    """
    sync_actor = SynchronizationActor.remote()
    ray.get(sync_actor.set_expected_ranks.remote([0, 1]))

    # Ranks 0 and 1 are the expected set, but this collective did not opt in.
    tasks = [_broadcast(sync_actor, r, 4, relaxable=False) for r in (0, 1)]
    done, _ = ray.wait(tasks, num_returns=len(tasks), timeout=5)
    assert not done, "A non-relaxable collective must still wait for all ranks."

    # It releases only once every rank arrives.
    tasks += [_broadcast(sync_actor, r, 4, relaxable=False) for r in (2, 3)]
    assert ray.get(tasks, timeout=30) == ["data-0"] * 4


def test_straggler_from_an_earlier_collective_is_not_released():
    """A rank left behind in collective N must not take collective N+1's payload.

    Relaxing the barrier breaks lockstep: the expected ranks stop waiting, so
    they can be in the next collective while a straggler is still arriving at
    the previous one. Without a sequence number the actor cannot tell the two
    apart, and the straggler leaves with the wrong value entirely -- a
    checkpoint directory name in answer to `get_preemption_info`, say.
    """
    sync_actor = SynchronizationActor.remote()
    ray.get(sync_actor.set_expected_ranks.remote([0, 1]))

    # Rank 3 is still arriving at collective 1.
    straggler = _broadcast(sync_actor, 3, 4, seq=1)
    _wait_until_parked(sync_actor, 1)

    # The expected ranks have already moved on to collective 2.
    expected = [_broadcast(sync_actor, r, 4, seq=2) for r in (0, 1)]
    assert ray.get(expected) == ["data-0"] * 2
    assert ray.get(sync_actor.get_released_seq.remote()) == 2

    done, _ = ray.wait([straggler], num_returns=1, timeout=5)
    assert not done, "Straggler was released with another collective's payload."


def test_straggler_in_the_current_collective_is_still_released():
    """A straggler that really is in the current collective rides along."""
    sync_actor = SynchronizationActor.remote()
    ray.get(sync_actor.set_expected_ranks.remote([0, 1]))

    straggler = _broadcast(sync_actor, 3, 4, seq=1)
    _wait_until_parked(sync_actor, 1)

    both = [_broadcast(sync_actor, r, 4, seq=1) for r in (0, 1)]
    assert ray.get(both) == ["data-0"] * 2
    assert ray.get(straggler, timeout=30) == "data-0"


def test_expected_ranks_progress_while_a_straggler_is_parked():
    """A parked straggler must not stall the expected ranks' next barrier."""
    sync_actor = SynchronizationActor.remote()
    ray.get(sync_actor.set_expected_ranks.remote([0, 1]))

    parked = _broadcast(sync_actor, 3, 4, seq=1)
    _wait_until_parked(sync_actor, 1)

    # Successive collectives must all release even though rank 3 still holds a
    # slot from collective 1.
    for seq in (2, 3, 4):
        both = [_broadcast(sync_actor, r, 4, seq=seq) for r in (0, 1)]
        assert ray.get(both) == ["data-0"] * 2

    done, _ = ray.wait([parked], num_returns=1, timeout=3)
    assert not done, "Straggler should still be parked."


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-x", __file__]))
