"""End-to-end tests for the streaming_split + replica-replacement code
path used by Ray Train V2 backends with ``has_replica_groups=True``
(e.g., torchft).

Two test surfaces are covered here:

1. ``DatasetsCallback`` lifecycle (unit, no trainer): verifies the
   provider hand-off across ``before_replica_group_shutdown`` /
   ``before_init_train_context_on_replica_group``, plus the defensive
   asserts on out-of-range / not-shut-down ranks.

2. ``test_replica_replacement_recovery`` (parametrized, integration):
   spins up a real DataParallelTrainer with the mock replica-group
   backend and a configurable failure injector; asserts each user-visible
   epoch sees every row exactly once (or, for the simultaneous-failure
   scenario where the controller falls through to a full worker-group
   restart, every row at least once).
"""

from dataclasses import dataclass
from typing import Optional, Tuple
from unittest.mock import MagicMock

import pytest

import ray
import ray.data
import ray.train
from ray.data._internal.iterator.stream_split_iterator import StreamSplitDataIterator
from ray.train.v2._internal.callbacks.datasets import DatasetsCallback
from ray.train.v2.api.data_parallel_trainer import DataParallelTrainer
from ray.train.v2.tests.util import (
    MockReplicaGroupBackendConfig,
    create_dummy_run_context,
)

# ---------------------------------------------------------------------
# DatasetsCallback lifecycle (unit tests, no trainer involved).
# ---------------------------------------------------------------------


def _mock_worker(world_rank: int, node_id: str = "node-x"):
    """A MagicMock worker with explicitly-set
    ``distributed_context.world_rank`` and ``metadata.node_id``
    (DatasetsCallback reads both).
    """
    w = MagicMock()
    w.distributed_context.world_rank = world_rank
    w.metadata.node_id = node_id
    return w


def _build_callback_with_workers(num_workers: int, num_rows: int = 40):
    """Construct a DatasetsCallback + mock worker list ready for the
    callback round-trip tests."""
    train_ds = ray.data.range(num_rows)
    data_config = ray.train.DataConfig(datasets_to_split=["train"])
    scaling_config = ray.train.ScalingConfig(num_workers=num_workers)
    train_run_context = create_dummy_run_context(
        dataset_config=data_config, scaling_config=scaling_config
    )
    workers = [_mock_worker(rank) for rank in range(num_workers)]
    callback = DatasetsCallback(
        train_run_context=train_run_context,
        datasets={"train": train_ds},
    )
    return callback, workers


def test_datasets_callback_replacement_round_trip(ray_start_4_cpus):
    """Walks the full lifecycle."""
    NUM_WORKERS = 2
    callback, workers = _build_callback_with_workers(NUM_WORKERS)

    # Init worker group
    providers = callback.before_init_train_context_on_worker_group(workers)[
        "dataset_shard_provider"
    ]
    assert len(providers) == NUM_WORKERS
    assert callback._shard_provider_active == [True, True]
    original_provider_0 = providers[0]
    iter_0 = original_provider_0._dataset_iterators["train"]
    assert isinstance(iter_0, StreamSplitDataIterator)
    assert iter_0._is_replacement is False

    # Shut down replica group
    fake_replica = MagicMock()
    fake_replica.get_workers.return_value = [workers[0]]
    callback.before_replica_group_shutdown(fake_replica)
    assert callback._shard_provider_active == [False, True]

    # Replace replica group
    repl_workers = [_mock_worker(0)]
    repl_providers = callback.before_init_train_context_on_replica_group(repl_workers)[
        "dataset_shard_provider"
    ]
    assert len(repl_providers) == 1
    assert repl_providers[0] is original_provider_0
    assert callback._shard_provider_active == [True, True]
    assert iter_0._is_replacement is True


def test_datasets_callback_replacement_before_shutdown_raises(ray_start_4_cpus):
    """Calling before_init_train_context_on_replica_group for a rank that
    is still marked active (no shutdown happened) is an error."""
    callback, workers = _build_callback_with_workers(num_workers=2)
    callback.before_init_train_context_on_worker_group(workers)
    assert callback._shard_provider_active == [True, True]

    with pytest.raises(AssertionError, match="still marked active"):
        callback.before_init_train_context_on_replica_group([_mock_worker(0)])


def test_datasets_callback_shutdown_out_of_range_rank_raises(ray_start_4_cpus):
    """Defensive: a worker whose world_rank is outside the configured
    world size triggers an assertion in before_replica_group_shutdown."""
    callback, workers = _build_callback_with_workers(num_workers=2)
    callback.before_init_train_context_on_worker_group(workers)

    fake_replica = MagicMock()
    fake_replica.get_workers.return_value = [_mock_worker(world_rank=99)]
    with pytest.raises(AssertionError, match="outside of the range"):
        callback.before_replica_group_shutdown(fake_replica)


# ---------------------------------------------------------------------
# Parametrized end-to-end recovery test.
# ---------------------------------------------------------------------


@dataclass(frozen=True)
class _FailurePoint:
    """A single failure to inject into the train_fn. The gate fires each
    point at most once — over the lifetime of the test (across all
    worker-process attempts), not per-process.

    Mid-batch trigger: ``after_n_rows`` fires when the worker has consumed
    that many rows in its current process attempt.

    End-of-epoch trigger: ``after_epoch`` fires after the inner
    iter_batches loop finishes for that epoch index.
    """

    rank: int
    after_n_rows: Optional[int] = None
    after_epoch: Optional[int] = None


@dataclass(frozen=True)
class _Scenario:
    name: str
    num_epochs: int
    max_failures: int
    failures: Tuple[_FailurePoint, ...]
    # When True, the controller is expected to fall through to a full
    # worker-group restart (e.g., simultaneous failure of all replicas);
    # the dataset replays from scratch, so per-epoch row duplicates are
    # OK and we only assert completeness.
    expects_full_restart: bool = False


_SCENARIOS = [
    _Scenario(
        name="single_failure_rank_0_mid_epoch",
        num_epochs=1,
        max_failures=1,
        failures=(_FailurePoint(rank=0, after_n_rows=20),),
    ),
    _Scenario(
        name="rank_0_failure_at_epoch_boundary",
        num_epochs=3,
        max_failures=1,
        failures=(_FailurePoint(rank=0, after_epoch=0),),
    ),
    _Scenario(
        name="two_failures_same_rank",
        num_epochs=1,
        max_failures=2,
        failures=(
            _FailurePoint(rank=0, after_n_rows=10),
            _FailurePoint(rank=0, after_n_rows=10),
        ),
    ),
    _Scenario(
        name="simultaneous_failures_two_ranks_full_restart",
        num_epochs=1,
        max_failures=1,
        # Both ranks fail at the same row count; both deaths land in the
        # same controller poll, so failing_rgs == all_rgs and the
        # controller falls through to a full worker-group restart.
        failures=(
            _FailurePoint(rank=0, after_n_rows=10),
            _FailurePoint(rank=1, after_n_rows=10),
        ),
        expects_full_restart=True,
    ),
]


@pytest.mark.parametrize("scenario", _SCENARIOS, ids=lambda s: s.name)
def test_replica_replacement_recovery(ray_start_4_cpus, scenario):
    """End-to-end recovery test under the various failure scenarios in
    ``_SCENARIOS``. Each scenario configures one or more
    ``_FailurePoint``s; the gate fires each at most once. After fit(),
    the test asserts that every row in [0, NUM_ROWS) is delivered to
    user code in each epoch:

    - exactly-once if the controller is expected to take the
      replica-replacement path (the data cursor is preserved across
      worker death);
    - at-least-once if a full worker-group restart is expected (the
      streaming pipeline is rebuilt and may replay rows).
    """
    NUM_ROWS = 100
    NUM_WORKERS = 2

    @ray.remote(num_cpus=0)
    class Collector:
        def __init__(self):
            self._rows = []

        def add(self, epoch, row):
            self._rows.append((epoch, row))

        def get_rows(self):
            return list(self._rows)

    @ray.remote(num_cpus=0)
    class FailureGate:
        """Tracks failure points; fires each at most once.
        ``maybe_fail`` returns True when a matching point fires (caller
        should raise), False otherwise.

        Points come in as ``(rank, after_n_rows, after_epoch)`` tuples
        — plain primitives so cross-process serialization doesn't pull
        in this test module's classes.
        """

        def __init__(self, points):
            self._points = list(points)
            self._fired = [False] * len(points)

        def maybe_fail(self, rank, rows_seen=None, epoch_just_finished=None):
            for i, (p_rank, p_after_n_rows, p_after_epoch) in enumerate(self._points):
                if (
                    self._fired[i]
                    or p_rank != rank
                    or (p_after_n_rows is not None and rows_seen != p_after_n_rows)
                    or (
                        p_after_epoch is not None
                        and epoch_just_finished != p_after_epoch
                    )
                ):
                    continue
                self._fired[i] = True
                return True
            return False

    failure_tuples = [
        (fp.rank, fp.after_n_rows, fp.after_epoch) for fp in scenario.failures
    ]

    collector = Collector.remote()
    gate = FailureGate.remote(failure_tuples)
    train_ds = ray.data.range(NUM_ROWS)

    num_epochs = scenario.num_epochs

    def train_fn():
        rank = ray.train.get_context().get_world_rank()
        rows_seen = 0
        shard = ray.train.get_dataset_shard("train")
        for epoch in range(num_epochs):
            for batch in shard.iter_batches(batch_size=10):
                for row_obj in batch["id"]:
                    row = int(row_obj)
                    ray.get(collector.add.remote(epoch, row))
                    rows_seen += 1
                    if ray.get(gate.maybe_fail.remote(rank=rank, rows_seen=rows_seen)):
                        raise RuntimeError(
                            f"Injected mid-batch failure at rank={rank} "
                            f"rows_seen={rows_seen}"
                        )
            # End-of-epoch failure check.
            if ray.get(gate.maybe_fail.remote(rank=rank, epoch_just_finished=epoch)):
                raise RuntimeError(
                    f"Injected boundary failure at rank={rank} after epoch={epoch}"
                )

    trainer = DataParallelTrainer(
        train_fn,
        datasets={"train": train_ds},
        backend_config=MockReplicaGroupBackendConfig(),
        scaling_config=ray.train.ScalingConfig(num_workers=NUM_WORKERS),
        run_config=ray.train.RunConfig(
            failure_config=ray.train.FailureConfig(max_failures=scenario.max_failures),
        ),
    )
    trainer.fit()

    entries = ray.get(collector.get_rows.remote())
    by_epoch = {}
    for epoch, row in entries:
        by_epoch.setdefault(epoch, []).append(row)

    for epoch in range(num_epochs):
        rows = sorted(by_epoch.get(epoch, []))
        if scenario.expects_full_restart:
            # Full restart replays the dataset; tolerate duplicates but
            # every row must appear at least once.
            assert set(rows) == set(range(NUM_ROWS)), (
                f"Epoch {epoch}: missing rows after full worker-group "
                f"restart. Got: {rows}"
            )
        else:
            # Replica replacement preserves the cursor; exactly-once.
            assert rows == list(range(NUM_ROWS)), (
                f"Epoch {epoch}: expected exactly-once delivery via "
                f"replica replacement, got: {rows}"
            )


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-x", __file__]))
