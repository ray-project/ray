"""Correctness, optimizer, and streaming checks for Dataset.shuffle_batches."""

import asyncio
import os
import sys
import threading
import unittest
from concurrent.futures import ThreadPoolExecutor
from functools import partial
from types import GeneratorType
from unittest.mock import patch

import numpy as np
import pandas as pd
import pyarrow as pa
import pytest

import ray
from ray.actor import ActorHandle
from ray.data import RandomSeedConfig
from ray.data._internal import shuffle_batches as shuffle_batches_impl
from ray.data._internal.compute import TaskPoolStrategy
from ray.data._internal.execution.execution_callback import ExecutionCallback
from ray.data._internal.execution.interfaces.execution_options import ExecutionResources
from ray.data._internal.execution.interfaces.task_context import TaskContext
from ray.data._internal.logical.operators import (
    Limit,
    ShuffleBatches,
    StreamingRepartition,
)
from ray.data._internal.logical.optimizers import get_execution_plan
from ray.data._internal.logical.rules.limit_pushdown import LimitPushdownRule
from ray.data._internal.tensor_extensions import chunked_tensor_take
from ray.data._internal.tensor_extensions.arrow import ArrowTensorArray
from ray.data.context import DataContext
from ray.data.dataset import MaterializedDataset
from ray.data.expressions import col

_ORIGINAL_SHUFFLE = shuffle_batches_impl._shuffle_batch
_BACKPRESSURE_STATE = None


class _ObserveBackpressure(ExecutionCallback):
    def on_execution_step(self, executor):
        state = _BACKPRESSURE_STATE
        if state is None or not state["paused"].is_set():
            return
        for op, op_state in executor._topology.items():
            if op.name != "ShuffleBatches":
                continue
            if (
                op._task_submission_backpressure_policy
                in ("ResourceBudget", "DownstreamCapacity")
                or op._task_output_backpressure_policy == "ResourceBudget"
            ):
                state["observed"]["submitted"] = op.metrics.num_tasks_submitted
                state["observed"]["queued_bytes"] = op_state.output_queue_bytes()
                state["backpressured"].set()


def _probe_shuffle(batch, seed_config, *, batch_idx=0, gate):
    """Measure the real map-task input and tensor-gather calls."""
    fast_takes = 0

    def profile(frame, event, arg):
        nonlocal fast_takes
        if (
            event == "call"
            and frame.f_code.co_filename == chunked_tensor_take.__file__
            and frame.f_code.co_name == "take"
        ):
            fast_takes += 1

    previous_profile = sys.getprofile()
    sys.setprofile(profile)
    try:
        result = _ORIGINAL_SHUFFLE(batch, seed_config, batch_idx=batch_idx)
    finally:
        sys.setprofile(previous_profile)
    assert isinstance(result, pa.Table)
    ctx = TaskContext.get_current()
    assert ctx is not None
    # Synchronize two workers so overlap doesn't depend on worker startup timing.
    ray.get(gate.arrive.remote(os.getpid()), timeout=30)
    metrics = {
        "_probe_chunks": max((c.num_chunks for c in batch.columns), default=0),
        "_probe_fast_takes": fast_takes,
        "_probe_pid": os.getpid(),
        "_probe_task": ctx.task_idx,
    }
    for name, value in metrics.items():
        result = result.append_column(name, pa.array([value] * len(result)))
    return result


def _fail_once_after_shuffle(batch, seed_config, *, batch_idx=0):
    result = _ORIGINAL_SHUFFLE(batch, seed_config, batch_idx=batch_idx)
    ctx = TaskContext.get_current()
    assert ctx is not None
    if "previous_shuffle" not in ctx.kwargs:
        ctx.kwargs["previous_shuffle"] = result["id"].to_pylist()
        raise ValueError("Injected failure after generating the permutation")
    assert ctx.kwargs["previous_shuffle"] == result["id"].to_pylist()
    return result


def _tag_shuffle_task(batch, seed_config, *, batch_idx=0):
    ctx = TaskContext.get_current()
    assert ctx is not None
    assert "window_seen" not in ctx.kwargs, "A task received more than one window"
    ctx.kwargs["window_seen"] = True
    result = _ORIGINAL_SHUFFLE(batch, seed_config, batch_idx=batch_idx)
    assert isinstance(result, pa.Table)
    return result.append_column("task_idx", pa.array([ctx.task_idx] * len(batch)))


def _record_shuffle_batch(batch, seed_config, *, batch_idx=0):
    result = _ORIGINAL_SHUFFLE(batch, seed_config, batch_idx=batch_idx)
    assert isinstance(result, pa.Table)
    ctx = TaskContext.get_current()
    assert ctx is not None
    metrics = {
        "batch_start": min(batch["id"].to_pylist()),
        "batch_idx": batch_idx,
        "task_idx": ctx.task_idx,
    }
    for name, value in metrics.items():
        result = result.append_column(name, pa.array([value] * len(result)))
    return result


def _record_map_batch(batch):
    return _record_shuffle_batch(
        batch, RandomSeedConfig(seed=42, reseed_after_execution=False)
    )


def _fail_once_in_second_batch(batch, seed_config, *, batch_idx=0):
    result = _ORIGINAL_SHUFFLE(batch, seed_config, batch_idx=batch_idx)
    ctx = TaskContext.get_current()
    assert ctx is not None
    state = ctx.kwargs
    previous = state.setdefault("previous_batches", {})
    ids = result["id"].to_pylist()
    if batch_idx in previous:
        assert ids == previous[batch_idx], "Retry changed the batch permutation"
    previous[batch_idx] = ids
    if batch_idx == 1 and not state.get("failed_once", False):
        state["failed_once"] = True
        raise ValueError("Injected failure in second batch")
    return result


class _WorkerCrashRecorder:
    def __init__(self, crash_batch_idx):
        self.crash_batch_idx = crash_batch_idx
        self.failed = False
        self.previous = {}
        self.replayed = set()

    def record(self, task_idx, batch_idx, ids):
        key = (task_idx, batch_idx)
        if key in self.previous:
            assert ids == self.previous[key], "Worker retry changed the permutation"
            self.replayed.add(key)
        self.previous[key] = ids
        if task_idx == 0 and batch_idx == self.crash_batch_idx and not self.failed:
            self.failed = True
            return True
        return False

    def get_replayed(self):
        return self.replayed


def _crash_once_after_shuffle(batch, seed_config, *, batch_idx=0, recorder):
    result = _ORIGINAL_SHUFFLE(batch, seed_config, batch_idx=batch_idx)
    ctx = TaskContext.get_current()
    assert ctx is not None
    if ray.get(
        recorder.record.remote(ctx.task_idx, batch_idx, result["id"].to_pylist())
    ):
        os._exit(1)
    return result


def _identity(batch):
    return batch


class _WindowGate:
    def __init__(self):
        self.event = asyncio.Event()
        self.arrivals = set()

    async def arrive(self, worker_pid):
        self.arrivals.add(worker_pid)
        if len(self.arrivals) >= 2:
            self.event.set()
        await self.event.wait()

    async def wait(self):
        await self.event.wait()

    async def release(self):
        self.event.set()


def _gated_windows(batch, gate):
    yield pa.table({"id": np.arange(8)})
    ray.get(gate.wait.remote(), timeout=30)
    yield pa.table({"id": np.arange(8, 16)})


def _add_payload(batch):
    return pa.table({"id": batch["id"], "payload": [b"x" * (128 * 1024)] * len(batch)})


def _shuffle(dataset, batch_size, *, exact=False, **kwargs):
    """Exercise direct batching and explicit exact-window composition."""
    if exact:
        dataset = dataset.repartition(target_num_rows_per_block=batch_size, strict=True)
    return dataset.shuffle_batches(batch_size=None if exact else batch_size, **kwargs)


class ShuffleBatchesTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.previous_context = DataContext.get_current().copy()
        # Leave resources for exact repartition alongside two shuffle tasks.
        ray.init(
            num_cpus=4,
            include_dashboard=False,
            object_store_memory=512 * 2**20,
            # Make test-only probe functions importable by local Ray workers
            # when this file is collected directly by pytest.
            runtime_env={"env_vars": {"PYTHONPATH": os.path.dirname(__file__)}},
        )
        ctx = DataContext.get_current()
        ctx.enable_progress_bars = False
        ctx.execution_options.preserve_order = True

    @classmethod
    def tearDownClass(cls):
        ray.shutdown()
        DataContext._set_current(cls.previous_context)

    def setUp(self):
        self.context_before_test = DataContext.get_current().copy()

    def tearDown(self):
        DataContext._set_current(self.context_before_test)

    def source(self, sizes):
        tables = []
        start = 0
        for size in sizes:
            tables.append(pa.table({"id": np.arange(start, start + size)}))
            start += size
        return ray.data.from_arrow(tables).materialize()

    def ids(self, dataset):
        return [row["id"] for row in dataset.take_all()]

    def test_none_shuffles_whole_blocks_without_repartition(self):
        source = self.source([5, 12, 3])
        with patch.object(
            shuffle_batches_impl, "_shuffle_batch", _record_shuffle_batch
        ):
            output = source.shuffle_batches(seed=42, compute=TaskPoolStrategy(size=1))
            rows = output.take_all()
        op = output._logical_plan.dag
        self.assertIsNone(op.batch_size)
        self.assertNotIsInstance(op.input_dependencies[0], StreamingRepartition)
        groups: dict[int, list[int]] = {}
        for row in rows:
            groups.setdefault(row["batch_start"], []).append(row["id"])
        self.assertEqual(
            {start: sorted(ids) for start, ids in groups.items()},
            {0: list(range(5)), 5: list(range(5, 17)), 17: list(range(17, 20))},
        )
        self.assertTrue(any(ids != sorted(ids) for ids in groups.values()))
        self.assertEqual(sorted(row["id"] for row in rows), list(range(20)))

    def test_none_retry_and_execution_reseeding(self):
        source = self.source([31, 29, 20])
        source.context.retried_map_errors = ["Injected failure"]
        source.context.max_map_retries = 1
        expected = self.ids(source.shuffle_batches(seed=42))
        with patch.object(
            shuffle_batches_impl, "_shuffle_batch", _fail_once_after_shuffle
        ):
            actual = self.ids(source.shuffle_batches(batch_size=None, seed=42))
        self.assertEqual(actual, expected)
        output = source.shuffle_batches(
            seed=RandomSeedConfig(seed=42, reseed_after_execution=True)
        )
        first, second = self.ids(output), self.ids(output)
        self.assertEqual(sorted(first), list(range(80)))
        self.assertEqual(sorted(second), list(range(80)))
        self.assertNotEqual(first, second)

    def test_none_tensor_fast_path_and_parallel_tasks(self):
        for variable in (False, True):
            with self.subTest(variable=variable):
                self.check_tensor_path(variable=variable, exact=False, blockwise=True)

    def test_exact_windows_and_tail(self):
        source = self.source([3, 9, 2, 15, 8])
        output = _shuffle(source, 8, exact=True, seed=42).materialize()
        blocks = ray.get(output.to_arrow_refs())
        self.assertEqual([len(b) for b in blocks], [8, 8, 8, 8, 5])
        for index, block in enumerate(blocks):
            self.assertEqual(
                sorted(block["id"].to_pylist()),
                list(range(index * 8, min((index + 1) * 8, 37))),
            )
        self.assertEqual(sorted(self.ids(output)), list(range(37)))

    def test_default_and_exact_shuffle_batch_sizes(self):
        source = self.source([5, 5, 5, 5])
        for exact, expected in ((False, [8, 2, 8, 2]), (True, [8, 8, 4])):
            with self.subTest(exact=exact):
                with patch.object(
                    shuffle_batches_impl, "_shuffle_batch", _record_shuffle_batch
                ):
                    output = _shuffle(source, 8, exact=exact, seed=42)
                    # Iterator batches cross shuffle windows. Probe the actual
                    # shuffle inputs rather than inferring them from output blocks.
                    rows = [
                        row
                        for batch in output.iter_batches(
                            batch_size=3, batch_format="pyarrow"
                        )
                        for row in batch.to_pylist()
                    ]
                groups: dict[int, list[int]] = {}
                for row in rows:
                    groups.setdefault(row["batch_start"], []).append(row["id"])
                windows = [sorted(ids) for _, ids in sorted(groups.items())]
                self.assertEqual([len(ids) for ids in windows], expected)
                start = 0
                for ids, size in zip(windows, expected):
                    self.assertEqual(ids, list(range(start, start + size)))
                    start += size
                self.assertEqual(sorted(row["id"] for row in rows), list(range(20)))

    def test_default_matches_map_batches_membership(self):
        def members(dataset):
            groups: dict[int, list[int]] = {}
            for row in dataset.take_all():
                groups.setdefault(row["batch_start"], []).append(row["id"])
            return [sorted(ids) for _, ids in sorted(groups.items())]

        for batch_size in (None, 8):
            for sizes in ([4] * 10, [3, 9, 2, 15, 8], [103], [3]):
                with self.subTest(sizes=sizes, batch_size=batch_size):
                    source = self.source(sizes)
                    reference = source.map_batches(
                        _record_map_batch,
                        batch_size=batch_size,
                        batch_format="pyarrow",
                        zero_copy_batch=True,
                        compute=TaskPoolStrategy(size=2),
                    )
                    with patch.object(
                        shuffle_batches_impl, "_shuffle_batch", _record_shuffle_batch
                    ):
                        output = source.shuffle_batches(
                            batch_size, seed=42, compute=TaskPoolStrategy(size=2)
                        )
                        actual = members(output)
                    self.assertEqual(actual, members(reference))
                    self.assertEqual(sorted(self.ids(output)), list(range(sum(sizes))))

    def test_repartition_is_explicit(self):
        source = self.source([3, 9, 2, 15, 8])
        for exact in (False, True):
            with self.subTest(exact=exact):
                output = _shuffle(source, 8, exact=exact, seed=42)
                op = output._logical_plan.dag
                self.assertEqual(op.batch_size, None if exact else 8)
                self.assertEqual(op.min_rows_per_bundled_input, None if exact else 8)
                self.assertEqual(
                    isinstance(op.input_dependencies[0], StreamingRepartition), exact
                )
                physical, _ = get_execution_plan(output._logical_plan)
                names = [p.name for p in physical.dag.post_order_iter()]
                self.assertEqual(
                    any(name.startswith("StreamingRepartition") for name in names),
                    exact,
                )

    def test_materialized_input_produces_lazy_output(self):
        source = self.source([3, 9, 2, 15, 8])
        for batch_size, exact in ((8, False), (None, False), (8, True)):
            with self.subTest(batch_size=batch_size, exact=exact):
                output = _shuffle(source, batch_size, exact=exact, seed=42)
                self.assertNotIsInstance(output, MaterializedDataset)
                with self.assertRaises(NotImplementedError):
                    output.num_blocks()
                cached = output.materialize()
                self.assertIsInstance(cached, MaterializedDataset)
                self.assertGreater(cached.num_blocks(), 0)
                self.assertEqual(sorted(self.ids(cached)), list(range(37)))

    def test_default_large_block_has_distinct_batch_permutations(self):
        source = self.source([103])
        with patch.object(
            shuffle_batches_impl, "_shuffle_batch", _record_shuffle_batch
        ):
            rows = _shuffle(source, 16, seed=42).take_all()
        self.assertEqual(len({row["task_idx"] for row in rows}), 1)
        self.assertEqual({row["batch_idx"] for row in rows}, set(range(7)))
        groups = {}
        for row in rows:
            groups.setdefault(row["batch_idx"], []).append(row["id"])
        self.assertEqual([len(groups[i]) for i in range(7)], [16] * 6 + [7])
        self.assertNotEqual(groups[0], [row_id - 16 for row_id in groups[1]])
        self.assertEqual(sorted(row["id"] for row in rows), list(range(103)))

    def test_seed_is_reproducible(self):
        source = self.source([13, 7, 17])
        for batch_size, exact in ((8, False), (None, False), (8, True)):
            with self.subTest(batch_size=batch_size, exact=exact):
                first = _shuffle(source, batch_size, exact=exact, seed=42).materialize()
                second = _shuffle(
                    source, batch_size, exact=exact, seed=42
                ).materialize()
                different = _shuffle(
                    source, batch_size, exact=exact, seed=9
                ).materialize()
                self.assertEqual(self.ids(first), self.ids(second))
                self.assertNotEqual(self.ids(first), self.ids(different))

    def test_unordered_execution_preserves_window_membership(self):
        source = self.source([3, 9, 2, 15, 8])
        source.context.execution_options.preserve_order = False
        output = _shuffle(source, 8, exact=True, seed=42).materialize()
        windows = {
            tuple(sorted(block["id"].to_pylist()))
            for block in ray.get(output.to_arrow_refs())
        }
        self.assertEqual(
            windows,
            {tuple(range(i, min(i + 8, 37))) for i in range(0, 37, 8)},
        )

    def test_limit_stays_after_shuffle(self):
        source = self.source([17, 17, 17])
        for batch_size, exact in ((8, False), (None, False), (8, True)):
            with self.subTest(batch_size=batch_size, exact=exact):
                output = _shuffle(source, batch_size, exact=exact, seed=42)
                self.assertEqual(self.ids(output.limit(11)), self.ids(output)[:11])

    def test_logical_metadata_and_limit_barrier(self):
        source = self.source([17, 17, 17])
        output = _shuffle(source, 8, seed=42)
        op = output._logical_plan.dag
        self.assertIsInstance(op, ShuffleBatches)
        self.assertFalse(op.can_modify_num_rows)
        self.assertEqual(output.schema(), source.schema())
        self.assertEqual(op.infer_metadata().num_rows, 51)
        for count in (1, 8, 11, 100):
            limited = Limit(count, input_dependencies=[op])
            optimized = LimitPushdownRule()._push_limit_down(limited)
            self.assertIsInstance(optimized, Limit)
            self.assertIs(optimized.input_dependencies[0], op)

    def test_shuffle_physical_operator_disables_fusion(self):
        source = self.source([103])
        output = _shuffle(source, 16, seed=42).map_batches(
            _identity, batch_size=64, batch_format="pyarrow"
        )
        physical, _ = get_execution_plan(output._logical_plan)
        operators = list(physical.dag.post_order_iter())
        shuffle = [op for op in operators if op.name == "ShuffleBatches"]
        self.assertEqual(len(shuffle), 1)
        self.assertFalse(shuffle[0].supports_fusion())

    def test_empty_input_preserves_schema_after_materialization(self):
        tables = [
            pa.table(
                {
                    "id": pa.array([], type=pa.int64()),
                    "label": pa.array([], type=pa.string()),
                }
            ),
            pd.DataFrame(
                {
                    "id": pd.Series([], dtype="Int64"),
                    "label": pd.Series([], dtype="category"),
                }
            ),
        ]
        for table in tables:
            source = (
                ray.data.from_arrow(table)
                if isinstance(table, pa.Table)
                else ray.data.from_pandas(table)
            )
            schema = source.schema()
            for batch_size in (None, 8):
                with self.subTest(format=type(table).__name__, batch_size=batch_size):
                    shuffled = source.shuffle_batches(batch_size=batch_size, seed=42)
                    self.assertEqual(shuffled.schema(fetch_if_missing=False), schema)
                    output = shuffled.materialize()
                    self.assertEqual(output.schema(), schema)
                    self.assertEqual(output.take_all(), [])

    def test_empty_blocks_do_not_change_nonempty_permutations(self):
        tables = [pa.table({"id": np.arange(start, start + 8)}) for start in (0, 8)]
        empty = tables[0].slice(0, 0)
        source = ray.data.from_arrow(tables)
        with_empty = ray.data.from_arrow([empty, tables[0], empty, tables[1], empty])
        for batch_size in (None, 4):
            with self.subTest(batch_size=batch_size):
                expected = self.ids(source.shuffle_batches(batch_size, seed=42))
                actual = self.ids(with_empty.shuffle_batches(batch_size, seed=42))
                self.assertEqual(actual, expected)

    def test_small_and_empty_inputs(self):
        self.assertEqual(
            self.ids(_shuffle(self.source([7]), 1, seed=42)), list(range(7))
        )
        empty = ray.data.from_arrow(pa.table({"id": pa.array([], type=pa.int64())}))
        for batch_size, exact in ((8, False), (None, False), (8, True)):
            with self.subTest(batch_size=batch_size, exact=exact):
                small = _shuffle(self.source([3]), batch_size, exact=exact, seed=42)
                self.assertEqual(sorted(self.ids(small)), [0, 1, 2])
                self.assertEqual(
                    _shuffle(empty, batch_size, exact=exact, seed=42).take_all(), []
                )

    def test_nullable_and_dictionary_columns_preserve_values_and_schema(self):
        table = pa.table(
            {
                "id": np.arange(37),
                "value": [None if i % 3 == 0 else i * 2 for i in range(37)],
                "label": pa.array([str(i % 5) for i in range(37)]).dictionary_encode(),
                "items": [None if i % 4 == 0 else [i, i + 1] for i in range(37)],
            }
        )
        source = ray.data.from_arrow([table.slice(i, 7) for i in range(0, 37, 7)])
        for batch_size, exact in ((8, False), (None, False), (8, True)):
            with self.subTest(batch_size=batch_size, exact=exact):
                output = _shuffle(
                    source, batch_size, exact=exact, seed=42
                ).materialize()
                self.assertEqual(output.schema(), source.schema())
                self.assertEqual(
                    sorted(output.take_all(), key=lambda row: row["id"]),
                    table.to_pylist(),
                )

    def test_invalid_batch_size_and_compute(self):
        source = self.source([3])
        for value in (0, -1):
            with self.assertRaises(ValueError):
                source.shuffle_batches(value)
        for value in (True, 1.5, "8"):
            with self.assertRaises(TypeError):
                source.shuffle_batches(value)
        with self.assertRaises(TypeError):
            source.shuffle_batches(8, compute=ray.data.ActorPoolStrategy(size=1))
        with self.assertRaises(TypeError):
            source.shuffle_batches(8, strict=True)

    def test_pandas_blocks_preserve_schema_dtypes_and_values(self):
        DataContext.get_current().enable_pandas_block = True
        frame = pd.DataFrame(
            {
                "id": np.arange(20),
                "text": ["a", "b"] * 10,
                "category": pd.Categorical(["a", "b"] * 10),
                "nullable": pd.array([1, None] * 10, dtype="Int64"),
                "all_null": np.full(20, np.nan),
            },
            index=np.arange(100, 120),
        )
        source = ray.data.from_pandas(
            [frame.iloc[:5], frame.iloc[5:13], frame.iloc[13:]]
        )
        schema = source.schema()
        for batch_size, exact in ((8, False), (None, False), (8, True)):
            with self.subTest(batch_size=batch_size, exact=exact):
                shuffled = _shuffle(source, batch_size, exact=exact, seed=42)
                self.assertEqual(shuffled.schema(fetch_if_missing=False), schema)
                output = shuffled.materialize()
                self.assertEqual(output.schema(), schema)
                self.assertTrue(
                    all(
                        isinstance(batch, pd.DataFrame)
                        for batch in output.iter_batches(
                            batch_size=None, batch_format=None
                        )
                    )
                )
                pd.testing.assert_frame_equal(
                    output.to_pandas().sort_values("id").reset_index(drop=True),
                    frame.reset_index(drop=True),
                )

    def test_retry_replays_with_the_same_task_context(self):
        batch = pa.table({"id": list(range(32))})
        config = RandomSeedConfig(seed=42, reseed_after_execution=False)

        with TaskContext.current(TaskContext(task_idx=3, op_name="test")):
            first = _ORIGINAL_SHUFFLE(batch, config)["id"].to_pylist()
            self.assertEqual(first, _ORIGINAL_SHUFFLE(batch, config)["id"].to_pylist())
        with TaskContext.current(TaskContext(task_idx=4, op_name="test")):
            self.assertNotEqual(
                first, _ORIGINAL_SHUFFLE(batch, config)["id"].to_pylist()
            )

    def test_in_process_retry(self):
        source = self.source([32, 32])
        source.context.retried_map_errors = ["Injected failure"]
        source.context.max_map_retries = 1
        for exact in (False, True):
            with self.subTest(exact=exact):
                with patch.object(
                    shuffle_batches_impl, "_shuffle_batch", _fail_once_after_shuffle
                ):
                    result = _shuffle(source, 32, exact=exact, seed=42).materialize()
                self.assertEqual(sorted(self.ids(result)), list(range(64)))

    def test_multi_batch_retry_replays_permutations(self):
        source = self.source([64])
        source.context.retried_map_errors = ["Injected failure"]
        source.context.max_map_retries = 1
        expected = self.ids(_shuffle(source, 16, seed=42))
        with patch.object(
            shuffle_batches_impl, "_shuffle_batch", _fail_once_in_second_batch
        ):
            actual = self.ids(_shuffle(source, 16, seed=42))
        self.assertEqual(actual, expected)

    def test_worker_crash_replays_permutations(self):
        source = self.source([64])
        # Flush early output before the non-exact task fails in its second batch.
        source.context.target_max_block_size = 96
        for exact in (False, True):
            with self.subTest(exact=exact):
                crash_batch_idx = 0 if exact else 1
                recorder = (
                    ray.remote(_WorkerCrashRecorder)
                    .options(num_cpus=0)
                    .remote(crash_batch_idx)
                )
                assert isinstance(recorder, ActorHandle)
                try:
                    expected = self.ids(_shuffle(source, 16, exact=exact, seed=42))
                    with patch.object(
                        shuffle_batches_impl,
                        "_shuffle_batch",
                        partial(_crash_once_after_shuffle, recorder=recorder),
                    ):
                        actual = self.ids(_shuffle(source, 16, exact=exact, seed=42))
                    self.assertEqual(actual, expected)
                    self.assertIn(
                        (0, crash_batch_idx), ray.get(recorder.get_replayed.remote())
                    )
                finally:
                    ray.kill(recorder)

    def test_large_source_block_creates_one_window_per_shuffle_task(self):
        source = self.source([103])
        with patch.object(shuffle_batches_impl, "_shuffle_batch", _tag_shuffle_task):
            result = _shuffle(source, 16, exact=True, seed=42).materialize()
        blocks = ray.get(result.to_arrow_refs())
        self.assertEqual([len(b) for b in blocks], [16] * 6 + [7])
        tasks = [set(b["task_idx"].to_pylist()) for b in blocks]
        self.assertTrue(all(len(t) == 1 for t in tasks))
        self.assertEqual(len({next(iter(t)) for t in tasks}), len(blocks))
        self.assertEqual(sorted(self.ids(result)), list(range(103)))

    def test_no_implicit_materialization(self):
        source = ray.data.range(37, override_num_blocks=3)
        for batch_size, exact in ((8, False), (None, False), (8, True)):
            with self.subTest(batch_size=batch_size, exact=exact):
                with patch.object(
                    ray.data.Dataset, "materialize", side_effect=AssertionError
                ):
                    result = _shuffle(source, batch_size, exact=exact, seed=42)
                    batches = list(
                        result.iter_batches(batch_size=7, batch_format="pyarrow")
                    )
                self.assertEqual(sum(len(b) for b in batches), 37)

    def test_small_resource_budget_can_finish_a_window(self):
        source = ray.data.range(37, override_num_blocks=19)
        source.context.execution_options.resource_limits = (
            ExecutionResources.for_limits(cpu=1, object_store_memory=2 * 2**20)
        )
        source.context.target_max_block_size = 1024
        source.context.execution_no_progress_timeout_s = 20
        for exact in (False, True):
            with self.subTest(exact=exact):
                output = _shuffle(
                    source, 16, exact=exact, seed=42, compute=TaskPoolStrategy(size=1)
                )
                self.assertEqual(sorted(self.ids(output)), list(range(37)))

    def test_first_window_arrives_before_upstream_finishes(self):
        for exact in (False, True):
            with self.subTest(exact=exact):
                self.check_first_window_arrives_before_upstream_finishes(exact)

    def check_first_window_arrives_before_upstream_finishes(self, exact):
        # Force the first yielded table out of the upstream output buffer before
        # the UDF waits. The gate makes eager/full-input execution fail reliably.
        # Output buffering flushes above (not at) the byte target. Keep this
        # below the first table's 64 bytes without triggering block splitting.
        DataContext.get_current().target_max_block_size = 48
        gate = ray.remote(_WindowGate).options(num_cpus=0).remote()
        assert isinstance(gate, ActorHandle)
        source = ray.data.range(1, override_num_blocks=1).map_batches(
            partial(_gated_windows, gate=gate),
            batch_format="pyarrow",
            batch_size=None,
        )
        iterator = iter(
            _shuffle(source, 8, exact=exact, seed=42).iter_batches(
                batch_size=8, batch_format="pyarrow", prefetch_batches=0
            )
        )
        assert isinstance(iterator, GeneratorType)
        try:
            with ThreadPoolExecutor(max_workers=1) as pool:
                pending = pool.submit(next, iterator)
                try:
                    first = pending.result(timeout=20)
                    assert isinstance(first, pa.Table)
                    self.assertEqual(sorted(first["id"].to_pylist()), list(range(8)))
                finally:
                    ray.get(gate.release.remote())
            remaining = []
            for batch in iterator:
                assert isinstance(batch, pa.Table)
                remaining.extend(batch["id"].to_pylist())
            self.assertEqual(sorted(remaining), list(range(8, 16)))
        finally:
            iterator.close()
            ray.kill(gate)

    def test_slow_consumer_uses_executor_backpressure_and_resumes(self):
        for exact in (False, True):
            with self.subTest(exact=exact):
                self.check_slow_consumer_uses_executor_backpressure_and_resumes(exact)

    def check_slow_consumer_uses_executor_backpressure_and_resumes(self, exact):
        global _BACKPRESSURE_STATE
        # Observe the real default budget policy while the iterator is paused.
        # Test scheduling/backlog, rather than asserting a hard RSS memory cap.
        paused = threading.Event()
        backpressured = threading.Event()
        observed = {}

        _BACKPRESSURE_STATE = {
            "paused": paused,
            "backpressured": backpressured,
            "observed": observed,
        }

        ctx = DataContext.get_current()
        ctx.target_max_block_size = 2 * 2**20
        ctx.execution_options.resource_limits = ExecutionResources.for_limits(
            object_store_memory=16 * 2**20
        )
        ctx.custom_execution_callback_classes = [_ObserveBackpressure]
        total_rows, window = 1024, 8
        output = _shuffle(
            ray.data.range(total_rows, override_num_blocks=128).map_batches(
                _add_payload, batch_format="pyarrow", batch_size=None
            ),
            window,
            exact=exact,
            seed=42,
            compute=TaskPoolStrategy(size=2),
        )
        iterator = iter(
            output.iter_batches(
                batch_size=window, batch_format="pyarrow", prefetch_batches=0
            )
        )
        assert isinstance(iterator, GeneratorType)
        try:
            first = next(iterator)
            assert isinstance(first, pa.Table)
            paused.set()
            self.assertTrue(backpressured.wait(timeout=20), observed)
            self.assertLess(observed["submitted"], total_rows // window, observed)
            paused.clear()
            ids = first["id"].to_pylist()
            for batch in iterator:
                assert isinstance(batch, pa.Table)
                ids.extend(batch["id"].to_pylist())
            self.assertEqual(sorted(ids), list(range(total_rows)))
        finally:
            paused.clear()
            iterator.close()
            _BACKPRESSURE_STATE = None

    def test_downstream_rebatch_preserves_one_window_per_shuffle_task(self):
        source = self.source([103])
        with patch.object(shuffle_batches_impl, "_shuffle_batch", _tag_shuffle_task):
            result = (
                _shuffle(source, 16, exact=True, seed=42)
                .map_batches(_identity, batch_size=64, batch_format="pyarrow")
                .materialize()
            )
        rows = result.take_all()
        self.assertEqual(sorted(row["id"] for row in rows), list(range(103)))
        self.assertEqual(len({row["task_idx"] for row in rows}), 7)

    def test_downstream_rebatch_preserves_permutation(self):
        source = self.source([4] * 16)
        for batch_size, exact in ((8, False), (None, False), (8, True)):
            with self.subTest(batch_size=batch_size, exact=exact):
                shuffled = _shuffle(source, batch_size, exact=exact, seed=42)
                expected = self.ids(shuffled)
                actual = self.ids(
                    shuffled.map_batches(
                        _identity, batch_size=64, batch_format="pyarrow"
                    )
                )
                self.assertEqual(actual, expected)

    def test_filter_stays_after_shuffle(self):
        source = self.source([8, 8, 8, 8])
        for batch_size, exact in ((8, False), (None, False), (8, True)):
            with self.subTest(batch_size=batch_size, exact=exact):
                shuffled = _shuffle(source, batch_size, exact=exact, seed=42)
                expected = [row_id for row_id in self.ids(shuffled) if row_id < 13]
                self.assertEqual(
                    self.ids(shuffled.filter(expr=col("id") < 13)), expected
                )

    def test_serialized_lineage_replays_permutation(self):
        source = ray.data.range(64, override_num_blocks=4)
        for batch_size, exact in ((8, False), (None, False), (8, True)):
            with self.subTest(batch_size=batch_size, exact=exact):
                shuffled = _shuffle(source, batch_size, exact=exact, seed=42)
                restored = ray.data.Dataset.deserialize_lineage(
                    shuffled.serialize_lineage()
                )
                expected = self.ids(shuffled)
                self.assertEqual(sorted(expected), list(range(64)))
                self.assertEqual(self.ids(restored), expected)

    def test_single_row_window_is_noop(self):
        source = self.source([7])
        self.assertIs(_shuffle(source, 1, seed=42), source)

    def test_invalid_seed_fails_on_driver(self):
        source = self.source([3])
        for seed in (-1, "42", 1.5):
            with self.assertRaises(ValueError):
                _shuffle(source, 8, seed=seed)

    def test_execution_reseeding(self):
        source = self.source([13, 7, 17])
        for batch_size, exact in ((8, False), (None, False), (8, True)):
            with self.subTest(batch_size=batch_size, exact=exact):
                output = _shuffle(
                    source,
                    batch_size,
                    exact=exact,
                    seed=RandomSeedConfig(seed=42, reseed_after_execution=True),
                )
                first = self.ids(output)
                second = self.ids(output)
                self.assertEqual(sorted(first), sorted(second))
                self.assertNotEqual(first, second)

    def test_unseeded_executions(self):
        source = self.source([64])
        for batch_size, exact in ((16, False), (None, False), (16, True)):
            with self.subTest(batch_size=batch_size, exact=exact):
                output = _shuffle(source, batch_size, exact=exact)
                first = self.ids(output)
                second = self.ids(output)
                self.assertEqual(sorted(first), list(range(64)))
                self.assertEqual(sorted(second), list(range(64)))
                self.assertNotEqual(first, second)

    def test_materialization_freezes_reseeded_permutation(self):
        source = self.source([13, 7, 17])
        for batch_size, exact in ((8, False), (None, False), (8, True)):
            with self.subTest(batch_size=batch_size, exact=exact):
                cached = _shuffle(
                    source,
                    batch_size,
                    exact=exact,
                    seed=RandomSeedConfig(seed=42, reseed_after_execution=True),
                ).materialize()
                first = self.ids(cached)
                second = self.ids(cached)
                self.assertEqual(first, second)

    def check_tensor_path(self, variable, exact, blockwise=False):
        count, window = (2048, 512) if variable else (512, 128)
        tables = []
        for start in range(0, count, 64):
            if variable:
                data = [
                    np.full((64 if i % 2 == 0 else 128, 64), i, dtype=np.float32)
                    for i in range(start, start + 64)
                ]
            else:
                data = np.broadcast_to(
                    np.arange(start, start + 64, dtype=np.float32)[:, None, None],
                    (64, 64, 64),
                ).copy()
            tables.append(
                pa.Table.from_arrays(
                    [
                        pa.array(np.arange(start, start + 64)),
                        ArrowTensorArray.from_numpy(data),
                    ],
                    names=["id", "tensor"],
                )
            )
        if blockwise:
            chunks_per_block = window // 64
            tables = [
                pa.concat_tables(tables[i : i + chunks_per_block])
                for i in range(0, len(tables), chunks_per_block)
            ]
        source = ray.data.from_arrow(tables).materialize()
        gate = ray.remote(_WindowGate).options(num_cpus=0).remote()
        assert isinstance(gate, ActorHandle)
        try:
            with patch.object(
                shuffle_batches_impl,
                "_shuffle_batch",
                partial(_probe_shuffle, gate=gate),
            ):
                output = _shuffle(
                    source,
                    None if blockwise else window,
                    exact=exact,
                    seed=42,
                    compute=TaskPoolStrategy(size=2),
                    num_cpus=1,
                ).materialize()
        finally:
            ray.kill(gate)
        blocks = ray.get(output.to_arrow_refs())
        all_ids = []
        worker_pids = set()
        task_indices = []
        for block in blocks:
            task_indices.append(block["_probe_task"][0].as_py())
            self.assertGreaterEqual(block["_probe_chunks"][0].as_py(), 2)
            self.assertEqual(block["_probe_fast_takes"][0].as_py(), 1)
            ids = block["id"].to_pylist()
            all_ids.extend(ids)
            self.assertEqual(len({i // window for i in ids}), 1)
            tensors = block["tensor"].combine_chunks().to_numpy(zero_copy_only=False)
            for i, tensor in zip(ids, tensors):
                expected_shape = (128 if variable and i % 2 else 64, 64)
                self.assertEqual(tensor.shape, expected_shape)
                self.assertEqual(tensor.dtype, np.dtype(np.float32))
                self.assertTrue(np.all(tensor == i))
            worker_pids.add(block["_probe_pid"][0].as_py())
        self.assertEqual(sorted(all_ids), list(range(count)))
        self.assertEqual(len(set(task_indices)), len(blocks))
        self.assertGreaterEqual(len(worker_pids), 2)

    def test_small_tensor_columns_use_fallback(self):
        count = 128
        for variable in (False, True):
            data = [
                np.full((4 if variable and i % 2 else 2, 4), i, dtype=np.int64)
                for i in range(count)
            ]
            table = pa.Table.from_arrays(
                [pa.array(np.arange(count)), ArrowTensorArray.from_numpy(data)],
                names=["id", "tensor"],
            )
            source = ray.data.from_arrow(
                [table.slice(i, 16) for i in range(0, count, 16)]
            ).materialize()
            for exact in (False, True):
                with self.subTest(variable=variable, exact=exact):
                    gate = ray.remote(_WindowGate).options(num_cpus=0).remote()
                    assert isinstance(gate, ActorHandle)
                    try:
                        with patch.object(
                            shuffle_batches_impl,
                            "_shuffle_batch",
                            partial(_probe_shuffle, gate=gate),
                        ):
                            output = _shuffle(
                                source, 64, exact=exact, seed=42
                            ).take_all()
                    finally:
                        ray.kill(gate)
                    self.assertEqual(
                        sorted(row["id"] for row in output), list(range(count))
                    )
                    for row in output:
                        self.assertEqual(row["_probe_fast_takes"], 0)
                        self.assertGreaterEqual(row["_probe_chunks"], 2)
                        shape = (4 if variable and row["id"] % 2 else 2, 4)
                        self.assertEqual(row["tensor"].shape, shape)
                        self.assertEqual(row["tensor"].dtype, np.dtype(np.int64))
                        self.assertTrue(np.all(row["tensor"] == row["id"]))

    def test_fixed_tensor_fast_path_and_parallel_tasks(self):
        for exact in (False, True):
            with self.subTest(exact=exact):
                self.check_tensor_path(variable=False, exact=exact)

    def test_variable_tensor_fast_path_and_parallel_tasks(self):
        for exact in (False, True):
            with self.subTest(exact=exact):
                self.check_tensor_path(variable=True, exact=exact)


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
