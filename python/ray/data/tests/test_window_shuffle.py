"""Correctness, optimizer, and streaming checks for Dataset.window_shuffle."""

import asyncio
import os
import sys
import threading
import time
import unittest
from concurrent.futures import ThreadPoolExecutor
from functools import partial
from unittest.mock import patch

import numpy as np
import pyarrow as pa

import ray
from ray.data import RandomSeedConfig
from ray.data._internal import window_shuffle as window_shuffle_impl
from ray.data._internal.compute import TaskPoolStrategy
from ray.data._internal.execution.execution_callback import ExecutionCallback
from ray.data._internal.execution.interfaces.execution_options import ExecutionResources
from ray.data._internal.execution.interfaces.task_context import TaskContext
from ray.data._internal.logical.operators import (
    Limit,
    StreamingRepartition,
    WindowShuffle,
)
from ray.data._internal.logical.optimizers import get_execution_plan
from ray.data._internal.logical.rules.limit_pushdown import LimitPushdownRule
from ray.data._internal.tensor_extensions import chunked_tensor_take
from ray.data._internal.tensor_extensions.arrow import ArrowTensorArray
from ray.data.context import DataContext

_ORIGINAL_SHUFFLE = window_shuffle_impl._shuffle_window
_BACKPRESSURE_STATE = None


class _ObserveBackpressure(ExecutionCallback):
    def on_execution_step(self, executor):
        state = _BACKPRESSURE_STATE
        if state is None or not state["paused"].is_set():
            return
        for op, op_state in executor._topology.items():
            if op.name != "WindowShuffle":
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

    started = time.monotonic_ns()
    previous_profile = sys.getprofile()
    sys.setprofile(profile)
    try:
        result = _ORIGINAL_SHUFFLE(batch, seed_config, batch_idx=batch_idx)
    finally:
        sys.setprofile(previous_profile)
    # Synchronize two workers so overlap doesn't depend on worker startup timing.
    ray.get(gate.arrive.remote(os.getpid()), timeout=30)
    metrics = {
        "_probe_chunks": max((c.num_chunks for c in batch.columns), default=0),
        "_probe_fast_takes": fast_takes,
        "_probe_pid": os.getpid(),
        "_probe_start": started,
        "_probe_end": time.monotonic_ns(),
        "_probe_task": TaskContext.get_current().task_idx,
    }
    for name, value in metrics.items():
        result = result.append_column(name, pa.array([value] * len(result)))
    return result


def _fail_once_after_shuffle(batch, seed_config, *, batch_idx=0):
    result = _ORIGINAL_SHUFFLE(batch, seed_config, batch_idx=batch_idx)
    ctx = TaskContext.get_current()
    if "previous_shuffle" not in ctx.kwargs:
        ctx.kwargs["previous_shuffle"] = result["id"].to_pylist()
        raise ValueError("Injected failure after generating the permutation")
    assert ctx.kwargs["previous_shuffle"] == result["id"].to_pylist()
    return result


def _tag_shuffle_task(batch, seed_config, *, batch_idx=0):
    ctx = TaskContext.get_current()
    assert "window_seen" not in ctx.kwargs, "A task received more than one window"
    ctx.kwargs["window_seen"] = True
    return _ORIGINAL_SHUFFLE(batch, seed_config, batch_idx=batch_idx).append_column(
        "task_idx", pa.array([ctx.task_idx] * len(batch))
    )


def _record_shuffle_batch(batch, seed_config, *, batch_idx=0):
    result = _ORIGINAL_SHUFFLE(batch, seed_config, batch_idx=batch_idx)
    metrics = {
        "batch_start": min(batch["id"].to_pylist()),
        "batch_idx": batch_idx,
        "task_idx": TaskContext.get_current().task_idx,
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
    state = TaskContext.get_current().kwargs
    previous = state.setdefault("previous_batches", {})
    ids = result["id"].to_pylist()
    if batch_idx in previous:
        assert ids == previous[batch_idx], "Retry changed the batch permutation"
    previous[batch_idx] = ids
    if batch_idx == 1 and not state.get("failed_once", False):
        state["failed_once"] = True
        raise ValueError("Injected failure in second batch")
    return result


@ray.remote(num_cpus=0)
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
    if ray.get(
        recorder.record.remote(
            TaskContext.get_current().task_idx, batch_idx, result["id"].to_pylist()
        )
    ):
        os._exit(1)
    return result


def _identity(batch):
    return batch


@ray.remote(num_cpus=0)
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


class WindowShuffleTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.previous_context = DataContext.get_current().copy()
        # Leave resources for strict repartition alongside two shuffle tasks.
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

    def test_exact_windows_and_tail(self):
        source = self.source([3, 9, 2, 15, 8])
        output = source.window_shuffle(8, strict=True, seed=42).materialize()
        blocks = ray.get(output.to_arrow_refs())
        self.assertEqual([len(b) for b in blocks], [8, 8, 8, 8, 5])
        for index, block in enumerate(blocks):
            self.assertEqual(
                sorted(block["id"].to_pylist()),
                list(range(index * 8, min((index + 1) * 8, 37))),
            )
        self.assertEqual(sorted(self.ids(output)), list(range(37)))

    def test_default_matches_map_batches_membership(self):
        def members(dataset):
            groups = {}
            for row in dataset.take_all():
                groups.setdefault(row["batch_start"], []).append(row["id"])
            return [sorted(ids) for _, ids in sorted(groups.items())]

        for sizes in ([4] * 10, [3, 9, 2, 15, 8], [103], [3]):
            with self.subTest(sizes=sizes):
                source = self.source(sizes)
                reference = source.map_batches(
                    _record_map_batch,
                    batch_size=8,
                    batch_format="pyarrow",
                    zero_copy_batch=True,
                    compute=TaskPoolStrategy(size=2),
                )
                with patch.object(
                    window_shuffle_impl, "_shuffle_window", _record_shuffle_batch
                ):
                    output = source.window_shuffle(
                        8, seed=42, compute=TaskPoolStrategy(size=2)
                    )
                self.assertEqual(members(output), members(reference))
                self.assertEqual(sorted(self.ids(output)), list(range(sum(sizes))))

    def test_strict_selects_repartition(self):
        source = self.source([3, 9, 2, 15, 8])
        for strict in (False, True):
            with self.subTest(strict=strict):
                output = source.window_shuffle(8, strict=strict, seed=42)
                op = output._logical_plan.dag
                self.assertEqual(op.strict, strict)
                self.assertEqual(op.batch_size, None if strict else 8)
                self.assertEqual(op.min_rows_per_bundled_input, None if strict else 8)
                self.assertEqual(
                    isinstance(op.input_dependencies[0], StreamingRepartition), strict
                )
                physical, _ = get_execution_plan(output._logical_plan)
                names = [p.name for p in physical.dag.post_order_iter()]
                self.assertEqual(
                    any(name.startswith("StreamingRepartition") for name in names),
                    strict,
                )

    def test_default_large_block_has_distinct_batch_permutations(self):
        source = self.source([103])
        with patch.object(
            window_shuffle_impl, "_shuffle_window", _record_shuffle_batch
        ):
            rows = source.window_shuffle(16, seed=42).take_all()
        self.assertEqual(len({row["task_idx"] for row in rows}), 1)
        self.assertEqual({row["batch_idx"] for row in rows}, set(range(7)))
        groups = {}
        for row in rows:
            groups.setdefault(row["batch_idx"], []).append(row["id"])
        self.assertEqual([len(groups[i]) for i in range(7)], [16] * 6 + [7])
        self.assertNotEqual(groups[0], [row_id - 16 for row_id in groups[1]])
        self.assertEqual(sorted(row["id"] for row in rows), list(range(103)))

    def test_seed_and_materialization(self):
        source = self.source([13, 7, 17])
        for strict in (False, True):
            with self.subTest(strict=strict):
                first = source.window_shuffle(8, strict=strict, seed=42).materialize()
                second = source.window_shuffle(8, strict=strict, seed=42).materialize()
                different = source.window_shuffle(
                    8, strict=strict, seed=9
                ).materialize()
                self.assertEqual(self.ids(first), self.ids(second))
                self.assertEqual(self.ids(first), self.ids(first))
                self.assertNotEqual(self.ids(first), self.ids(different))

    def test_unordered_execution_preserves_window_membership(self):
        source = self.source([3, 9, 2, 15, 8])
        source.context.execution_options.preserve_order = False
        output = source.window_shuffle(8, strict=True, seed=42).materialize()
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
        for strict in (False, True):
            with self.subTest(strict=strict):
                output = source.window_shuffle(8, strict=strict, seed=42)
                self.assertEqual(self.ids(output.limit(11)), self.ids(output)[:11])

    def test_logical_metadata_and_limit_barrier(self):
        source = self.source([17, 17, 17])
        output = source.window_shuffle(8, seed=42)
        op = output._logical_plan.dag
        self.assertIsInstance(op, WindowShuffle)
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
        output = source.window_shuffle(16, seed=42).map_batches(
            _identity, batch_size=64, batch_format="pyarrow"
        )
        physical, _ = get_execution_plan(output._logical_plan)
        operators = list(physical.dag.post_order_iter())
        shuffle = [op for op in operators if op.name == "WindowShuffle"]
        self.assertEqual(len(shuffle), 1)
        self.assertFalse(shuffle[0].supports_fusion())

    def test_small_and_empty_inputs(self):
        self.assertEqual(
            self.ids(self.source([7]).window_shuffle(1, seed=42)), list(range(7))
        )
        empty = ray.data.from_arrow(pa.table({"id": pa.array([], type=pa.int64())}))
        for strict in (False, True):
            with self.subTest(strict=strict):
                small = self.source([3]).window_shuffle(8, strict=strict, seed=42)
                self.assertEqual(sorted(self.ids(small)), [0, 1, 2])
                self.assertEqual(
                    empty.window_shuffle(8, strict=strict, seed=42).take_all(), []
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
        for strict in (False, True):
            with self.subTest(strict=strict):
                output = source.window_shuffle(8, strict=strict, seed=42).materialize()
                self.assertEqual(output.schema(), source.schema())
                self.assertEqual(
                    sorted(output.take_all(), key=lambda row: row["id"]),
                    table.to_pylist(),
                )

    def test_invalid_window_size_and_compute(self):
        source = self.source([3])
        for value in (0, -1):
            with self.assertRaises(ValueError):
                source.window_shuffle(value)
        for value in (True, 1.5, "8"):
            with self.assertRaises(TypeError):
                source.window_shuffle(value)
        with self.assertRaises(TypeError):
            source.window_shuffle(8, compute=ray.data.ActorPoolStrategy(size=1))
        for value in (None, 0, 1, "true"):
            with self.assertRaises(TypeError):
                source.window_shuffle(8, strict=value)

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
        for strict in (False, True):
            with self.subTest(strict=strict):
                with patch.object(
                    window_shuffle_impl, "_shuffle_window", _fail_once_after_shuffle
                ):
                    result = source.window_shuffle(
                        32, strict=strict, seed=42
                    ).materialize()
                self.assertEqual(sorted(self.ids(result)), list(range(64)))

    def test_multi_batch_retry_replays_permutations(self):
        source = self.source([64])
        source.context.retried_map_errors = ["Injected failure"]
        source.context.max_map_retries = 1
        expected = self.ids(source.window_shuffle(16, seed=42))
        with patch.object(
            window_shuffle_impl, "_shuffle_window", _fail_once_in_second_batch
        ):
            actual = self.ids(source.window_shuffle(16, seed=42))
        self.assertEqual(actual, expected)

    def test_worker_crash_replays_permutations(self):
        source = self.source([64])
        # Flush early output before the non-strict task fails in its second batch.
        source.context.target_max_block_size = 96
        for strict in (False, True):
            with self.subTest(strict=strict):
                crash_batch_idx = 0 if strict else 1
                recorder = _WorkerCrashRecorder.remote(crash_batch_idx)
                try:
                    expected = self.ids(
                        source.window_shuffle(16, strict=strict, seed=42)
                    )
                    with patch.object(
                        window_shuffle_impl,
                        "_shuffle_window",
                        partial(_crash_once_after_shuffle, recorder=recorder),
                    ):
                        actual = self.ids(
                            source.window_shuffle(16, strict=strict, seed=42)
                        )
                    self.assertEqual(actual, expected)
                    self.assertIn(
                        (0, crash_batch_idx), ray.get(recorder.get_replayed.remote())
                    )
                finally:
                    ray.kill(recorder)

    def test_large_source_block_creates_one_window_per_shuffle_task(self):
        source = self.source([103])
        with patch.object(window_shuffle_impl, "_shuffle_window", _tag_shuffle_task):
            result = source.window_shuffle(16, strict=True, seed=42).materialize()
        blocks = ray.get(result.to_arrow_refs())
        self.assertEqual([len(b) for b in blocks], [16] * 6 + [7])
        tasks = [set(b["task_idx"].to_pylist()) for b in blocks]
        self.assertTrue(all(len(t) == 1 for t in tasks))
        self.assertEqual(len({next(iter(t)) for t in tasks}), len(blocks))
        self.assertEqual(sorted(self.ids(result)), list(range(103)))

    def test_no_implicit_materialization(self):
        source = ray.data.range(37, override_num_blocks=3)
        for strict in (False, True):
            with self.subTest(strict=strict):
                with patch.object(
                    ray.data.Dataset, "materialize", side_effect=AssertionError
                ):
                    result = source.window_shuffle(8, strict=strict, seed=42)
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
        for strict in (False, True):
            with self.subTest(strict=strict):
                output = source.window_shuffle(
                    16, strict=strict, seed=42, compute=TaskPoolStrategy(size=1)
                )
                self.assertEqual(sorted(self.ids(output)), list(range(37)))

    def test_first_window_arrives_before_upstream_finishes(self):
        for strict in (False, True):
            with self.subTest(strict=strict):
                self.check_first_window_arrives_before_upstream_finishes(strict)

    def check_first_window_arrives_before_upstream_finishes(self, strict):
        # Force the first yielded table out of the upstream output buffer before
        # the UDF waits. The gate makes eager/full-input execution fail reliably.
        # Output buffering flushes above (not at) the byte target. Keep this
        # below the first table's 64 bytes without triggering block splitting.
        DataContext.get_current().target_max_block_size = 48
        gate = _WindowGate.remote()
        source = ray.data.range(1, override_num_blocks=1).map_batches(
            _gated_windows,
            batch_format="pyarrow",
            batch_size=None,
            fn_args=(gate,),
        )
        iterator = iter(
            source.window_shuffle(8, strict=strict, seed=42).iter_batches(
                batch_size=8, batch_format="pyarrow", prefetch_batches=0
            )
        )
        try:
            with ThreadPoolExecutor(max_workers=1) as pool:
                pending = pool.submit(next, iterator)
                try:
                    first = pending.result(timeout=20)
                    self.assertEqual(sorted(first["id"].to_pylist()), list(range(8)))
                finally:
                    ray.get(gate.release.remote())
            remaining = [i for b in iterator for i in b["id"].to_pylist()]
            self.assertEqual(sorted(remaining), list(range(8, 16)))
        finally:
            iterator.close()
            ray.kill(gate)

    def test_slow_consumer_uses_executor_backpressure_and_resumes(self):
        for strict in (False, True):
            with self.subTest(strict=strict):
                self.check_slow_consumer_uses_executor_backpressure_and_resumes(strict)

    def check_slow_consumer_uses_executor_backpressure_and_resumes(self, strict):
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
        output = (
            ray.data.range(total_rows, override_num_blocks=128)
            .map_batches(_add_payload, batch_format="pyarrow", batch_size=None)
            .window_shuffle(
                window, strict=strict, seed=42, compute=TaskPoolStrategy(size=2)
            )
        )
        iterator = iter(
            output.iter_batches(
                batch_size=window, batch_format="pyarrow", prefetch_batches=0
            )
        )
        try:
            first = next(iterator)
            paused.set()
            self.assertTrue(backpressured.wait(timeout=20), observed)
            self.assertLess(observed["submitted"], total_rows // window, observed)
            paused.clear()
            ids = first["id"].to_pylist()
            ids.extend(i for b in iterator for i in b["id"].to_pylist())
            self.assertEqual(sorted(ids), list(range(total_rows)))
        finally:
            paused.clear()
            iterator.close()
            _BACKPRESSURE_STATE = None

    def test_downstream_rebatch_preserves_one_window_per_shuffle_task(self):
        source = self.source([103])
        with patch.object(window_shuffle_impl, "_shuffle_window", _tag_shuffle_task):
            result = (
                source.window_shuffle(16, strict=True, seed=42)
                .map_batches(_identity, batch_size=64, batch_format="pyarrow")
                .materialize()
            )
        rows = result.take_all()
        self.assertEqual(sorted(row["id"] for row in rows), list(range(103)))
        self.assertEqual(len({row["task_idx"] for row in rows}), 7)

    def test_single_row_window_is_noop(self):
        source = self.source([7])
        self.assertIs(source.window_shuffle(1, seed=42), source)

    def test_invalid_seed_fails_on_driver(self):
        source = self.source([3])
        for seed in (-1, "42", 1.5):
            with self.assertRaises(ValueError):
                source.window_shuffle(8, seed=seed)

    def test_execution_reseeding(self):
        source = self.source([13, 7, 17])
        for strict in (False, True):
            with self.subTest(strict=strict):
                output = source.window_shuffle(
                    8,
                    strict=strict,
                    seed=RandomSeedConfig(seed=42, reseed_after_execution=True),
                )
                first = self.ids(output)
                second = self.ids(output)
                self.assertEqual(sorted(first), sorted(second))
                self.assertNotEqual(first, second)

    def test_unseeded_executions(self):
        source = self.source([64])
        for strict in (False, True):
            with self.subTest(strict=strict):
                output = source.window_shuffle(16, strict=strict)
                first = self.ids(output)
                second = self.ids(output)
                self.assertEqual(sorted(first), list(range(64)))
                self.assertEqual(sorted(second), list(range(64)))
                self.assertNotEqual(first, second)

    def check_tensor_path(self, variable, strict):
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
        source = ray.data.from_arrow(tables).materialize()
        gate = _WindowGate.remote()
        try:
            with patch.object(
                window_shuffle_impl,
                "_shuffle_window",
                partial(_probe_shuffle, gate=gate),
            ):
                output = source.window_shuffle(
                    window,
                    strict=strict,
                    seed=42,
                    compute=TaskPoolStrategy(size=2),
                    num_cpus=1,
                ).materialize()
        finally:
            ray.kill(gate)
        blocks = ray.get(output.to_arrow_refs())
        all_ids = []
        intervals = []
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
            intervals.append(
                tuple(
                    block[name][0].as_py()
                    for name in ("_probe_pid", "_probe_start", "_probe_end")
                )
            )
        self.assertEqual(sorted(all_ids), list(range(count)))
        self.assertEqual(len(set(task_indices)), len(blocks))
        self.assertGreaterEqual(len({pid for pid, _, _ in intervals}), 2, intervals)
        self.assertTrue(
            any(
                a[0] != b[0] and max(a[1], b[1]) < min(a[2], b[2])
                for a in intervals
                for b in intervals
            )
        )

    def test_fixed_tensor_fast_path_and_parallel_tasks(self):
        for strict in (False, True):
            with self.subTest(strict=strict):
                self.check_tensor_path(variable=False, strict=strict)

    def test_variable_tensor_fast_path_and_parallel_tasks(self):
        for strict in (False, True):
            with self.subTest(strict=strict):
                self.check_tensor_path(variable=True, strict=strict)


if __name__ == "__main__":
    unittest.main(verbosity=2)
