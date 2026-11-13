import pickle
import threading
import weakref
from typing import Any, Dict, Generator, List, Optional, Union
from unittest import mock

import pandas as pd
import pyarrow as pa
import pytest

import ray
from ray._common.test_utils import wait_for_condition
from ray.data._internal.arrow_block import ArrowBlockAccessor
from ray.data._internal.arrow_ops import transform_pyarrow
from ray.data._internal.delegating_block_builder import DelegatingBlockBuilder
from ray.data._internal.execution.interfaces import (
    BlockEntry,
    ExecutionOptions,
    ExecutionResources,
    RefBundle,
)
from ray.data._internal.execution.interfaces.physical_operator import DataOpTask
from ray.data._internal.execution.metadata_fetcher import ThreadedMetadataFetcher
from ray.data._internal.execution.operators.input_data_buffer import InputDataBuffer
from ray.data._internal.execution.operators.limit_operator import LimitOperator
from ray.data._internal.execution.operators.top_n_operator import (
    _MERGE_BATCH_WAIT_S,
    _MERGE_PEAK_MEMORY_MULTIPLIER,
    TopNMergeOperator,
    _batch_candidates,
    _estimate_merge_peak_bytes,
    _final_top_n_task,
    _merge_top_n_blocks,
)
from ray.data._internal.execution.streaming_executor_state import (
    build_streaming_topology,
    update_operator_states,
)
from ray.data._internal.execution.util import make_ref_bundles
from ray.data._internal.pandas_block import PandasBlockAccessor
from ray.data._internal.planner.exchange.sort_task_spec import SortKey
from ray.data._internal.stats import Timer
from ray.data._internal.table_block import TableBlockAccessor
from ray.data.block import (
    Block,
    BlockAccessor,
    BlockMetadata,
    Schema,
)
from ray.data.context import DataContext
from ray.data.tests.conftest import *  # noqa: F401, F403
from ray.data.tests.conftest import noop_counter
from ray.data.tests.util import run_op_tasks_sync
from ray.exceptions import RayTaskError
from ray.tests.conftest import *  # noqa: F401, F403


@pytest.fixture
def fixed_merge_clock() -> Generator[None, None, None]:
    # Prevent incidental deadline flushes; tests advance time explicitly.
    with mock.patch(
        "ray.data._internal.execution.operators.top_n_operator.monotonic",
        return_value=0.0,
    ):
        yield


def _make_top_n_merge_operator(
    context: DataContext,
    *,
    sort_key: Optional[SortKey] = None,
    limit: int = 2,
    ray_remote_args: Optional[Dict[str, Any]] = None,
    output_schema: Optional[Schema] = None,
) -> TopNMergeOperator:
    operator = TopNMergeOperator(
        InputDataBuffer(context, []),
        context,
        sort_key=sort_key or SortKey("id"),
        limit=limit,
        ray_remote_args=ray_remote_args or {},
        output_schema=output_schema,
        name=f"TopNMerge(limit={limit})",
    )
    operator.start(ExecutionOptions(), noop_counter())
    return operator


def _assert_owned_bundles_destroyed(
    destroy_if_owned: mock.Mock, bundles: List[RefBundle]
) -> None:
    owned_calls = [
        call for call in destroy_if_owned.call_args_list if call.args[0].owns_blocks
    ]

    assert owned_calls == [mock.call(bundle) for bundle in bundles]


def _expire_pending_batch(operator: TopNMergeOperator) -> None:
    assert operator._pending_since is not None

    with mock.patch(
        "ray.data._internal.execution.operators.top_n_operator.monotonic",
        return_value=operator._pending_since + 2 * _MERGE_BATCH_WAIT_S,
    ):
        operator.refresh_state()


def _advance_merge_to_stage(
    operator: TopNMergeOperator, bundle: RefBundle, stage: str
) -> None:
    operator.add_input(bundle, 0)

    if stage == "collecting":
        return

    if stage == "state":
        _expire_pending_batch(operator)
        run_op_tasks_sync(operator)
        return

    operator.all_inputs_done()

    if stage == "output":
        run_op_tasks_sync(operator)


@pytest.mark.parametrize(
    "sizes,max_bytes,max_blocks,expected",
    [
        ([40, 30, 50, 10], 80, 32, [[0, 1], [2, 3]]),
        ([10, 10, 10], 80, 2, [[0, 1], [2]]),
        ([100, 10], 80, 32, [[0], [1]]),
        ([40, 40, 10], 80, 32, [[0, 1], [2]]),
    ],
)
def test_batch_candidates_uses_shared_limits(sizes, max_bytes, max_blocks, expected):
    refs = [mock.sentinel.a, mock.sentinel.b, mock.sentinel.c, mock.sentinel.d]
    entries = [
        BlockEntry(
            ref,
            BlockMetadata(
                num_rows=1, size_bytes=size, input_files=None, exec_stats=None
            ),
        )
        for ref, size in zip(refs, sizes)
    ]
    operator = _make_top_n_merge_operator(DataContext())
    for entry in entries:
        operator._input_buffer.add(RefBundle((entry,), owns_blocks=False, schema=None))

    with (
        mock.patch(
            "ray.data._internal.execution.operators.top_n_operator._MERGE_BATCH_BYTES",
            max_bytes,
        ),
        mock.patch(
            "ray.data._internal.execution.operators.top_n_operator._MERGE_BATCH_BLOCKS",
            max_blocks,
        ),
    ):
        batches = list(_batch_candidates(entries))
        driver_batches = []
        while operator._input_buffer.has_next():
            batch = operator._get_candidate_batch()
            driver_batches.append(
                [entry for bundle in batch for entry in bundle.blocks]
            )
            for _ in batch:
                operator._input_buffer.get_next()

    assert (
        batches
        == driver_batches
        == [[entries[index] for index in batch] for batch in expected]
    )


@pytest.mark.parametrize("num_rows,expected_peak", [(10, 120), (None, 400)])
def test_merge_memory_estimates_batch_peak(num_rows, expected_peak):
    entries = [
        BlockEntry(
            mock.sentinel.ref,
            BlockMetadata(
                num_rows=num_rows, size_bytes=100, input_files=None, exec_stats=None
            ),
        )
        for _ in range(4)
    ]
    batches = [[entry] for entry in entries]

    # Known rows cap state at 2 * 10 bytes, so peak is 100 + 20 = 120.
    # Unknown row counts budget all prior inputs: peak is 300 + 100 = 400.
    assert _estimate_merge_peak_bytes(batches, limit=2) == expected_peak

    operator = _make_top_n_merge_operator(DataContext())

    assert (
        operator._merge_task_remote_args(batches)["memory"]
        == _MERGE_PEAK_MEMORY_MULTIPLIER * expected_peak
    )


def test_top_n_oversized_bundle_reserves_only_batch_peak(
    ray_start_regular_shared_2_cpus,
):
    bundles = make_ref_bundles([[value, value + 1] for value in range(0, 8, 2)])
    bundle = RefBundle(
        tuple(entry for bundle in bundles for entry in bundle.blocks),
        owns_blocks=True,
        schema=bundles[0].schema,
    )
    block_bytes = bundles[0].size_bytes()
    operator = _make_top_n_merge_operator(DataContext())

    with mock.patch(
        "ray.data._internal.execution.operators.top_n_operator._MERGE_BATCH_BLOCKS", 1
    ):
        try:
            with mock.patch.object(operator, "_maybe_submit_merge"):
                operator.add_input(bundle, 0)

            # Worker peak is one fetched block plus the running two-row result.
            assert (
                operator.per_task_resource_allocation().memory
                == _MERGE_PEAK_MEMORY_MULTIPLIER * (2 * block_bytes)
            )

            operator.refresh_state()

            assert (
                operator.current_logical_usage().memory
                == _MERGE_PEAK_MEMORY_MULTIPLIER * (2 * block_bytes)
            )

            run_op_tasks_sync(operator)
            operator.all_inputs_done()
            output = operator.get_next()
            output_ref = output.block_refs[0]

            assert isinstance(output_ref, ray.ObjectRef)
            assert ray.get(output_ref)["id"].tolist() == [0, 1]

            output.destroy_if_owned()
        finally:
            operator.shutdown(Timer(), force=True)


@pytest.mark.parametrize(
    "block_format,num_blocks",
    [("arrow", 1), ("arrow", 2), ("pandas", 1), ("pandas", 2), ("mixed", 2)],
)
def test_final_top_n_task_selects_once_without_redundant_conversion_and_concat(
    ray_start_regular_shared_2_cpus, block_format: str, num_blocks: int
) -> None:
    blocks = [
        pa.table({"id": values})
        if block_format == "arrow" or (block_format == "mixed" and index == 0)
        else pd.DataFrame({"id": values})
        for index, values in enumerate(([9, 7, 5, 3], [8, 6, 4, 2]))
    ][:num_blocks]
    refs = [ray.put(block) for block in blocks]

    with (
        mock.patch.object(
            ArrowBlockAccessor,
            "top_n",
            autospec=True,
            side_effect=ArrowBlockAccessor.top_n,
        ) as arrow_select,
        mock.patch.object(
            PandasBlockAccessor,
            "top_n",
            autospec=True,
            side_effect=PandasBlockAccessor.top_n,
        ) as pandas_select,
        mock.patch.object(
            TableBlockAccessor,
            "try_convert_block_type",
            wraps=TableBlockAccessor.try_convert_block_type,
        ) as convert,
        mock.patch.object(
            transform_pyarrow, "concat", wraps=transform_pyarrow.concat
        ) as arrow_concat,
        mock.patch.object(pd, "concat", wraps=pd.concat) as pandas_concat,
    ):
        result, _ = list(
            _final_top_n_task([refs], SortKey("id"), 2, None, DataContext())
        )

    assert arrow_select.call_count + pandas_select.call_count == 1
    assert convert.call_count == (num_blocks if block_format == "mixed" else 0)
    assert arrow_concat.call_count + pandas_concat.call_count == int(num_blocks > 1)

    assert BlockAccessor.for_block(result).to_arrow().to_pydict() == {
        "id": [3, 5] if num_blocks == 1 else [2, 3]
    }


def test_merge_empty_batch_does_not_concat_running_state():
    with mock.patch.object(
        DelegatingBlockBuilder, "add_block", side_effect=AssertionError("concat")
    ):
        result = _merge_top_n_blocks(
            [pa.table({"id": pa.array([], type=pa.int64())})],
            pa.table({"id": [1, 2]}),
            SortKey("id"),
            2,
        )

    assert BlockAccessor.for_block(result).to_arrow().to_pydict() == {"id": [1, 2]}


def test_final_top_n_task_keeps_running_top_n_across_batches(
    ray_start_regular_shared_2_cpus,
):
    batches = [
        [ray.put(pa.table({"id": [5, 9]})), ray.put(pd.DataFrame({"id": [7]}))],
        [ray.put(pa.table({"id": [1, 8]}))],
        [ray.put(pa.table({"id": [3]}))],
    ]

    result, _ = list(
        _final_top_n_task(
            batches, SortKey("id"), 3, pa.schema([("id", pa.int64())]), DataContext()
        )
    )

    assert BlockAccessor.for_block(result).to_arrow().to_pylist() == [
        {"id": 1},
        {"id": 3},
        {"id": 5},
    ]


@pytest.mark.parametrize(
    "resource_args",
    [
        {"num_cpus": 2},
        {"num_cpus": 0, "num_gpus": 0.5, "memory": 1024},
    ],
)
def test_top_n_merge_propagates_remote_options(
    ray_start_regular_shared_2_cpus,
    restore_data_context,
    fixed_merge_clock,
    resource_args,
):
    context = DataContext.get_current()
    context.execution_options.label_selector = {
        "subcluster": "train",
        "accelerator": "any",
    }
    sort_key = SortKey("id")
    ray_remote_args = {
        **resource_args,
        "label_selector": {"accelerator": "cpu"},
    }
    output_schema = pa.schema([("id", pa.int64())])
    bundles = make_ref_bundles([[3, 1], [2]])
    operator = _make_top_n_merge_operator(
        context,
        sort_key=sort_key,
        ray_remote_args=ray_remote_args,
        output_schema=output_schema,
    )

    remote_fn = mock.Mock()
    configured_remote_fn = mock.Mock()
    remote_fn.options.return_value = configured_remote_fn
    block_gen = mock.Mock(spec=ray.ObjectRefGenerator)
    block_gen._generator_ref = bundles[0].block_refs[0]
    configured_remote_fn.remote.return_value = block_gen
    expected_bytes = sum(bundle.size_bytes() for bundle in bundles)
    expected_resources = {
        "memory": _MERGE_PEAK_MEMORY_MULTIPLIER * expected_bytes,
        **resource_args,
    }

    with mock.patch(
        "ray.data._internal.execution.operators.top_n_operator.cached_remote_fn",
        return_value=remote_fn,
    ):
        try:
            for bundle in bundles:
                operator.add_input(bundle, 0)
            operator.all_inputs_done()

            remote_fn.options.assert_called_once_with(
                **expected_resources,
                label_selector={"subcluster": "train", "accelerator": "cpu"},
                name="TopNMerge(limit=2)",
                num_returns="streaming",
            )
            configured_remote_fn.remote.assert_called_once_with(
                [[entry.ref for bundle in bundles for entry in bundle.blocks]],
                sort_key,
                2,
                output_schema,
                context,
                0,
            )

            assert (
                operator.current_logical_usage()
                == ExecutionResources.from_resource_dict(expected_resources)
            )
        finally:
            # The mocked generator has no Ray task to cancel.
            with mock.patch("ray.cancel"):
                operator.shutdown(Timer())


def test_final_top_n_task_installs_data_context(
    ray_start_regular_shared_2_cpus,
):
    context = DataContext()
    context.use_polars_sort = True
    block = pa.table({"id": [2, 1]})

    with mock.patch.object(
        DataContext, "current", wraps=DataContext.current
    ) as current:
        result, metadata_bytes = list(
            _final_top_n_task(
                [[ray.put(block)]],
                SortKey("id"),
                1,
                block.schema,
                context,
            )
        )

    current.assert_called_once_with(context)

    assert isinstance(result, pa.Table)
    assert isinstance(metadata_bytes, bytes)
    assert result.to_pylist() == [{"id": 1}]

    metadata = pickle.loads(metadata_bytes)

    assert metadata.metadata.exec_stats.task_idx == 0
    assert metadata.metadata.task_exec_stats.task_wall_time_s > 0


@pytest.mark.parametrize("previous_state", [False, True])
@pytest.mark.parametrize("finish_inputs", [False, True])
def test_top_n_merge_cleans_inputs_on_failure(
    ray_start_regular_shared_2_cpus,
    restore_data_context,
    fixed_merge_clock,
    previous_state,
    finish_inputs,
):
    context = DataContext.get_current()
    bundles = make_ref_bundles([[2, 1]])
    operator = _make_top_n_merge_operator(context)
    if previous_state:
        operator.add_input(make_ref_bundles([[4, 3]])[0], 0)
        _expire_pending_batch(operator)
        run_op_tasks_sync(operator)

        assert operator._merge.output is not None

        bundles.insert(0, operator._merge.output)

    remote_fn = mock.Mock()
    remote_fn.options.return_value.remote.side_effect = RuntimeError("merge failed")

    with (
        mock.patch(
            "ray.data._internal.execution.operators.top_n_operator.cached_remote_fn",
            return_value=remote_fn,
        ),
        mock.patch.object(
            RefBundle, "destroy_if_owned", autospec=True
        ) as destroy_if_owned,
    ):
        for bundle in bundles[int(previous_state) :]:
            operator.add_input(bundle, 0)

        with pytest.raises(RuntimeError):
            if finish_inputs:
                operator.all_inputs_done()
            else:
                _expire_pending_batch(operator)
        operator.refresh_state()

    _assert_owned_bundles_destroyed(destroy_if_owned, bundles)

    assert operator.get_active_tasks() == []
    assert operator.metrics.obj_store_mem_internal_inqueue == 0
    assert not operator.can_add_input()

    remote_fn.options.return_value.remote.assert_called_once()


def test_top_n_merge_cleans_empty_inputs():
    context = DataContext()
    bundle = RefBundle((), schema=None, owns_blocks=True)
    operator = _make_top_n_merge_operator(context)

    with mock.patch.object(
        RefBundle, "destroy_if_owned", autospec=True
    ) as destroy_if_owned:
        operator.add_input(bundle, 0)
        operator.all_inputs_done()

    assert not operator.has_next()
    assert operator.has_completed()

    assert operator.get_stats() == {"TopNMerge(limit=2)": []}

    destroy_if_owned.assert_called_once_with(bundle)


def test_top_n_merge_async_lifecycle(
    ray_start_regular_shared_2_cpus, fixed_merge_clock
) -> None:
    bundles = make_ref_bundles([[3, 1], [2]])
    operator = _make_top_n_merge_operator(DataContext.get_current())

    try:
        assert not operator.has_completed()

        for bundle in bundles:
            operator.add_input(bundle, 0)

        assert not operator.has_next()
        assert operator.get_active_tasks() == []

        with mock.patch("ray.get", side_effect=AssertionError("blocking driver get")):
            operator.all_inputs_done()

        assert not operator.has_execution_finished()
        assert not operator.has_next()
        assert len(operator.get_active_tasks()) == 1
        assert isinstance(operator.get_active_tasks()[0], DataOpTask)

        run_op_tasks_sync(operator)

        # Execution is finished, but completion must wait for output consumption.
        assert operator.has_execution_finished()
        assert not operator.has_completed()
        assert operator.get_active_tasks() == []
        assert operator.has_next()

        output = operator.get_next()
        output_ref = output.block_refs[0]

        assert isinstance(output_ref, ray.ObjectRef)

        block = BlockAccessor.for_block(ray.get(output_ref))

        assert block.to_arrow().to_pydict() == {"id": [1, 2]}
        assert operator.has_completed()

        output.destroy_if_owned()
    finally:
        operator.shutdown(Timer(), force=True)


def test_top_n_merge_releases_inputs_only_after_task_completion(
    ray_start_regular_shared_2_cpus, fixed_merge_clock
) -> None:
    bundles = make_ref_bundles([[3, 1], [2]])
    operator = _make_top_n_merge_operator(DataContext.get_current())

    try:
        for bundle in bundles:
            operator.add_input(bundle, 0)

        with mock.patch.object(
            RefBundle, "destroy_if_owned", autospec=True
        ) as destroy_if_owned:
            operator.all_inputs_done()

            destroy_if_owned.assert_not_called()

            run_op_tasks_sync(operator)

            _assert_owned_bundles_destroyed(destroy_if_owned, bundles)
    finally:
        operator.shutdown(Timer(), force=True)


def test_top_n_merge_tracks_resources_and_metrics(
    ray_start_regular_shared_2_cpus, fixed_merge_clock
) -> None:
    bundles = make_ref_bundles([[3, 1], [2]])
    expected_bytes = sum(bundle.size_bytes() for bundle in bundles)
    operator = _make_top_n_merge_operator(DataContext.get_current())

    try:
        assert not operator.throttling_disabled()

        for bundle in bundles:
            operator.add_input(bundle, 0)

        assert operator.metrics.obj_store_mem_internal_inqueue == expected_bytes
        assert operator.internal_input_queue_num_blocks() == 2

        operator.all_inputs_done()

        assert not operator.throttling_disabled()
        assert operator.current_logical_usage() == ExecutionResources(
            cpu=1, memory=_MERGE_PEAK_MEMORY_MULTIPLIER * expected_bytes
        )

        assert operator.metrics.num_tasks_running == 1

        run_op_tasks_sync(operator)

        assert operator.current_logical_usage() == ExecutionResources.zero()

        assert operator.metrics.num_tasks_finished == 1
        assert operator.metrics.num_task_outputs_generated == 1
        assert operator.metrics.obj_store_mem_pending_task_inputs == 0

        assert operator.internal_output_queue_num_blocks() == 1
        assert operator.num_outputs_total() == 1
        assert operator.num_output_rows_total() == 2

        assert len(operator.get_stats()[operator.name]) == 1

        output = operator.get_next()

        assert (
            operator._block_ref_counter.get_object_store_memory_usage(operator.id)
            == output.size_bytes()
        )
        assert operator.metrics.obj_store_mem_internal_outqueue == 0

        output.destroy_if_owned()
    finally:
        operator.shutdown(Timer(), force=True)


@pytest.mark.parametrize("finish_inputs", [False, True])
def test_top_n_merge_cleans_inputs_on_task_failure(
    ray_start_regular_shared_2_cpus, fixed_merge_clock, finish_inputs: bool
) -> None:
    operator = _make_top_n_merge_operator(
        DataContext.get_current(), sort_key=SortKey("missing")
    )
    bundles = make_ref_bundles([[2, 1], [3]])

    try:
        operator.add_input(bundles[0], 0)
        _expire_pending_batch(operator)

        assert len(operator.get_active_tasks()) == 1

        # Failure must release queued inputs as well as the active task's inputs.
        operator.add_input(bundles[1], 0)

        assert operator.internal_input_queue_num_blocks() == 1
        assert operator.can_add_input()

        if finish_inputs:
            operator.all_inputs_done()

        with mock.patch.object(
            RefBundle, "destroy_if_owned", autospec=True
        ) as destroy_if_owned:
            with pytest.raises(RayTaskError):
                run_op_tasks_sync(operator)

            _assert_owned_bundles_destroyed(destroy_if_owned, bundles)

        operator.refresh_state()

        assert operator.get_active_tasks() == []
        assert operator.has_completed()
        assert not operator.can_add_input()

        assert operator.metrics.num_tasks_submitted == 1
        assert operator.metrics.num_tasks_failed == 1
        assert operator.metrics.obj_store_mem_internal_inqueue == 0
        assert operator.metrics.obj_store_mem_pending_task_inputs == 0

        assert operator.current_logical_usage() == ExecutionResources.zero()

        assert operator.internal_input_queue_num_blocks() == 0
        assert not operator.has_next()
    finally:
        operator.shutdown(Timer(), force=True)


@pytest.mark.parametrize("stage", ["collecting", "running", "state", "output"])
def test_top_n_merge_shutdown_releases_bundles(
    ray_start_regular_shared_2_cpus, fixed_merge_clock, stage: str
) -> None:
    operator = _make_top_n_merge_operator(DataContext.get_current())
    bundle = make_ref_bundles([[2, 1]])[0]
    _advance_merge_to_stage(operator, bundle, stage)

    if stage == "output":
        expected_bundle = operator._output_buffer.to_list()[0]
    elif stage == "state":
        expected_bundle = operator._merge.output
    else:
        expected_bundle = bundle

    assert expected_bundle is not None

    task = operator._merge.task

    with (
        mock.patch.object(
            RefBundle, "destroy_if_owned", autospec=True
        ) as destroy_if_owned,
        mock.patch("ray.cancel", wraps=ray.cancel) as cancel,
    ):
        operator.shutdown(Timer(), force=True)
        operator.shutdown(Timer(), force=True)

        _assert_owned_bundles_destroyed(destroy_if_owned, [expected_bundle])

        if task is not None:
            cancel.assert_called_once_with(
                task.get_waitable(), recursive=True, force=True
            )
        else:
            cancel.assert_not_called()

    assert operator.get_active_tasks() == []
    assert not operator.has_next()

    assert operator.metrics.obj_store_mem_internal_inqueue == 0
    assert operator.metrics.obj_store_mem_internal_outqueue == 0
    assert operator.metrics.obj_store_mem_pending_task_inputs == 0

    assert operator.current_logical_usage() == ExecutionResources.zero()

    if task is not None:
        assert task.has_finished
        assert operator.metrics.num_tasks_running == 0


def test_top_n_merge_preserves_schema_without_candidate_blocks(
    ray_start_regular_shared_2_cpus,
):
    schema = pa.schema([("id", pa.int64())])
    operator = _make_top_n_merge_operator(
        DataContext.get_current(), output_schema=schema
    )
    operator.all_inputs_done()
    operator.refresh_state()
    operator.refresh_state()

    assert operator.get_active_tasks() == []
    assert operator.internal_output_queue_num_blocks() == 1

    output = operator.get_next()

    assert output.schema == schema

    output_ref = output.block_refs[0]

    assert isinstance(output_ref, ray.ObjectRef)
    assert ray.get(output_ref).equals(schema.empty_table())
    assert operator.has_completed()

    output.destroy_if_owned()
    operator.refresh_state()

    assert not operator.has_next()


@pytest.mark.parametrize("stage", ["collecting", "running", "state", "output"])
def test_top_n_downstream_completion_clears_queues_before_shutdown(
    ray_start_regular_shared_2_cpus, fixed_merge_clock, stage: str
) -> None:
    context = DataContext()
    operator = _make_top_n_merge_operator(context)
    downstream = LimitOperator(0, operator, context)
    topology = build_streaming_topology(downstream, ExecutionOptions(), noop_counter())
    bundle = make_ref_bundles([[2, 1]])[0]
    _advance_merge_to_stage(operator, bundle, stage)
    task = operator._merge.task

    try:
        with (
            # Keep upstream open so only the downstream limit triggers completion.
            mock.patch.object(
                operator.input_dependencies[0], "has_completed", return_value=False
            ),
            mock.patch("ray.cancel", wraps=ray.cancel) as cancel,
        ):
            update_operator_states(topology)
            update_operator_states(topology)

            assert operator.has_completed()
            assert not operator.can_add_input()
            assert not operator.has_next()

            assert operator.internal_input_queue_num_blocks() == 0
            assert operator.internal_output_queue_num_blocks() == 0

            # Early completion clears queues; shutdown cancels the active task.
            cancel.assert_not_called()

        if task is not None:
            assert operator.get_active_tasks() == [task]
            assert operator.metrics.obj_store_mem_pending_task_inputs > 0

        operator.refresh_state()
    finally:
        operator.shutdown(Timer(), force=True)

    assert not operator.get_active_tasks()
    assert not operator._merge.inputs
    assert operator._merge.output is None

    assert operator.metrics.obj_store_mem_pending_task_inputs == 0
    assert operator.metrics.num_tasks_running == 0


@pytest.mark.parametrize("threshold", ["blocks", "bytes"])
def test_top_n_incremental_batches_and_backpressure(
    ray_start_regular_shared_2_cpus, threshold: str
) -> None:
    bundles = make_ref_bundles([[value, value - 1] for value in range(20, 0, -2)])
    target = "_MERGE_BATCH_BLOCKS" if threshold == "blocks" else "_MERGE_BATCH_BYTES"
    value = 1 if threshold == "blocks" else bundles[0].size_bytes()
    operator = _make_top_n_merge_operator(DataContext())
    try:
        with mock.patch(
            f"ray.data._internal.execution.operators.top_n_operator.{target}", value
        ):
            operator.add_input(bundles[0], 0)
            first_task = operator.get_active_tasks()[0]
            operator.add_input(bundles[1], 0)

            assert not operator.can_add_input()
            assert operator.get_active_tasks() == [first_task]
            assert not operator.has_next()

            for bundle in bundles[2:]:
                # The done callback can submit another merge; leave it running.
                run_op_tasks_sync(operator, only_existing=True)

                assert operator.can_add_input()
                assert len(operator.get_active_tasks()) == 1

                state_rows = operator._merge.inputs[0].num_rows()

                assert state_rows is not None and state_rows <= 2
                assert not operator.has_next()

                operator.add_input(bundle, 0)

            with mock.patch.object(
                RefBundle, "destroy_if_owned", autospec=True
            ) as destroy:
                run_op_tasks_sync(operator)

                assert mock.call(bundles[-1]) in destroy.call_args_list

            assert operator._merge.output is not None
            assert operator._merge.output.num_rows() == 2

            assert operator.internal_input_queue_num_blocks() == 1
            assert operator.metrics.obj_store_mem_pending_task_inputs == 0

            assert not operator.has_next()
            assert not operator.has_execution_finished()

            with mock.patch(
                "ray.get", side_effect=AssertionError("blocking driver get")
            ):
                operator.all_inputs_done()
            output = operator.get_next()
            output_ref = output.block_refs[0]

            assert isinstance(output_ref, ray.ObjectRef)
            assert ray.get(output_ref)["id"].tolist() == [1, 2]
            assert operator.has_completed()

            assert operator.metrics.num_tasks_finished == len(bundles)
            assert len(operator.get_stats()[operator.name]) == len(bundles)

            output.destroy_if_owned()
    finally:
        operator.shutdown(Timer(), force=True)


def test_top_n_partial_batch_flushes_at_deadline_without_more_input(
    ray_start_regular_shared_2_cpus,
    fixed_merge_clock,
) -> None:
    operator = _make_top_n_merge_operator(DataContext())
    bundle = make_ref_bundles([[3, 1]])[0]
    operator.add_input(bundle, 0)

    assert not operator.get_active_tasks()
    assert operator._pending_since is not None

    with mock.patch(
        "ray.data._internal.execution.operators.top_n_operator.monotonic",
        return_value=operator._pending_since + _MERGE_BATCH_WAIT_S / 2,
    ):
        operator.refresh_state()

        assert not operator.get_active_tasks()

    _expire_pending_batch(operator)

    assert len(operator.get_active_tasks()) == 1

    run_op_tasks_sync(operator)

    assert not operator.has_next()

    operator.all_inputs_done()
    output = operator.get_next()
    output_ref = output.block_refs[0]

    assert isinstance(output_ref, ray.ObjectRef)
    assert ray.get(output_ref)["id"].tolist() == [1, 3]
    assert operator.has_completed()

    output.destroy_if_owned()


@pytest.mark.parametrize("previous_state", [False, True])
@pytest.mark.parametrize("finish_inputs", [False, True])
def test_top_n_failure_after_yield_discards_partial_state(
    ray_start_regular_shared_2_cpus,
    fixed_merge_clock,
    previous_state: bool,
    finish_inputs: bool,
) -> None:
    def fail_after_yield(*args: Any) -> Generator[Union[Block, bytes], None, None]:
        yield from _final_top_n_task(*args)
        raise RuntimeError("failed after yielding")

    operator = _make_top_n_merge_operator(DataContext())
    bundles = make_ref_bundles([[3, 1], [2]])
    if previous_state:
        operator.add_input(make_ref_bundles([[5, 4]])[0], 0)
        _expire_pending_batch(operator)
        run_op_tasks_sync(operator)

        assert operator._merge.output is not None

        bundles.insert(0, operator._merge.output)

    try:
        operator.add_input(bundles[int(previous_state)], 0)
        with mock.patch(
            "ray.data._internal.execution.operators.top_n_operator.cached_remote_fn",
            return_value=ray.remote(fail_after_yield),
        ):
            _expire_pending_batch(operator)

            assert len(operator.get_active_tasks()) == 1

            operator.add_input(bundles[-1], 0)

            assert operator.internal_input_queue_num_blocks() == 1
            assert operator.can_add_input()

            if finish_inputs:
                operator.all_inputs_done()

        with mock.patch.object(RefBundle, "destroy_if_owned", autospec=True) as destroy:
            with pytest.raises(RayTaskError, match="failed after yielding"):
                run_op_tasks_sync(operator)

        for bundle in bundles:
            assert mock.call(bundle) in destroy.call_args_list
        # Also discard the yielded output: it is provisional until task success.
        assert (
            len([c for c in destroy.call_args_list if c.args[0].owns_blocks])
            == len(bundles) + 1
        )

        operator.refresh_state()

        assert not operator.has_next()
        assert not operator.can_add_input()
        assert not operator.get_active_tasks()
        assert operator.has_completed()

        assert operator.internal_input_queue_num_blocks() == 0

        assert operator.metrics.num_tasks_submitted == 1 + int(previous_state)
        assert operator.metrics.num_tasks_failed == 1
        assert operator.metrics.obj_store_mem_internal_inqueue == 0
        assert operator.metrics.obj_store_mem_pending_task_inputs == 0

        assert operator.current_logical_usage() == ExecutionResources.zero()
    finally:
        operator.shutdown(Timer(), force=True)


@pytest.mark.parametrize("termination", ["none", "shutdown", "finished"])
def test_top_n_delayed_metadata_does_not_commit_or_resurrect_state(
    ray_start_regular_shared_2_cpus, termination: str
) -> None:
    release_metadata = threading.Event()

    def get_metadata(refs: Any, **kwargs: Any) -> Any:
        if not release_metadata.wait(timeout=30):
            raise TimeoutError("test did not release merge metadata")
        return ray.get(refs, **kwargs)

    fetcher = ThreadedMetadataFetcher(
        get_objects=get_metadata,
        # Keep the metadata gate off the driver's synchronous size-lookup path.
        get_object_locations=lambda refs: {ref: {"object_size": 1} for ref in refs},
    )
    operator = _make_top_n_merge_operator(DataContext())
    bundle = make_ref_bundles([[2, 1]])[0]
    fetcher.start()
    try:
        operator.add_input(bundle, 0)
        operator.all_inputs_done()
        active_task = operator.get_active_tasks()[0]

        assert isinstance(active_task, DataOpTask)

        task: DataOpTask = active_task

        def defer_output() -> bool:
            task.on_data_ready(None, fetcher)
            fetcher.submit(operator.id, [task])
            return task.has_pending_emits()

        wait_for_condition(defer_output)

        assert not fetcher.emit_ready_and_fire_done_callbacks()

        operator.refresh_state()

        # Worker completion alone cannot commit output with unresolved metadata.
        assert not task.has_finished
        assert operator._merge.output is None
        assert not operator.has_next()
        assert not operator.has_execution_finished()

        if termination == "shutdown":
            operator.shutdown(Timer(), force=True)
        elif termination == "finished":
            operator.mark_execution_finished()
        release_metadata.set()

        def finish_fetch() -> bool:
            assert not fetcher.emit_ready_and_fire_done_callbacks()

            task.on_data_ready(None, fetcher)
            fetcher.submit(operator.id, [task])
            return task.has_finished and not task.has_pending_emits()

        wait_for_condition(finish_fetch)

        if termination != "none":
            assert not operator.has_next()
            assert operator.metrics.num_tasks_failed == int(termination == "shutdown")
        else:
            output = operator.get_next()
            output_ref = output.block_refs[0]

            assert isinstance(output_ref, ray.ObjectRef)
            assert ray.get(output_ref)["id"].tolist() == [1, 2]
            assert operator.has_completed()

            output.destroy_if_owned()

        assert not operator.get_active_tasks()

        assert operator.metrics.num_tasks_finished == 1
        assert operator.metrics.obj_store_mem_pending_task_inputs == 0
    finally:
        release_metadata.set()
        fetcher.stop()
        operator.shutdown(Timer(), force=True)


def test_top_n_partial_dequeue_keeps_pending_deadline(
    ray_start_regular_shared_2_cpus,
    fixed_merge_clock,
) -> None:
    operator = _make_top_n_merge_operator(DataContext())
    bundles = make_ref_bundles([[3], [2], [1]])
    # Two bundles trigger a flush, but only the first fits in the submitted batch.
    with mock.patch(
        "ray.data._internal.execution.operators.top_n_operator._MERGE_BATCH_BYTES",
        2 * bundles[0].size_bytes() - 1,
    ):
        try:
            operator.add_input(bundles[0], 0)
            pending_since = operator._pending_since
            operator.add_input(bundles[1], 0)

            assert operator.get_active_tasks()
            assert operator._pending_since == pending_since
            assert operator._input_buffer.num_blocks() == 1

            with mock.patch(
                "ray.data._internal.execution.operators.top_n_operator.monotonic",
                return_value=10.0,
            ):
                operator.add_input(bundles[2], 0)

                assert operator._pending_since == pending_since

                run_op_tasks_sync(operator)

            assert operator._pending_since is None

            operator.all_inputs_done()
            output = operator.get_next()
            output_ref = output.block_refs[0]

            assert isinstance(output_ref, ray.ObjectRef)
            assert ray.get(output_ref)["id"].tolist() == [1, 2]

            output.destroy_if_owned()
        finally:
            operator.shutdown(Timer(), force=True)


def test_top_n_reserves_memory_for_accumulated_state(
    ray_start_regular_shared_2_cpus,
) -> None:
    operator = _make_top_n_merge_operator(DataContext(), limit=10)
    bundles = make_ref_bundles([[value] for value in range(10, 0, -1)])
    max_block_bytes = max(bundle.size_bytes() for bundle in bundles)
    with mock.patch(
        "ray.data._internal.execution.operators.top_n_operator._MERGE_BATCH_BLOCKS", 1
    ):
        try:
            for bundle in bundles:
                state = operator._merge.output
                state_bytes = state.size_bytes() if state is not None else 0
                operator.add_input(bundle, 0)

                assert (
                    operator.current_logical_usage().memory
                    == _MERGE_PEAK_MEMORY_MULTIPLIER
                    * (state_bytes + bundle.size_bytes())
                )

                run_op_tasks_sync(operator)

            assert operator._merge.output is not None
            assert operator._merge.output.size_bytes() > max_block_bytes

            operator.all_inputs_done()
            output = operator.get_next()
            output_ref = output.block_refs[0]

            assert isinstance(output_ref, ray.ObjectRef)
            assert ray.get(output_ref)["id"].tolist() == list(range(1, 11))

            output.destroy_if_owned()
        finally:
            operator.shutdown(Timer(), force=True)


@pytest.mark.parametrize("block_format", ["arrow", "pandas"])
def test_final_top_n_task_releases_batch_before_fetching_next(
    block_format: str,
) -> None:
    previous_blocks = []
    calls = 0

    def get_blocks(refs: Any) -> List[Block]:
        nonlocal calls
        calls += 1
        if calls == 1:
            blocks = [
                pa.table({"id": values})
                if block_format == "arrow"
                else pd.DataFrame({"id": values})
                for values in ([6, 4], [5, 3])
            ]
            previous_blocks.extend(weakref.ref(block) for block in blocks)
            return blocks

        # Check before the next allocation, not just after the merge returns.
        assert all(block() is None for block in previous_blocks)

        return [pa.table({"id": [1]})]

    with mock.patch("ray.get", side_effect=get_blocks):
        result, _ = list(
            _final_top_n_task(
                [[mock.sentinel.first], [mock.sentinel.second]],
                SortKey("id"),
                2,
                None,
                DataContext(),
            )
        )

    assert BlockAccessor.for_block(result).to_arrow().to_pydict() == {"id": [1, 3]}


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
