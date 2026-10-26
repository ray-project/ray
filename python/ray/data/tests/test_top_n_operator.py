import pickle
from typing import Any, Dict, List, Optional
from unittest import mock

import pandas as pd
import pyarrow as pa
import pytest

import ray
from ray.data._internal.execution.interfaces import (
    BlockEntry,
    ExecutionOptions,
    ExecutionResources,
    RefBundle,
)
from ray.data._internal.execution.interfaces.physical_operator import DataOpTask
from ray.data._internal.execution.operators.input_data_buffer import InputDataBuffer
from ray.data._internal.execution.operators.top_n_operator import (
    TopNMergeOperator,
    _batch_candidates,
    _final_top_n_task,
)
from ray.data._internal.execution.util import make_ref_bundles
from ray.data._internal.planner.exchange.sort_task_spec import SortKey
from ray.data._internal.stats import Timer
from ray.data.block import (
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


def test_batch_candidates_splits_by_bytes():
    refs = [mock.sentinel.a, mock.sentinel.b, mock.sentinel.c, mock.sentinel.d]
    entries = [
        BlockEntry(
            ref,
            BlockMetadata(
                num_rows=1, size_bytes=size, input_files=None, exec_stats=None
            ),
        )
        for ref, size in zip(refs, [40, 30, 50, 10])
    ]

    with mock.patch(
        "ray.data._internal.execution.operators.top_n_operator._MERGE_BATCH_BYTES",
        80,
    ):
        batches, merge_bytes = _batch_candidates(entries)

    assert batches == [refs[:2], refs[2:]]
    # Largest batch (70) plus the running Top-N (at most the largest block, 50).
    assert merge_bytes == 120


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
    max_block_bytes = max(bundle.size_bytes() for bundle in bundles)
    expected_resources = {
        "memory": 2 * (expected_bytes + max_block_bytes),
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


def test_top_n_merge_cleans_inputs_on_failure(
    ray_start_regular_shared_2_cpus,
    restore_data_context,
):
    context = DataContext.get_current()
    bundles = make_ref_bundles([[2, 1]])
    operator = _make_top_n_merge_operator(context)
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
        for bundle in bundles:
            operator.add_input(bundle, 0)
        with pytest.raises(RuntimeError):
            operator.all_inputs_done()

    destroy_if_owned.assert_called_once_with(bundles[0])
    assert operator.get_active_tasks() == []
    assert operator.metrics.obj_store_mem_internal_inqueue == 0


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


def test_top_n_merge_async_lifecycle(ray_start_regular_shared_2_cpus):
    context = DataContext.get_current()
    bundles = make_ref_bundles([[3, 1], [2]])
    operator = _make_top_n_merge_operator(context)
    assert operator.throttling_disabled()
    assert not operator.has_completed()
    for bundle in bundles:
        operator.add_input(bundle, 0)
    expected_bytes = sum(bundle.size_bytes() for bundle in bundles)
    assert operator.metrics.obj_store_mem_internal_inqueue == expected_bytes
    assert operator.internal_input_queue_num_blocks() == 2
    assert not operator.has_next()
    assert operator.get_active_tasks() == []

    with (
        mock.patch.object(
            RefBundle, "destroy_if_owned", autospec=True
        ) as destroy_if_owned,
        mock.patch("ray.get", side_effect=AssertionError("blocking driver get")),
    ):
        operator.all_inputs_done()
        destroy_if_owned.assert_not_called()

    assert not operator.throttling_disabled()
    assert not operator.has_execution_finished()
    assert not operator.has_next()
    assert len(operator.get_active_tasks()) == 1
    assert isinstance(operator.get_active_tasks()[0], DataOpTask)
    max_block_bytes = max(bundle.size_bytes() for bundle in bundles)
    assert operator.current_logical_usage() == ExecutionResources(
        cpu=1, memory=2 * (expected_bytes + max_block_bytes)
    )
    assert operator.metrics.num_tasks_running == 1
    assert operator._extra_metrics() == {
        "top_n_candidate_blocks": 2,
        "top_n_candidate_rows": 3,
        "top_n_candidate_bytes": expected_bytes,
    }

    with mock.patch.object(
        RefBundle, "destroy_if_owned", autospec=True
    ) as destroy_if_owned:
        run_op_tasks_sync(operator)
        _assert_owned_bundles_destroyed(destroy_if_owned, bundles)

    assert operator.has_execution_finished()
    assert not operator.has_completed()
    assert operator.get_active_tasks() == []
    assert operator.current_logical_usage() == ExecutionResources.zero()
    assert operator.metrics.num_tasks_finished == 1
    assert operator.metrics.num_task_outputs_generated == 1
    assert operator.metrics.obj_store_mem_pending_task_inputs == 0
    assert operator.internal_output_queue_num_blocks() == 1
    assert operator.num_outputs_total() == 1
    assert operator.num_output_rows_total() == 2

    output = operator.get_next()
    assert (
        operator._block_ref_counter.get_object_store_memory_usage(operator.id)
        == output.size_bytes()
    )

    output_ref = output.block_refs[0]
    assert isinstance(output_ref, ray.ObjectRef)

    block = BlockAccessor.for_block(ray.get(output_ref))
    assert block.to_arrow().to_pydict() == {"id": [1, 2]}
    assert len(operator.get_stats()[operator.name]) == 1
    assert operator.metrics.obj_store_mem_internal_outqueue == 0
    assert operator.has_completed()
    output.destroy_if_owned()


def test_top_n_merge_cleans_inputs_on_task_failure(ray_start_regular_shared_2_cpus):
    operator = _make_top_n_merge_operator(
        DataContext.get_current(), sort_key=SortKey("missing")
    )
    bundles = make_ref_bundles([[2, 1]])
    operator.add_input(bundles[0], 0)
    operator.all_inputs_done()

    with mock.patch.object(
        RefBundle, "destroy_if_owned", autospec=True
    ) as destroy_if_owned:
        with pytest.raises(RayTaskError):
            run_op_tasks_sync(operator)
        _assert_owned_bundles_destroyed(destroy_if_owned, bundles)

    assert operator.get_active_tasks() == []
    assert operator.metrics.num_tasks_failed == 1
    assert operator.metrics.obj_store_mem_pending_task_inputs == 0
    assert operator.current_logical_usage() == ExecutionResources.zero()
    assert not operator.has_next()


@pytest.mark.parametrize("stage", ["collecting", "running", "output"])
def test_top_n_merge_shutdown_releases_bundles(ray_start_regular_shared_2_cpus, stage):
    operator = _make_top_n_merge_operator(DataContext.get_current())
    bundle = make_ref_bundles([[2, 1]])[0]
    operator.add_input(bundle, 0)
    if stage != "collecting":
        operator.all_inputs_done()
    if stage == "output":
        run_op_tasks_sync(operator)

    expected_bundle = (
        operator._output_buffer.to_list()[0] if stage == "output" else bundle
    )
    task = operator._merge_task
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

    assert operator.get_active_tasks() == []
    output = operator.get_next()
    assert output.schema == schema
    output_ref = output.block_refs[0]
    assert isinstance(output_ref, ray.ObjectRef)
    assert ray.get(output_ref).equals(schema.empty_table())
    assert operator.has_completed()
    output.destroy_if_owned()


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
