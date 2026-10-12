from dataclasses import replace
from typing import Any, Dict, Optional
from unittest import mock

import pyarrow as pa
import pytest

import ray
from ray.data._internal.execution.interfaces import (
    BlockEntry,
    RefBundle,
)
from ray.data._internal.execution.operators.input_data_buffer import InputDataBuffer
from ray.data._internal.execution.operators.top_n_operator import (
    TOP_N_MAX_CANDIDATE_BLOCKS_CONFIG_KEY,
    TOP_N_MAX_CANDIDATE_BYTES_CONFIG_KEY,
    TOP_N_MAX_CANDIDATE_ROWS_CONFIG_KEY,
    TopNMergeOperator,
    _check_candidate_budget,
    _final_top_n_task,
)
from ray.data._internal.execution.util import make_ref_bundles
from ray.data._internal.planner.exchange.sort_task_spec import SortKey
from ray.data.block import BlockMetadata, BlockMetadataWithSchema, Schema
from ray.data.context import DataContext
from ray.data.tests.conftest import *  # noqa: F401, F403
from ray.tests.conftest import *  # noqa: F401, F403


def _make_top_n_merge_operator(
    context: DataContext,
    *,
    sort_key: Optional[SortKey] = None,
    limit: int = 2,
    ray_remote_args: Optional[Dict[str, Any]] = None,
    output_schema: Optional[Schema] = None,
) -> TopNMergeOperator:
    return TopNMergeOperator(
        InputDataBuffer(context, []),
        context,
        sort_key=sort_key or SortKey("id"),
        limit=limit,
        ray_remote_args=ray_remote_args or {},
        output_schema=output_schema,
        name=f"TopNMerge(limit={limit})",
    )


@pytest.mark.parametrize(
    ("config_key", "config_value", "expected_exceeded"),
    [
        (TOP_N_MAX_CANDIDATE_BLOCKS_CONFIG_KEY, 1, "blocks"),
        (TOP_N_MAX_CANDIDATE_BYTES_CONFIG_KEY, 1, "bytes"),
    ],
)
def test_top_n_candidate_budget_checks_blocks_and_bytes(
    ray_start_regular_shared_2_cpus,
    config_key,
    config_value,
    expected_exceeded,
):
    context = DataContext.get_current()
    context.set_config(config_key, config_value)
    bundles = make_ref_bundles([[3, 1], [2]])
    candidate_summary = {"blocks": 0, "rows": 0, "bytes": 0}
    expected_bytes = sum(bundle.size_bytes() for bundle in bundles)

    try:
        with pytest.raises(ValueError, match=rf"exceeded=\['{expected_exceeded}'\]"):
            _check_candidate_budget(bundles, context, candidate_summary)
        assert candidate_summary == {
            "blocks": 2,
            "rows": 3,
            "bytes": expected_bytes,
        }
    finally:
        for bundle in bundles:
            bundle.destroy_if_owned()


@pytest.mark.parametrize("missing_field", ["num_rows", "size_bytes"])
def test_top_n_candidate_budget_rejects_unknown_metadata(missing_field):
    metadata = BlockMetadata(
        num_rows=1,
        size_bytes=1,
        input_files=None,
        exec_stats=None,
    )
    metadata = replace(metadata, **{missing_field: None})
    bundle = mock.Mock(
        spec=RefBundle,
        blocks=(BlockEntry(mock.sentinel.block_ref, metadata),),
    )

    with pytest.raises(ValueError, match="must include num_rows and size_bytes"):
        _check_candidate_budget([bundle], DataContext())


def test_top_n_candidate_budget_rejects_boolean_config():
    context = DataContext()
    context.set_config(TOP_N_MAX_CANDIDATE_ROWS_CONFIG_KEY, True)

    with pytest.raises(ValueError, match="must be a positive integer"):
        _check_candidate_budget([], context)


def test_top_n_merge_reports_metrics_propagates_options_and_cleans_inputs(
    ray_start_regular_shared_2_cpus,
    restore_data_context,
):
    context = DataContext.get_current()
    context.execution_options.label_selector = {
        "subcluster": "train",
        "accelerator": "any",
    }
    sort_key = SortKey("id")
    ray_remote_args = {
        "num_cpus": 2,
        "label_selector": {"accelerator": "cpu"},
    }
    result = pa.table({"id": [1, 2]})
    metadata_with_schema = BlockMetadataWithSchema.from_block(result)
    output_ref = ray.put(result)
    metadata_ref = ray.put(metadata_with_schema)
    bundles = make_ref_bundles([[3, 1], [2]])
    operator = _make_top_n_merge_operator(
        context,
        sort_key=sort_key,
        ray_remote_args=ray_remote_args,
        output_schema=result.schema,
    )

    remote_fn = mock.Mock()
    configured_remote_fn = mock.Mock()
    remote_fn.options.return_value = configured_remote_fn
    configured_remote_fn.remote.return_value = (output_ref, metadata_ref)

    with (
        mock.patch(
            "ray.data._internal.execution.operators.top_n_operator.cached_remote_fn",
            return_value=remote_fn,
        ),
        mock.patch.object(
            RefBundle, "destroy_if_owned", autospec=True
        ) as destroy_if_owned,
    ):
        output_bundles, stats = operator.get_transformation_fn()(
            bundles, mock.sentinel.task_context
        )

    remote_fn.options.assert_called_once_with(
        num_cpus=2,
        label_selector={"subcluster": "train", "accelerator": "cpu"},
    )
    configured_remote_fn.remote.assert_called_once_with(
        [entry.ref for bundle in bundles for entry in bundle.blocks],
        sort_key,
        2,
        result.schema,
        context,
    )
    assert destroy_if_owned.call_args_list == [mock.call(bundle) for bundle in bundles]
    assert operator._extra_metrics() == {
        "top_n_candidate_blocks": 2,
        "top_n_candidate_rows": 3,
        "top_n_candidate_bytes": sum(bundle.size_bytes() for bundle in bundles),
    }
    assert list(stats) == ["TopNMerge(limit=2)"]
    assert len(output_bundles) == 1
    assert output_bundles[0].block_refs == [output_ref]
    output_bundles[0].destroy_if_owned()


def test_final_top_n_task_installs_data_context(
    ray_start_regular_shared_2_cpus,
):
    context = DataContext()
    context.use_polars_sort = True
    block = pa.table({"id": [2, 1]})

    with mock.patch.object(
        DataContext, "current", wraps=DataContext.current
    ) as current:
        result, _ = _final_top_n_task(
            [ray.put(block)],
            SortKey("id"),
            1,
            block.schema,
            context,
        )

    current.assert_called_once_with(context)
    assert result.to_pylist() == [{"id": 1}]


@pytest.mark.parametrize("failure", ["budget", "remote"])
def test_top_n_merge_cleans_inputs_on_failure(
    ray_start_regular_shared_2_cpus,
    restore_data_context,
    failure,
):
    context = DataContext.get_current()
    bundles = make_ref_bundles([[2, 1]])
    operator = _make_top_n_merge_operator(context)

    if failure == "budget":
        context.set_config(TOP_N_MAX_CANDIDATE_ROWS_CONFIG_KEY, 1)
        remote_patch = mock.patch(
            "ray.data._internal.execution.operators.top_n_operator.cached_remote_fn"
        )
        expected_error = ValueError
    else:
        remote_fn = mock.Mock()
        remote_fn.remote.side_effect = RuntimeError("merge failed")
        remote_patch = mock.patch(
            "ray.data._internal.execution.operators.top_n_operator.cached_remote_fn",
            return_value=remote_fn,
        )
        expected_error = RuntimeError

    with (
        remote_patch as cached_remote_fn,
        mock.patch.object(
            RefBundle, "destroy_if_owned", autospec=True
        ) as destroy_if_owned,
        pytest.raises(expected_error),
    ):
        operator.get_transformation_fn()(bundles, mock.sentinel.task_context)

    destroy_if_owned.assert_called_once_with(bundles[0])
    if failure == "budget":
        cached_remote_fn.assert_not_called()


def test_top_n_merge_cleans_empty_inputs():
    context = DataContext()
    bundle = RefBundle((), schema=None, owns_blocks=True)
    operator = _make_top_n_merge_operator(context)

    with mock.patch.object(
        RefBundle, "destroy_if_owned", autospec=True
    ) as destroy_if_owned:
        output_bundles, stats = operator.get_transformation_fn()(
            [bundle], mock.sentinel.task_context
        )

    assert output_bundles == []
    assert stats == {"TopNMerge(limit=2)": []}
    destroy_if_owned.assert_called_once_with(bundle)
