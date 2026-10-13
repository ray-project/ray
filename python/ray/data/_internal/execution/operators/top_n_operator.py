from typing import Any, Dict, List, Optional, Tuple

import ray
from ray.data._internal.delegating_block_builder import DelegatingBlockBuilder
from ray.data._internal.execution.interfaces import (
    BlockEntry,
    PhysicalOperator,
    RefBundle,
    TaskContext,
)
from ray.data._internal.execution.operators.base_physical_operator import (
    AllToAllOperator,
)
from ray.data._internal.execution.util import merge_label_selector
from ray.data._internal.pandas_block import PandasBlockSchema
from ray.data._internal.planner.exchange.sort_task_spec import SortKey
from ray.data._internal.remote_fn import cached_remote_fn
from ray.data._internal.table_block import TableBlockAccessor
from ray.data.block import (
    Block,
    BlockAccessor,
    BlockExecStats,
    BlockMetadataWithSchema,
    Schema,
    to_stats,
)
from ray.data.context import DataContext
from ray.types import ObjectRef

TOP_N_MAX_CANDIDATE_BLOCKS_CONFIG_KEY = "top_n_max_candidate_blocks"
TOP_N_MAX_CANDIDATE_ROWS_CONFIG_KEY = "top_n_max_candidate_rows"
TOP_N_MAX_CANDIDATE_BYTES_CONFIG_KEY = "top_n_max_candidate_bytes"

DEFAULT_TOP_N_MAX_CANDIDATE_BLOCKS = 4_096
DEFAULT_TOP_N_MAX_CANDIDATE_ROWS = 2_000_000
DEFAULT_TOP_N_MAX_CANDIDATE_BYTES = 256 * 1024 * 1024


# TODO(vicwen): Replace this blocking AllToAll wrapper with a native
# asynchronous PhysicalOperator.
class TopNMergeOperator(AllToAllOperator):
    """Final TopN merge with fixed-size candidate summary metrics."""

    def __init__(
        self,
        input_op: PhysicalOperator,
        data_context: DataContext,
        *,
        sort_key: SortKey,
        limit: int,
        ray_remote_args: Dict[str, Any],
        output_schema: Optional[Schema],
        name: str,
    ):
        self._candidate_summary = {"blocks": 0, "rows": 0, "bytes": 0}
        bulk_fn = _make_final_top_n_bulk_fn(
            sort_key,
            limit,
            ray_remote_args,
            output_schema,
            data_context,
            self._candidate_summary,
        )
        super().__init__(
            bulk_fn,
            input_op,
            data_context,
            num_outputs=1,
            name=name,
        )

    def _extra_metrics(self) -> Dict[str, Any]:
        return {
            "top_n_candidate_blocks": self._candidate_summary["blocks"],
            "top_n_candidate_rows": self._candidate_summary["rows"],
            "top_n_candidate_bytes": self._candidate_summary["bytes"],
        }


def _empty_block_from_schema(schema: Schema) -> Block:
    """Create an empty Pandas or Arrow block that preserves ``schema``."""
    import pandas as pd
    import pyarrow as pa

    if isinstance(schema, PandasBlockSchema):
        return pd.DataFrame(
            {
                name: pd.Series(dtype=dtype)
                for name, dtype in zip(schema.names, schema.types)
            }
        )
    assert isinstance(schema, pa.Schema), type(schema)
    return schema.empty_table()


def _final_top_n_task(
    candidate_refs: List[ObjectRef[Block]],
    sort_key: SortKey,
    limit: int,
    output_schema: Optional[Schema],
    data_context: DataContext,
) -> Tuple[Block, BlockMetadataWithSchema]:
    """Merge sorted candidate blocks and return the global Top-N result."""
    blocks: List[Block] = ray.get(candidate_refs)
    if not blocks:
        raise ValueError("TopN final merge received no candidate blocks.")
    with DataContext.current(data_context):
        blocks = TableBlockAccessor.normalize_block_types(blocks)

        if output_schema is not None and all(
            BlockAccessor.for_block(block).num_rows() == 0 for block in blocks
        ):
            result = _empty_block_from_schema(output_schema)
        else:
            builder = DelegatingBlockBuilder()
            for block in blocks:
                builder.add_block(block)
            candidates = builder.build()
            result = BlockAccessor.for_block(candidates).top_n(sort_key, limit)

    return result, BlockMetadataWithSchema.from_block(
        result,
        block_exec_stats=BlockExecStats.builder().build(),
    )


def _get_positive_config(context: DataContext, key: str, default: int) -> int:
    """Read a strictly positive integer configuration value from the context."""
    value = context.get_config(key, default)
    if type(value) is not int or value <= 0:
        raise ValueError(
            f"TopN config {key!r} must be a positive integer, got {value!r}."
        )
    return value


def _check_candidate_budget(
    input_bundles: List[RefBundle],
    data_context: DataContext,
    candidate_summary: Optional[Dict[str, int]] = None,
) -> List[BlockEntry]:
    """Validate that candidates fit within the final single-task merge budget."""
    entries = [entry for bundle in input_bundles for entry in bundle.blocks]
    max_blocks = _get_positive_config(
        data_context,
        TOP_N_MAX_CANDIDATE_BLOCKS_CONFIG_KEY,
        DEFAULT_TOP_N_MAX_CANDIDATE_BLOCKS,
    )
    max_rows = _get_positive_config(
        data_context,
        TOP_N_MAX_CANDIDATE_ROWS_CONFIG_KEY,
        DEFAULT_TOP_N_MAX_CANDIDATE_ROWS,
    )
    max_bytes = _get_positive_config(
        data_context,
        TOP_N_MAX_CANDIDATE_BYTES_CONFIG_KEY,
        DEFAULT_TOP_N_MAX_CANDIDATE_BYTES,
    )

    candidate_rows = 0
    candidate_bytes = 0
    for entry in entries:
        metadata = entry.metadata
        if metadata.num_rows is None or metadata.size_bytes is None:
            raise ValueError(
                "TopN candidate metadata must include num_rows and size_bytes "
                "before the final merge."
            )
        candidate_rows += metadata.num_rows
        candidate_bytes += metadata.size_bytes

    actual = {
        "blocks": len(entries),
        "rows": candidate_rows,
        "bytes": candidate_bytes,
    }
    if candidate_summary is not None:
        candidate_summary.update(actual)
    limits = {
        "blocks": max_blocks,
        "rows": max_rows,
        "bytes": max_bytes,
    }
    exceeded = [name for name in actual if actual[name] > limits[name]]
    if exceeded:
        # TODO(vicwen): Replace this fail-closed single-task budget with a
        # bounded hierarchical or distributed overflow path.
        raise ValueError(
            "TopN candidates exceed the experimental single-task merge budget: "
            f"actual={actual}, limits={limits}, exceeded={exceeded}."
        )

    return entries


def _make_final_top_n_bulk_fn(
    sort_key: SortKey,
    limit: int,
    ray_remote_args: Dict[str, Any],
    output_schema: Optional[Schema],
    data_context: DataContext,
    candidate_summary: Dict[str, int],
):
    """Create the all-to-all callback that validates and merges Top-N candidates."""

    def bulk_fn(
        input_bundles: List[RefBundle],
        _task_context: TaskContext,
    ):
        try:
            entries = _check_candidate_budget(
                input_bundles, data_context, candidate_summary
            )
            if not entries:
                if output_schema is None:
                    return [], {f"TopNMerge(limit={limit})": []}
                result = _empty_block_from_schema(output_schema)
                metadata_with_schema = BlockMetadataWithSchema.from_block(result)
                result_ref: ObjectRef[Block] = ray.put(result)
                output_bundle = RefBundle(
                    (BlockEntry(result_ref, metadata_with_schema.metadata),),
                    owns_blocks=True,
                    schema=metadata_with_schema.schema,
                )
                return [output_bundle], {
                    f"TopNMerge(limit={limit})": to_stats([metadata_with_schema])
                }

            final_task = cached_remote_fn(_final_top_n_task, num_returns=2)
            remote_args = merge_label_selector(
                ray_remote_args,
                data_context.execution_options.label_selector,
            )
            if remote_args:
                final_task = final_task.options(**remote_args)

            output_ref: ObjectRef[Block]
            metadata_ref: ObjectRef[BlockMetadataWithSchema]
            output_ref, metadata_ref = final_task.remote(
                [entry.ref for entry in entries],
                sort_key,
                limit,
                output_schema,
                data_context,
            )
            metadata_with_schema: BlockMetadataWithSchema = ray.get(metadata_ref)
            output_bundle = RefBundle(
                (BlockEntry(output_ref, metadata_with_schema.metadata),),
                owns_blocks=True,
                schema=metadata_with_schema.schema,
            )

            return [output_bundle], {
                f"TopNMerge(limit={limit})": to_stats([metadata_with_schema])
            }
        finally:
            for bundle in input_bundles:
                bundle.destroy_if_owned()

    return bulk_fn
