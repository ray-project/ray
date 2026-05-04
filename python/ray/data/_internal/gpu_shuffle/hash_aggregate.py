import logging
import pickle
import time
from collections import defaultdict
from dataclasses import dataclass
from typing import Any, Dict, Iterator, List, Optional, Sequence, Tuple

import ray
import ray.exceptions
from ray.actor import ActorHandle
from ray.data._internal.execution.interfaces import PhysicalOperator
from ray.data._internal.execution.operators.hash_shuffle import (
    _get_total_cluster_resources,
)
from ray.data._internal.gpu_shuffle.hash_shuffle import (
    GPUShuffleOperator,
    _GPU_PARTITION_ID_KEY,
    _wait_for_refs_with_timeout,
)
from ray.data._internal.table_block import TableBlockAccessor
from ray.data.aggregate import AggregateFn, AggregateFnV2, Count, Max, Mean, Min, Sum
from ray.data.block import Block, BlockAccessor, BlockExecStats, BlockMetadataWithSchema
from ray.data.context import DataContext

logger = logging.getLogger(__name__)


_GLOBAL_AGGREGATE_KEY = "__ray_gpu_hash_aggregate_global_key"


def _resolve_aggregation_names(aggregation_fns: Sequence[AggregateFn]) -> List[str]:
    """Resolve duplicate aggregate names the same way TableBlockAccessor does."""

    counts: Dict[str, int] = defaultdict(int)
    resolved_names: List[str] = []

    for agg in aggregation_fns:
        name = agg.name
        if counts[name] > 0:
            name = TableBlockAccessor._munge_conflict(name, counts[name])
        counts[agg.name] += 1
        resolved_names.append(name)

    return resolved_names


def _groupby(df, key_columns: Tuple[str, ...]):
    try:
        return df.groupby(list(key_columns), dropna=False)
    except TypeError:
        # Older cuDF releases did not accept the dropna keyword.
        return df.groupby(list(key_columns))


def _rename_last_column(df, name: str):
    return df.rename(columns={df.columns[-1]: name})


def _group_size(df, key_columns: Tuple[str, ...], output_column: str):
    grouped = _groupby(df, key_columns)
    return _rename_last_column(grouped.size().reset_index(), output_column)


def _group_count(
    df,
    key_columns: Tuple[str, ...],
    target_column: str,
    output_column: str,
):
    grouped = _groupby(df, key_columns)
    return _rename_last_column(grouped[target_column].count().reset_index(), output_column)


def _group_aggregate(
    df,
    key_columns: Tuple[str, ...],
    target_column: str,
    aggregate_name: str,
    output_column: str,
):
    grouped = _groupby(df, key_columns)
    column_group = grouped[target_column]

    if aggregate_name == "sum":
        try:
            aggregated = column_group.sum(min_count=1)
        except (TypeError, NotImplementedError):
            aggregated = column_group.sum()
    else:
        aggregated = getattr(column_group, aggregate_name)()

    return _rename_last_column(aggregated.reset_index(), output_column)


def _merge_on_keys(left, right, key_columns: Tuple[str, ...]):
    return left.merge(right, on=list(key_columns), how="outer")


def _empty_dataframe(cudf, columns: Sequence[str]):
    df = cudf.DataFrame()
    for column in columns:
        df[column] = []
    return df


class GPUAggregationSpec:
    """GPU aggregation contract for custom ``AggregateFnV2`` implementations.

    A custom ``AggregateFnV2`` can opt in to GPU hash aggregation by implementing
    either ``_gpu_aggregate_spec(output_name, accumulator_prefix)`` or
    ``to_gpu_aggregate_spec(output_name, accumulator_prefix)`` and returning an
    instance of this class.
    """

    output_name: str
    required_columns: Tuple[str, ...]
    accumulator_columns: Tuple[str, ...]

    def partial_aggregate(self, df, key_columns: Tuple[str, ...]):
        """Return a cuDF DataFrame with key columns and accumulator columns."""
        raise NotImplementedError

    def final_aggregate(self, df, key_columns: Tuple[str, ...]):
        """Return a cuDF DataFrame with key columns and the final output column."""
        raise NotImplementedError

    def empty_global_partial_values(self) -> Dict[str, Any]:
        """Return accumulator values for an empty global-aggregation input block."""
        return {column: None for column in self.accumulator_columns}


@dataclass
class _BuiltinGPUAggregationSpec(GPUAggregationSpec):
    kind: str
    output_name: str
    target_column: Optional[str]
    ignore_nulls: bool
    accumulator_prefix: str

    @property
    def required_columns(self) -> Tuple[str, ...]:
        return (self.target_column,) if self.target_column is not None else tuple()

    @property
    def accumulator_columns(self) -> Tuple[str, ...]:
        if self.kind == "mean":
            return (
                f"{self.accumulator_prefix}_sum",
                f"{self.accumulator_prefix}_count",
                f"{self.accumulator_prefix}_null_count",
            )
        return (f"{self.accumulator_prefix}_value",)

    def partial_aggregate(self, df, key_columns: Tuple[str, ...]):
        if self.kind == "count":
            return self._partial_count(df, key_columns)
        if self.kind == "mean":
            return self._partial_mean(df, key_columns)
        return self._partial_simple_reduction(df, key_columns)

    def empty_global_partial_values(self) -> Dict[str, Any]:
        if self.kind == "count":
            return {self.accumulator_columns[0]: 0}
        if self.kind == "sum":
            return {self.accumulator_columns[0]: None if self.ignore_nulls else 0}
        if self.kind == "min":
            return {
                self.accumulator_columns[0]: (
                    None if self.ignore_nulls else float("+inf")
                )
            }
        if self.kind == "max":
            return {
                self.accumulator_columns[0]: (
                    None if self.ignore_nulls else float("-inf")
                )
            }
        if self.kind == "mean":
            sum_col, count_col, null_count_col = self.accumulator_columns
            return {sum_col: None, count_col: 0, null_count_col: 0}
        return super().empty_global_partial_values()

    def final_aggregate(self, df, key_columns: Tuple[str, ...]):
        if self.kind == "count":
            return self._final_count(df, key_columns)
        if self.kind == "mean":
            return self._final_mean(df, key_columns)
        return self._final_simple_reduction(df, key_columns)

    def _partial_count(self, df, key_columns: Tuple[str, ...]):
        acc_col = self.accumulator_columns[0]
        if self.target_column is None or not self.ignore_nulls:
            return _group_size(df, key_columns, acc_col)
        return _group_count(df, key_columns, self.target_column, acc_col)

    def _final_count(self, df, key_columns: Tuple[str, ...]):
        acc_col = self.accumulator_columns[0]
        result = _group_aggregate(df, key_columns, acc_col, "sum", self.output_name)
        return result[list(key_columns) + [self.output_name]]

    def _partial_simple_reduction(self, df, key_columns: Tuple[str, ...]):
        assert self.target_column is not None

        acc_col = self.accumulator_columns[0]
        size_col = f"{self.accumulator_prefix}_size"
        count_col = f"{self.accumulator_prefix}_count"

        result = _group_aggregate(
            df,
            key_columns,
            self.target_column,
            self.kind,
            acc_col,
        )
        counts = _group_count(df, key_columns, self.target_column, count_col)
        sizes = _group_size(df, key_columns, size_col)
        result = _merge_on_keys(result, counts, key_columns)
        result = _merge_on_keys(result, sizes, key_columns)

        if self.ignore_nulls:
            null_mask = result[count_col] == 0
        else:
            null_mask = result[size_col] != result[count_col]
        result.loc[null_mask, acc_col] = None

        return result[list(key_columns) + [acc_col]]

    def _final_simple_reduction(self, df, key_columns: Tuple[str, ...]):
        acc_col = self.accumulator_columns[0]
        size_col = f"{self.accumulator_prefix}_partial_size"
        count_col = f"{self.accumulator_prefix}_partial_count"

        result = _group_aggregate(df, key_columns, acc_col, self.kind, self.output_name)
        counts = _group_count(df, key_columns, acc_col, count_col)
        sizes = _group_size(df, key_columns, size_col)
        result = _merge_on_keys(result, counts, key_columns)
        result = _merge_on_keys(result, sizes, key_columns)

        if self.ignore_nulls:
            null_mask = result[count_col] == 0
        else:
            null_mask = result[size_col] != result[count_col]
        result.loc[null_mask, self.output_name] = None

        return result[list(key_columns) + [self.output_name]]

    def _partial_mean(self, df, key_columns: Tuple[str, ...]):
        assert self.target_column is not None

        sum_col, count_col, null_count_col = self.accumulator_columns
        size_col = f"{self.accumulator_prefix}_size"

        result = _group_aggregate(
            df,
            key_columns,
            self.target_column,
            "sum",
            sum_col,
        )
        counts = _group_count(df, key_columns, self.target_column, count_col)
        sizes = _group_size(df, key_columns, size_col)
        result = _merge_on_keys(result, counts, key_columns)
        result = _merge_on_keys(result, sizes, key_columns)
        result[null_count_col] = result[size_col] - result[count_col]

        if not self.ignore_nulls:
            result.loc[result[null_count_col] > 0, sum_col] = None

        return result[list(key_columns) + list(self.accumulator_columns)]

    def _final_mean(self, df, key_columns: Tuple[str, ...]):
        sum_col, count_col, null_count_col = self.accumulator_columns
        final_sum_col = f"{self.accumulator_prefix}_final_sum"
        final_count_col = f"{self.accumulator_prefix}_final_count"
        final_null_count_col = f"{self.accumulator_prefix}_final_null_count"

        sums = _group_aggregate(df, key_columns, sum_col, "sum", final_sum_col)
        counts = _group_aggregate(df, key_columns, count_col, "sum", final_count_col)
        null_counts = _group_aggregate(
            df, key_columns, null_count_col, "sum", final_null_count_col
        )

        result = _merge_on_keys(sums, counts, key_columns)
        result = _merge_on_keys(result, null_counts, key_columns)
        result[self.output_name] = result[final_sum_col] / result[final_count_col]

        null_mask = result[final_count_col] == 0
        if not self.ignore_nulls:
            null_mask = null_mask | (result[final_null_count_col] > 0)
        result.loc[null_mask, self.output_name] = None

        return result[list(key_columns) + [self.output_name]]


class GPUAggregationPlan:
    """Executable GPU aggregation plan shared by the driver and GPU actors."""

    def __init__(
        self,
        key_columns: Tuple[str, ...],
        specs: Tuple[GPUAggregationSpec, ...],
    ):
        self._key_columns = key_columns
        self._specs = specs
        self._is_global = len(key_columns) == 0
        global_key = _GLOBAL_AGGREGATE_KEY
        required_columns = {
            column for spec in specs for column in spec.required_columns
        }
        while global_key in required_columns:
            global_key = f"_{global_key}"
        self._shuffle_key_columns = (
            (global_key,) if self._is_global else key_columns
        )

    @property
    def shuffle_key_columns(self) -> Tuple[str, ...]:
        return self._shuffle_key_columns

    @property
    def output_names(self) -> Tuple[str, ...]:
        return tuple(spec.output_name for spec in self._specs)

    @property
    def required_columns(self) -> Tuple[str, ...]:
        columns = list(self._key_columns)
        for spec in self._specs:
            for column in spec.required_columns:
                if column not in columns:
                    columns.append(column)
        return tuple(columns)

    @property
    def accumulator_columns(self) -> Tuple[str, ...]:
        columns: List[str] = []
        for spec in self._specs:
            columns.extend(spec.accumulator_columns)
        return tuple(columns)

    def partial_aggregate(self, df):
        import cudf

        if self._is_global:
            df = df.copy(deep=False)
            df[self._shuffle_key_columns[0]] = 0

        key_columns = self._shuffle_key_columns
        if len(df) == 0:
            if self._is_global:
                values: Dict[str, List[Any]] = {key_columns[0]: [0]}
                for spec in self._specs:
                    for column, value in spec.empty_global_partial_values().items():
                        values[column] = [value]
                return cudf.DataFrame(values)[
                    list(key_columns) + list(self.accumulator_columns)
                ]
            return _empty_dataframe(cudf, list(key_columns) + list(self.accumulator_columns))

        result = None
        for spec in self._specs:
            partial = spec.partial_aggregate(df, key_columns)
            result = partial if result is None else _merge_on_keys(result, partial, key_columns)

        assert result is not None
        return result[list(key_columns) + list(self.accumulator_columns)]

    def final_aggregate(self, df):
        import cudf

        key_columns = self._shuffle_key_columns
        output_columns = ([] if self._is_global else list(key_columns)) + list(
            self.output_names
        )

        if len(df) == 0:
            return _empty_dataframe(cudf, output_columns)

        result = None
        for spec in self._specs:
            finalized = spec.final_aggregate(df, key_columns)
            result = (
                finalized
                if result is None
                else _merge_on_keys(result, finalized, key_columns)
            )

        assert result is not None
        if self._is_global:
            result = result.drop(columns=[self._shuffle_key_columns[0]])

        return result[output_columns]


def _get_custom_gpu_aggregation_spec(
    agg: AggregateFn,
    *,
    output_name: str,
    accumulator_prefix: str,
) -> Optional[GPUAggregationSpec]:
    factory = getattr(agg, "_gpu_aggregate_spec", None) or getattr(
        agg, "to_gpu_aggregate_spec", None
    )
    if factory is None:
        return None

    spec = factory(output_name=output_name, accumulator_prefix=accumulator_prefix)
    if not isinstance(spec, GPUAggregationSpec):
        raise TypeError(
            f"{type(agg).__name__} returned {type(spec).__name__} from its GPU "
            "aggregation spec factory. Expected GPUAggregationSpec."
        )
    return spec


def _get_builtin_gpu_aggregation_spec(
    agg: AggregateFn,
    *,
    output_name: str,
    accumulator_prefix: str,
) -> Optional[GPUAggregationSpec]:
    if not isinstance(agg, AggregateFnV2):
        return None

    target_column = agg.get_target_column()
    ignore_nulls = agg._ignore_nulls

    if isinstance(agg, Count):
        return _BuiltinGPUAggregationSpec(
            "count",
            output_name=output_name,
            target_column=target_column,
            ignore_nulls=ignore_nulls,
            accumulator_prefix=accumulator_prefix,
        )

    if target_column is None:
        return None

    if isinstance(agg, Sum):
        kind = "sum"
    elif isinstance(agg, Min):
        kind = "min"
    elif isinstance(agg, Max):
        kind = "max"
    elif isinstance(agg, Mean):
        kind = "mean"
    else:
        return None

    return _BuiltinGPUAggregationSpec(
        kind,
        output_name=output_name,
        target_column=target_column,
        ignore_nulls=ignore_nulls,
        accumulator_prefix=accumulator_prefix,
    )


def build_gpu_aggregation_plan(
    key_columns: Tuple[str, ...],
    aggregation_fns: Tuple[AggregateFn, ...],
) -> Optional[GPUAggregationPlan]:
    if not aggregation_fns:
        return None

    resolved_names = _resolve_aggregation_names(aggregation_fns)
    specs: List[GPUAggregationSpec] = []

    for index, (agg, output_name) in enumerate(zip(aggregation_fns, resolved_names)):
        accumulator_prefix = f"__ray_gpu_agg_{index}"
        spec = _get_custom_gpu_aggregation_spec(
            agg,
            output_name=output_name,
            accumulator_prefix=accumulator_prefix,
        )
        if spec is None:
            spec = _get_builtin_gpu_aggregation_spec(
                agg,
                output_name=output_name,
                accumulator_prefix=accumulator_prefix,
            )
        if spec is None:
            return None
        specs.append(spec)

    return GPUAggregationPlan(key_columns, tuple(specs))


def supports_gpu_hash_aggregate(aggregation_fns: Tuple[AggregateFn, ...]) -> bool:
    return build_gpu_aggregation_plan(tuple(), aggregation_fns) is not None


@ray.remote(num_gpus=1)
class GPUHashAggregateActor:
    """One GPU rank for hash shuffle plus aggregate."""

    def __init__(
        self,
        nranks: int,
        total_nparts: int,
        aggregation_plan: GPUAggregationPlan,
        rmm_pool_size: Any = None,
        spill_memory_limit: Any = "auto",
    ):
        from ray.data._internal.gpu_shuffle.rapidsmpf_backend import (
            BulkRapidsMPFShuffler,
        )

        self._aggregation_plan = aggregation_plan
        self._shuffler = BulkRapidsMPFShuffler(
            nranks=nranks,
            total_nparts=total_nparts,
            shuffle_on=list(aggregation_plan.shuffle_key_columns),
            rmm_pool_size=rmm_pool_size,
            spill_memory_limit=spill_memory_limit,
        )
        self._shuffle_columns: Optional[List[str]] = None

    def setup_root(self) -> Tuple[int, bytes]:
        logger.info("UCXX setup_root starting on GPU hash aggregate rank 0.")
        t0 = time.perf_counter()
        result = self._shuffler.setup_root()
        elapsed = time.perf_counter() - t0
        logger.info(
            "UCXX setup_root completed in %.2fs for GPU hash aggregate rank %d.",
            elapsed,
            result[0],
        )
        return result

    def setup_worker(self, root_address: bytes) -> None:
        logger.info(
            "UCXX setup_worker starting for GPU hash aggregate "
            "(root_address=%d bytes).",
            len(root_address),
        )
        t0 = time.perf_counter()
        self._shuffler.setup_worker(root_address)
        elapsed = time.perf_counter() - t0
        logger.info("UCXX setup_worker completed in %.2fs.", elapsed)

    def insert_batch(self, block: Block) -> int:
        import cudf

        table = BlockAccessor.for_block(block).to_arrow()
        df = cudf.DataFrame.from_arrow(table)

        required_columns = self._aggregation_plan.required_columns
        if required_columns:
            df = df[list(required_columns)]

        partial = self._aggregation_plan.partial_aggregate(df)
        if self._shuffle_columns is None:
            self._shuffle_columns = list(partial.columns)

        self._shuffler.insert_chunk(table=partial, column_names=self._shuffle_columns)
        return table.num_rows

    def finish_and_extract(self) -> Iterator:
        self._shuffler.insert_finished()

        import cudf
        from rapidsmpf.utils.cudf import pylibcudf_to_cudf_dataframe

        self._shuffle_columns = self._shuffle_columns or list(
            self._aggregation_plan.shuffle_key_columns
            + self._aggregation_plan.accumulator_columns
        )

        for partition_id, partition in self._shuffler.extract():
            exec_stats_builder = BlockExecStats.builder()
            if partition.num_columns() == 0:
                cdf = cudf.DataFrame()
            else:
                cdf = pylibcudf_to_cudf_dataframe(
                    partition, column_names=self._shuffle_columns
                ).copy(deep=True)

            output_df = self._aggregation_plan.final_aggregate(cdf)
            block = output_df.to_arrow(preserve_index=False)

            existing_metadata = block.schema.metadata or {}
            tagged_schema = block.schema.with_metadata(
                {**existing_metadata, _GPU_PARTITION_ID_KEY: str(partition_id).encode()}
            )
            exec_stats = exec_stats_builder.build()
            stats = yield block
            if stats:
                object.__setattr__(
                    exec_stats, "block_ser_time_s", stats.object_creation_dur_s
                )
            block_meta = BlockMetadataWithSchema.from_block(
                block, block_exec_stats=exec_stats
            )
            bm = BlockMetadataWithSchema.from_metadata(
                block_meta.metadata, schema=tagged_schema
            )
            yield pickle.dumps(bm)


class GPUHashAggregateRankPool:
    def __init__(
        self,
        nranks: int,
        total_nparts: int,
        aggregation_plan: GPUAggregationPlan,
        rmm_pool_size: Any,
        spill_memory_limit: Any,
        setup_timeout_s: float,
    ):
        self._nranks = nranks
        self._total_nparts = total_nparts
        self._aggregation_plan = aggregation_plan
        self._rmm_pool_size = rmm_pool_size
        self._spill_memory_limit = spill_memory_limit
        self._setup_timeout_s = setup_timeout_s
        self._actors: List[ActorHandle] = []
        self._shutdown = False

    @property
    def is_shutdown(self) -> bool:
        return self._shutdown

    @property
    def nranks(self) -> int:
        return self._nranks

    @property
    def actors(self) -> List[ActorHandle]:
        return self._actors

    def start(self) -> None:
        timeout = self._setup_timeout_s
        t_start = time.perf_counter()

        logger.info(
            "GPUHashAggregateRankPool: creating %d actor(s) (total_nparts=%d).",
            self._nranks,
            self._total_nparts,
        )
        self._actors = [
            GPUHashAggregateActor.options(
                num_gpus=1,
                scheduling_strategy="SPREAD",
            ).remote(
                nranks=self._nranks,
                total_nparts=self._total_nparts,
                aggregation_plan=self._aggregation_plan,
                rmm_pool_size=self._rmm_pool_size,
                spill_memory_limit=self._spill_memory_limit,
            )
            for _ in range(self._nranks)
        ]

        remaining = max(0, timeout - (time.perf_counter() - t_start))
        try:
            _, root_address_bytes = ray.get(
                self._actors[0].setup_root.remote(), timeout=remaining
            )
        except ray.exceptions.GetTimeoutError:
            raise TimeoutError(
                "UCXX setup_root on GPU hash aggregate rank 0 did not complete "
                f"within {timeout}s. Check GPU/network health."
            )

        remaining = max(0, timeout - (time.perf_counter() - t_start))
        worker_refs = [
            actor.setup_worker.remote(root_address_bytes) for actor in self._actors
        ]
        _wait_for_refs_with_timeout(worker_refs, remaining, "setup_worker")

    def get_actor_for_block(self, block_idx: int) -> ActorHandle:
        return self._actors[block_idx % self._nranks]

    def shutdown(self, force: bool = False) -> None:
        if force:
            for actor in self._actors:
                ray.kill(actor)
        self._actors.clear()
        self._shutdown = True


def _derive_num_gpu_ranks(data_context: DataContext) -> int:
    if data_context.gpu_shuffle_num_actors is not None:
        return data_context.gpu_shuffle_num_actors

    total_resources = _get_total_cluster_resources()
    num_gpus = int(total_resources.gpu or 0)
    if num_gpus == 0:
        raise RuntimeError(
            "ShuffleStrategy.GPU_SHUFFLE requires GPU resources in the cluster. "
            "Set DataContext.gpu_shuffle_num_actors to override the number of ranks."
        )
    return num_gpus


class GPUHashAggregateOperator(GPUShuffleOperator):
    """GPU-native hash aggregate using RAPIDS MPF for the shuffle stage."""

    def __init__(
        self,
        data_context: DataContext,
        input_op: PhysicalOperator,
        key_columns: Tuple[str, ...],
        aggregation_fns: Tuple[AggregateFn, ...],
        *,
        num_partitions: Optional[int] = None,
    ):
        aggregation_plan = build_gpu_aggregation_plan(key_columns, aggregation_fns)
        if aggregation_plan is None:
            raise ValueError("GPUHashAggregateOperator received unsupported aggregations.")

        nranks = _derive_num_gpu_ranks(data_context)
        target_num_partitions = (
            num_partitions
            if len(key_columns) > 0 and num_partitions is not None
            else data_context.default_hash_shuffle_parallelism
        )
        target_num_partitions = max(target_num_partitions, nranks)

        super().__init__(
            input_op,
            data_context,
            key_columns=aggregation_plan.shuffle_key_columns,
            columns=None,
            num_partitions=target_num_partitions,
            should_sort=False,
        )

        self._name = (
            f"GPUHashAggregate(key_columns={key_columns}, "
            f"num_partitions={target_num_partitions})"
        )
        self._aggregation_plan = aggregation_plan
        self._rank_pool = GPUHashAggregateRankPool(
            nranks=nranks,
            total_nparts=target_num_partitions,
            aggregation_plan=aggregation_plan,
            rmm_pool_size=data_context.gpu_shuffle_rmm_pool_size,
            spill_memory_limit=data_context.gpu_shuffle_spill_memory_limit,
            setup_timeout_s=data_context.gpu_shuffle_setup_timeout_s,
        )

    def get_sub_progress_bar_names(self) -> List[str]:
        return ["GPU Shuffle", "GPU Aggregation"]

    def set_sub_progress_bar(self, name: str, pg: "BaseProgressBar") -> None:
        if name == "GPU Shuffle":
            self._shuffle_bar = pg
        elif name == "GPU Aggregation":
            self._reduce_bar = pg

    def get_stats(self):
        shuffle_name = f"{self._name}_shuffle"
        aggregate_name = f"{self._name}_aggregate"
        return {
            shuffle_name: self._shuffled_blocks_stats,
            aggregate_name: self._output_blocks_stats,
        }
