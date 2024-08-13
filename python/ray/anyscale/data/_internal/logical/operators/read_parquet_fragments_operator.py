from typing import Any, Dict, List

from .partition_parquet_fragments_operator import PartitionParquetFragments
from ray.data._internal.logical.interfaces import LogicalOperator
from ray.data._internal.logical.operators.map_operator import AbstractMap
from ray.data.block import BlockMetadata


class ReadParquetFragments(AbstractMap):
    """Logical operator that reads Parquet fragments.

    The corresponding physical operator accepts inputs of serialized Parquet fragments
    and outputs of read Parquet data. To ensure that each read task reads an appropriate
    amount of data, this logical operator should be preceded by the
    ``PartitionParquetFragments`` operator.
    """

    def __init__(
        self,
        input_dependency: LogicalOperator,
        *,
        block_udf,
        to_batches_kwargs,
        default_read_batch_size_rows: int,
        columns: List[str],
        schema,
        include_paths: bool,
        ray_remote_args: Dict[str, Any],
        concurrency: int
    ):
        super().__init__(
            name="ReadParquetFragments",
            input_op=input_dependency,
            ray_remote_args=ray_remote_args,
        )
        self.block_udf = block_udf
        self.to_batches_kwargs = to_batches_kwargs
        self.default_read_batch_size_rows = default_read_batch_size_rows
        self.columns = columns
        self.parquet_schema = schema
        self.include_paths = include_paths
        self.ray_remote_args = ray_remote_args
        self.concurrency = concurrency

    def is_read(self) -> bool:
        return True

    def aggregate_output_metadata(self) -> BlockMetadata:
        assert len(self.input_dependencies) == 1
        assert isinstance(self.input_dependencies[0], PartitionParquetFragments)
        return self.input_dependencies[0].aggregate_output_metadata()
