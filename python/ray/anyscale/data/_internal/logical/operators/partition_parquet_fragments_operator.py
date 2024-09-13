from typing import List, Optional

from ray.data._internal.datasource.parquet_datasource import SerializedFragment
from ray.data._internal.logical.interfaces import LogicalOperator
from ray.data.block import BlockMetadata
from ray.data.datasource import PathPartitionFilter


class PartitionParquetFragments(LogicalOperator):
    """Logical operator that partitions input fragments for read tasks.

    The corresponding physical operator accepts inputs and produces outputs of
    serialized Parquet fragments. The operator uses the Parquet metadata to produce
    blocks that represents 128 MiB of read data. For example, if each fragment produces
    64 MiB of read data, the physical operator outputs blocks with two fragments.

    This operator is necessary so that Ray Data launches an appropriate number of read
    tasks.
    """

    def __init__(
        self,
        *,
        serialized_fragments: List[SerializedFragment],
        encoding_ratio: float,
        shuffle: Optional[str] = None,
        filesystem,
        partition_filter: PathPartitionFilter,
    ):
        super().__init__(name="PartitionParquetFragments", input_dependencies=[])

        self.serialized_fragments = serialized_fragments
        self.encoding_ratio = encoding_ratio
        self.shuffle = shuffle
        self.filesystem = filesystem
        self.partition_filter = partition_filter

    def aggregate_output_metadata(self) -> BlockMetadata:
        return BlockMetadata(
            None,
            None,
            None,
            input_files=[f.deserialize().path for f in self.serialized_fragments],
            exec_stats=None,
        )
