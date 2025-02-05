from ray.anyscale.data._internal.logical.operators.list_files_operator import (
    PATH_COLUMN_NAME,
)
from ray.anyscale.data._internal.logical.operators.partition_files_operator import (
    PartitionFiles,
)
from ray.anyscale.data._internal.logical.operators.read_files_operator import ReadFiles
from ray.data._internal.logical.interfaces import LogicalPlan, Rule
from ray.data._internal.logical.operators.count_operator import Count
from ray.data._internal.logical.operators.map_operator import MapBatches
from ray.data.block import DataBatch


class PushdownCountFiles(Rule):
    """Optimization rule that pushes down counting to the file reader.

    `FileReader` subclasses can implement `count_rows` to efficiently count the number
    of rows in a file. If a `ReadFiles` operator is followed by a `Count` operator, this
    rule replaces the `ReadFiles` with an operator that calls `count_rows` and outputs
    the number of rows. This avoids reading any actual data.
    """

    def apply(self, plan: LogicalPlan) -> LogicalPlan:
        count = plan.dag
        if not isinstance(count, Count):
            return plan

        assert len(count.input_dependencies) == 1, len(count.input_dependencies)
        read_files = count.input_dependencies[0]

        # If `ReadFiles` op was optimized by predicate pushdown, this
        # PushdownCountFiles based on file stats won't work, so skip this rule.
        if (
            not isinstance(read_files, ReadFiles)
            or read_files.filter_expr is not None
            or not read_files.reader.supports_count_rows()
        ):
            return plan

        assert len(read_files.input_dependencies) == 1, len(
            read_files.input_dependencies
        )
        partition_files = read_files.input_dependencies[0]
        assert isinstance(partition_files, PartitionFiles), partition_files

        def count_rows(batch: DataBatch) -> DataBatch:
            assert PATH_COLUMN_NAME in batch.column_names, batch.column_names
            num_rows = read_files.reader.count_rows(
                batch[PATH_COLUMN_NAME].to_pylist(),
                filesystem=read_files.filesystem,
            )
            return {Count.COLUMN_NAME: [num_rows]}

        count_rows_op = MapBatches(
            partition_files,
            count_rows,
            batch_format="pyarrow",
            batch_size=None,
            zero_copy_batch=True,
        )

        return LogicalPlan(count_rows_op, plan._context)
