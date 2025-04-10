from ray.anyscale.data._internal.logical.operators.list_files_operator import (
    PATH_COLUMN_NAME,
    ListFiles,
)
from ray.anyscale.data._internal.logical.operators.read_files_operator import ReadFiles
from ray.anyscale.data._internal.readers import SupportsRowCounting
from ray.data._internal.logical.interfaces import LogicalPlan, Rule
from ray.data._internal.logical.operators.count_operator import Count
from ray.data._internal.logical.operators.map_operator import MapBatches
from ray.data.block import DataBatch


class PushdownCountFiles(Rule):
    """Optimization rule that pushes down counting to the file reader.

    `FileReader` subclasses can implement `SupportsRowCounting` to efficiently count the
    number of rows in a file. If a `ReadFiles` operator is followed by a `Count`
    operator, this rule replaces the `ReadFiles` with an operator that calls
    `count_rows` and outputs the number of rows. This avoids reading any actual data.
    """

    # NOTE: Default CPU allocation is 1, so we're lowering this to allow
    #       at least 2 tasks to run per CPU core
    _PER_TASK_NUM_CPUS_ALLOCATION = 0.5

    def apply(self, plan: LogicalPlan) -> LogicalPlan:
        count = plan.dag
        if not isinstance(count, Count):
            return plan

        assert len(count.input_dependencies) == 1, len(count.input_dependencies)
        read_files = count.input_dependencies[0]

        if (
            not isinstance(read_files, ReadFiles)
            or not isinstance(read_files.reader, SupportsRowCounting)
            or not read_files.reader.can_count_rows()
            # If `ReadFiles` op was optimized by predicate pushdown, this
            # PushdownCountFiles based on file stats won't work, so skip this rule.
            or read_files.filter_expr is not None
        ):
            return plan

        assert len(read_files.input_dependencies) == 1, len(
            read_files.input_dependencies
        )
        list_files = read_files.input_dependencies[0]

        assert isinstance(list_files, ListFiles), list_files

        def count_rows(batch: DataBatch) -> DataBatch:
            assert PATH_COLUMN_NAME in batch.column_names, batch.column_names
            num_rows = read_files.reader.count_rows(
                batch[PATH_COLUMN_NAME].to_pylist(),
                filesystem=read_files.filesystem,
            )
            return {Count.COLUMN_NAME: [num_rows]}

        count_rows_op = MapBatches(
            list_files,
            count_rows,
            batch_format="pyarrow",
            batch_size=read_files.reader.count_rows_batch_size(),
            zero_copy_batch=True,
            ray_remote_args={"num_cpus": self._PER_TASK_NUM_CPUS_ALLOCATION},
        )

        return LogicalPlan(count_rows_op, plan._context)
