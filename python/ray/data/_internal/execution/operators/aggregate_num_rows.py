import ray
from ray.data._internal.delegating_block_builder import DelegatingBlockBuilder
from ray.data._internal.execution.interfaces import PhysicalOperator, RefBundle
from ray.data._internal.stats import StatsDict
from ray.data.block import BlockAccessor
from ray.data.context import DataContext


class AggregateNumRows(PhysicalOperator):
    """Count number of rows in input bundles.

    This operator aggregates the number of rows in input bundles using the bundles'
    block metadata. It outputs a single row with the specified column name.
    """

    def __init__(
        self,
        input_dependencies,
        data_context: DataContext,
        column_name: str,
    ):
        super().__init__(
            "AggregateNumRows",
            input_dependencies,
            data_context,
        )

        self._column_name = column_name

        self._num_rows = 0
        self._has_outputted = False
        self._estimated_num_output_bundles = 1
        self._estimated_output_num_rows = 1

    def has_next(self) -> bool:
        return self._inputs_complete and not self._has_outputted

    def _get_next_inner(self) -> RefBundle:
        assert self._inputs_complete

        builder = DelegatingBlockBuilder()
        builder.add({self._column_name: self._num_rows})
        block = builder.build()
        block_ref = ray.put(block)

        metadata = BlockAccessor.for_block(block).get_metadata()
        schema = BlockAccessor.for_block(block).schema()
        bundle = RefBundle([(block_ref, metadata)], owns_blocks=True, schema=schema)

        self._has_outputted = True
        return bundle

    def get_stats(self) -> StatsDict:
        return {}

    def _add_input_inner(self, refs, input_index) -> None:
        assert refs.num_rows() is not None
        self._num_rows += refs.num_rows()

    def throttling_disabled(self) -> bool:
        return True

    def implements_accurate_memory_accounting(self) -> bool:
        return True
