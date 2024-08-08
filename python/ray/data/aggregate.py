from typing import TYPE_CHECKING, Callable, Optional, Union

from ray.data.block import AggType, Block, BlockAccessor, KeyType, T, U
from ray.util.annotations import PublicAPI

if TYPE_CHECKING:
    import pyarrow as pa


@PublicAPI
class AggregateFn:
    """Defines an aggregate function in the accumulator style.

    Aggregates a collection of inputs of type T into
    a single output value of type U.
    See https://www.sigops.org/s/conferences/sosp/2009/papers/yu-sosp09.pdf
    for more details about accumulator-based aggregation.

    Args:
        init: This is called once for each group to return the empty accumulator.
            For example, an empty accumulator for a sum would be 0.
        merge: This may be called multiple times, each time to merge
            two accumulators into one.
        name: The name of the aggregation. This will be used as the column name
            in the output Dataset.
        accumulate_row: This is called once per row of the same group.
            This combines the accumulator and the row, returns the updated
            accumulator. Exactly one of accumulate_row and accumulate_block must
            be provided.
        accumulate_block: This is used to calculate the aggregation for a
            single block, and is vectorized alternative to accumulate_row. This will
            be given a base accumulator and the entire block, allowing for
            vectorized accumulation of the block. Exactly one of accumulate_row and
            accumulate_block must be provided.
        finalize: This is called once to compute the final aggregation
            result from the fully merged accumulator.
    """

    def __init__(
        self,
        init: Callable[[KeyType], AggType],
        merge: Callable[[AggType, AggType], AggType],
        name: str,
        accumulate_row: Callable[[AggType, T], AggType] = None,
        accumulate_block: Callable[[AggType, Block], AggType] = None,
        finalize: Optional[Callable[[AggType], U]] = None,
    ):
        if (accumulate_row is None and accumulate_block is None) or (
            accumulate_row is not None and accumulate_block is not None
        ):
            raise ValueError(
                "Exactly one of accumulate_row or accumulate_block must be provided."
            )
        if accumulate_block is None:

            def accumulate_block(a: AggType, block: Block) -> AggType:
                block_acc = BlockAccessor.for_block(block)
                for r in block_acc.iter_rows(public_row_format=False):
                    a = accumulate_row(a, r)
                return a

        if not isinstance(name, str):
            raise TypeError("`name` must be provided.")

        if finalize is None:
            finalize = lambda a: a  # noqa: E731

        self.init = init
        self.merge = merge
        self.name = name
        self.accumulate_block = accumulate_block
        self.finalize = finalize

    def _validate(self, schema: Optional[Union[type, "pa.lib.Schema"]]) -> None:
        """Raise an error if this cannot be applied to the given schema."""
        pass
