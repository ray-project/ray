from ray.data._internal.block_list import BlockList
from ray.data._internal.plan import AllToAllStage
from ray.data._internal.split import _split_at_index


class LimitStage(AllToAllStage):
    """Implementation of `Dataset.limit()`."""

    def __init__(self, limit: int):
        self._limit = limit
        super().__init__(
            "Limit",
            None,
            self._do_limit,
        )

    @property
    def limit(self) -> int:
        return self._limit

    def _do_limit(
        self,
        input_block_list: BlockList,
        clear_input_blocks: bool,
        *_,
    ):
        if clear_input_blocks:
            block_list = input_block_list.copy()
            input_block_list.clear()
        else:
            block_list = input_block_list
        block_list = block_list.truncate_by_rows(self._limit)
        blocks, metadata, _, _ = _split_at_index(block_list, self._limit)
        return (
            BlockList(
                blocks,
                metadata,
                owned_by_consumer=block_list._owned_by_consumer,
            ),
            {},
        )
