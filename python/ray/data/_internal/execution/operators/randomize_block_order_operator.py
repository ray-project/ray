from collections import deque
from random import Random
from typing import Deque, List, Optional

from ray.data._internal.execution.interfaces import PhysicalOperator, RefBundle
from ray.data._internal.execution.operators.base_physical_operator import (
    OneToOneOperator,
)
from ray.data.block import BlockMetadata

from ray.data._internal.stats import StatsDict

NEW_DATA_TO_SHUFFLE_RATIO = 0.3


class RandomizeBlockOrderOperator(OneToOneOperator):
    def __init__(
        self,
        input_op: PhysicalOperator,
        window_size: Optional[int],
        seed: Optional[int],
    ):
        self._name = (
            f"RandomizeBlockOrderOperator(window_size={window_size}, seed={seed})"
        )
        super().__init__(self._name, input_op)
        self._window_size = window_size
        self._random = Random(seed)
        self._out_buffer: List[RefBundle] = []
        self._output_metadata: List[BlockMetadata] = []
        self._num_outputs_total = input_op.num_outputs_total()
        self._num_new_blocks_since_last_shuffle = 0

    def add_input(self, refs: RefBundle, input_index: int) -> None:
        assert not self.completed()
        assert input_index == 0, input_index
        self._out_buffer.append(refs)
        self._num_new_blocks_since_last_shuffle += 1

    def all_inputs_done(self) -> None:
        super().all_inputs_done()
        self._random.shuffle(self._out_buffer)

    def has_next(self) -> bool:
        if self._window_size is None:
            return self._inputs_complete and len(self._out_buffer) > 0
        else:
            if self._inputs_complete:
                return len(self._out_buffer) > 0
            else:
                return len(self._out_buffer) >= self._window_size

    def _maybe_shuffle(self):
        if self._window_size is None:
            return
        if (
            self._num_new_blocks_since_last_shuffle
            >= self._window_size * NEW_DATA_TO_SHUFFLE_RATIO
        ):
            self._random.shuffle(self._out_buffer)
            self._num_new_blocks_since_last_shuffle = 0

    def get_next(self) -> RefBundle:
        self._maybe_shuffle()
        index = self._random.randint(0, len(self._out_buffer) - 1)
        res = self._out_buffer.pop(index)
        self._output_metadata.extend([meta for _, meta in res.blocks])
        return res

    def get_stats(self) -> StatsDict:
        return {self._name: self._output_metadata}

    def num_outputs_total(self) -> Optional[int]:
        return self._num_outputs_total
