from typing import List

from ray.rllib.utils.annotations import PublicAPI
from ray.rllib.utils.framework import TensorType


@PublicAPI
class ListBatch:
    """Represents a variable-length list of items from extra_spaces.List.

    ListBatches are created when you use extra_spaces.List, and are accessible
    as part of input_dict["obs"] in ModelV2 forward functions.

    Example:
        Suppose the gym space definition was:
            List(List(Box(K), N), M)

        Then in the model forward function, input_dict["obs"] is of type:
            ListBatch(ListBatch(<Tensor shape=(B, M, N, K)>))

        The tensor is accessible via:
            input_dict["obs"].value

        And the actual data lengths via:
            # outer repetition, shape [B], range [0, M]
            input_dict["obs"].lengths
                -and-
            # inner repetition, shape [B, M], range [0, N]
            input_dict["obs"].value.lengths

    Attributes:
        value (Tensor): The padded data tensor of shape [B, max_len, ..., sz],
            where B is the batch dimension, max_len is the max length of this
            list, followed by any number of sub list max lens, followed by the
            actual data size.
        lengths (List[int]): Tensor of shape [B, ...] that represents the
            number of valid items in each list. When the list is nested within
            other lists, there will be extra dimensions for the parent list
            max lens.
        max_len (int): The max number of items allowed in each list.
    """

    def __init__(self, value: TensorType, lengths: List[int], max_len: int):
        self.value = value
        self.lengths = lengths
        self.max_len = max_len

    def __repr__(self):
        return "ListBatch(value={}, lengths={}, max_len={})".format(
            repr(self.value), repr(self.lengths), self.max_len)

    def __str__(self):
        return repr(self)
