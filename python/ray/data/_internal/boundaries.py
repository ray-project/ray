from typing import Dict, Union

import numpy as np


def get_block_boundaries(
    block: Union[np.ndarray, Dict[str, np.ndarray]],
) -> np.ndarray:
    """Compute block boundaries based on the key(s).

    That is, a list of starting indices of each group and an end index of
    the last group.

    Args:
        block: numpy arrays of the group key(s). This is generally coming from
            ``BlockAccessor.to_numpy()``.

    Returns:
        A list of starting indices of each group. The first entry is 0 and
        the last entry is ``len(array)``.
    """

    if isinstance(block, dict):
        # For multiple keys, we create a numpy record array
        dtype = [(k, v.dtype) for k, v in block.items()]
        block = np.array(list(zip(*block.values())), dtype=dtype)

    return np.hstack([[0], np.where(block[1:] != block[:-1])[0] + 1, [len(block)]])
