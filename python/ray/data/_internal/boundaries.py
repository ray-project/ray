from typing import Dict, Union

import numpy as np


def get_key_boundaries(
    keys: Union[np.ndarray, Dict[str, np.ndarray]], include_first: bool = True
) -> np.ndarray:
    """Compute block boundaries based on the key(s), that is, a list of
    starting indices of each group and a end index of the last group.

    Args:
        keys: numpy arrays of the group key(s).
        include_first: Whether to include the first index (0).

    Returns:
        A list of starting indices of each group. The first entry is 0 and
        the last entry is ``len(array)``.
    """

    if isinstance(keys, dict):
        # For multiple keys, we create a numpy record array
        dtype = [(k, v.dtype) for k, v in keys.items()]
        keys = np.array(list(zip(*keys.values())), dtype=dtype)

    if include_first:
        return np.hstack([[0], np.where(keys[1:] != keys[:-1])[0] + 1, [len(keys)]])
    else:
        return np.hstack([np.where(keys[1:] != keys[:-1])[0] + 1, [len(keys)]])
