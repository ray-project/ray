import sys

import numpy as np


def numpy_size(array: np.ndarray) -> int:
    """Calculate the size of a numpy ndarray."""
    total_size = array.nbytes
    if array.dtype == object:
        for item in array.flat:
            total_size += sys.getsizeof(item)
    return total_size
