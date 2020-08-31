import numpy as np
import ray.experimental.array.remote as ra
import ray

from .core import DistArray


@ray.remote
def normal(shape):
    num_blocks = DistArray.compute_num_blocks(shape)
    object_refs = np.empty(num_blocks, dtype=object)
    for index in np.ndindex(*num_blocks):
        object_refs[index] = ra.random.normal.remote(
            DistArray.compute_block_shape(index, shape))
    result = DistArray(shape, object_refs)
    return result
