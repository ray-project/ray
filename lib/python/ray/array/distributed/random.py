from typing import List

import numpy as np
import ray.array.remote as ra
import ray

from core import *

@ray.remote([List], [DistArray])
def normal(shape):
  num_blocks = DistArray.compute_num_blocks(shape)
  objrefs = np.empty(num_blocks, dtype=object)
  for index in np.ndindex(*num_blocks):
    objrefs[index] = ra.random.normal(DistArray.compute_block_shape(index, shape))
  result = DistArray()
  result.construct(shape, objrefs)
  return result
