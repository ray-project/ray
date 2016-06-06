from typing import List

import numpy as np
import halo.arrays.remote as ra
import halo

from core import *

@halo.remote([List[int]], [DistArray])
def normal(shape):
  num_blocks = DistArray.compute_num_blocks(shape)
  objrefs = np.empty(num_blocks, dtype=object)
  for index in np.ndindex(*num_blocks):
    objrefs[index] = ra.random.normal(DistArray.compute_block_shape(index, shape))
  result = DistArray()
  result.construct(shape, objrefs)
  return result
