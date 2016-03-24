from typing import List

import numpy as np
import arrays.single as single
import orchpy as op

from core import *

@op.distributed([List[int]], [DistArray])
def normal(shape):
  num_blocks = DistArray.compute_num_blocks(shape)
  objrefs = np.empty(num_blocks, dtype=object)
  for index in np.ndindex(*num_blocks):
    objrefs[index] = single.random.normal(DistArray.compute_block_shape(index, shape))
  result = DistArray()
  result.construct(shape, objrefs)
  return result
