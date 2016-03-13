from typing import List
import numpy as np
import orchpy as op

@op.distributed([List[int]], [np.ndarray])
def normal(shape):
  return np.random.normal(size=shape)
