from typing import List
import numpy as np
import halo

@halo.distributed([List[int]], [np.ndarray])
def normal(shape):
  return np.random.normal(size=shape)
