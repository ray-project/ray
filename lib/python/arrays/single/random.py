from typing import List
import numpy as np
import halo

@halo.remote([List[int]], [np.ndarray])
def normal(shape):
  return np.random.normal(size=shape)
