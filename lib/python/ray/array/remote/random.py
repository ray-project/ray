from typing import List
import numpy as np
import ray

@ray.remote([List], [np.ndarray])
def normal(shape):
  return np.random.normal(size=shape)
