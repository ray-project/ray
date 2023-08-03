import numpy as np

import ray


@ray.remote
def normal(shape):
    return np.random.normal(size=shape)
