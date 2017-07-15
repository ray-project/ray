from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import numpy as np
import ray


@ray.remote
def normal(shape):
    return np.random.normal(size=shape)
