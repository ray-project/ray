from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import numpy as np

import ray
from ray.experimental.serve import RayServeMixin, batched_input


@ray.remote
class VectorizedAdder(RayServeMixin):
    """Actor that adds scaler_increment to input batch.

    result = np.array(input_batch) + scaler_increment
    """

    def __init__(self, scaler_increment):
        self.inc = scaler_increment

    @batched_input
    def __call__(self, input_batch):
        arr = np.array(input_batch)
        arr += self.inc
        return arr.tolist()


@ray.remote
class ScalerAdder(RayServeMixin):
    """Actor that adds a scaler_increment to a single input."""

    def __init__(self, scaler_increment):
        self.inc = scaler_increment

    def __call__(self, input_scaler):
        return input_scaler + self.inc


@ray.remote
class VectorDouble(RayServeMixin):
    """Actor that doubles the batched input."""

    @batched_input
    def __call__(self, batched_vectors):
        matrix = np.array(batched_vectors)
        matrix *= 2
        return [v.tolist() for v in matrix]
