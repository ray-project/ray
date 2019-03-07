import numpy as np

import ray
from ray.experimental.serve import RayServeMixin, single_input


@ray.remote
class VectorizedAdder(RayServeMixin):
    """Actor that adds scaler_increment to input batch

    result = np.array(input_batch) + scaler_increment
    """

    def __init__(self, scaler_increment):
        self.inc = scaler_increment

    def __call__(self, input_batch):
        arr = np.array(input_batch)
        arr += self.inc
        return arr.tolist()


@ray.remote
class ScalerAdder(RayServeMixin):
    """Actor that adds scaler_increment to single input, using @single_input decorator

    result = [inp + scaler_increment for inp in input_batch]
    """

    def __init__(self, scaler_increment):
        self.inc = scaler_increment

    @single_input
    def __call__(self, input_scaler):
        return input_scaler + self.inc


@ray.remote
class VectorDouble(RayServeMixin):
    """Actor that doubles the batched input,
       in this case the batched input can be N-Dimensional.

    result = input_batch * 2
    """

    def __call__(self, batched_vectors):
        matrix = np.array(batched_vectors)
        matrix *= 2
        return [v.tolist() for v in matrix]
