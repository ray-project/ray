import numpy as np
import ray
from ray.serve import RayServeMixin, single_input


@ray.remote
class VectorizedAdder(RayServeMixin):
    def __init__(self, scaler_increment):
        self.inc = scaler_increment

    def __call__(self, input_batch):
        arr = np.array(input_batch)
        arr += self.inc
        return arr.tolist()


@ray.remote
class ScalerAdder(RayServeMixin):
    def __init__(self, scaler_increment):
        self.inc = scaler_increment

    @single_input
    def __call__(self, input_scaler):
        return input_scaler + self.inc
