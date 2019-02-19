import numpy as np
import ray
from ray.serve import RayServeMixin


@ray.remote
class VectorizedAdder(RayServeMixin):
    def __init__(self, scaler_increment):
        self.inc = scaler_increment

    def __call__(self, input_batch):
        arr = np.array(input_batch)
        arr += self.inc
        return arr.tolist()
