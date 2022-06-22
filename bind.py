import ray
from ray import serve

import numpy as np


@ray.remote
def download(n):
    return np.arange(n)


@serve.deployment(num_replicas=10)
class MyModel:
    def __init__(self, arr):
        self._arr = arr

    def __call__(self, *args):
        return len(self._arr)


m = MyModel.bind(download.bind(1000000000))
