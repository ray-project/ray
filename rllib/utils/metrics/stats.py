import numpy as np

from ray.rllib.utils import force_list


class Stats:
    def __init__(self, init_value=None, reduce="mean"):
        self.values = force_list(init_value)
        self._reduce_method = reduce

    def push(self, value):
        self.values.append(value)

    def reduce(self):
        return Stats(
            init_value=getattr(np, self._reduce_method)(self.values),
            reduce=self._reduce_method,
        )
