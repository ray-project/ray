import numpy as np

from ray.rllib.utils import force_list


class Stats:
    def __init__(self, init_value=None, reduction="mean"):
        self.values = [] if init_value is None else [init_value]
        self.reductions = force_list(reduction)

    def push(self, value):
        self.values.append(value)

    def reduce(self):
        return {
            r: getattr(np, r)(self.values) for r in self.reductions
        }
