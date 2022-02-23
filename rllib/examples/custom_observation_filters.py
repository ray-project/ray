"""Example of a custom observation filter

This example shows:
  - using a custom observation filter

"""
import argparse

import numpy as np
import ray
from ray import tune
from ray.rllib.utils.filter import Filter
from ray.rllib.utils.framework import try_import_tf

tf1, tf, tfv = try_import_tf()

parser = argparse.ArgumentParser()
parser.add_argument(
    "--run", type=str, default="PPO", help="The RLlib-registered algorithm to use."
)
parser.add_argument("--stop-iters", type=int, default=200)


class SimpleRollingStat:
    def __init__(self, n=0, m=0, s=0):
        self._n = n
        self._m = m
        self._s = s

    def copy(self):
        return SimpleRollingStat(self._n, self._m, self._s)

    def push(self, x):
        self._n += 1
        delta = x - self._m
        self._m += delta / self._n
        self._s += delta * delta * (self._n - 1) / self._n

    def update(self, other):
        n1 = self._n
        n2 = other._n
        n = n1 + n2
        if n == 0:
            return

        delta = self._m - other._m
        delta2 = delta * delta

        self._n = n
        self._m = (n1 * self._m + n2 * other._m) / n
        self._s = self._s + other._s + delta2 * n1 * n2 / n

    @property
    def n(self):
        return self._n

    @property
    def mean(self):
        return self._m

    @property
    def var(self):
        return self._s / (self._n - 1) if self._n > 1 else np.square(self._m)

    @property
    def std(self):
        return np.sqrt(self.var)


class CustomFilter(Filter):
    """
    Filter that normalizes by using a single mean
    and std sampled from all obs inputs
    """

    is_concurrent = False

    def __init__(self, shape):
        self.rs = SimpleRollingStat()
        self.buffer = SimpleRollingStat()
        self.shape = shape

    def reset_buffer(self) -> None:
        self.buffer = SimpleRollingStat(self.shape)

    def apply_changes(self, other, with_buffer=False):
        self.rs.update(other.buffer)
        if with_buffer:
            self.buffer = other.buffer.copy()

    def copy(self):
        other = CustomFilter(self.shape)
        other.sync(self)
        return other

    def as_serializable(self):
        return self.copy()

    def sync(self, other):
        assert other.shape == self.shape, "Shapes don't match!"
        self.rs = other.rs.copy()
        self.buffer = other.buffer.copy()

    def __call__(self, x, update=True):
        x = np.asarray(x)
        if update:
            if len(x.shape) == len(self.shape) + 1:
                # The vectorized case.
                for i in range(x.shape[0]):
                    self.push_stats(x[i], (self.rs, self.buffer))
            else:
                # The unvectorized case.
                self.push_stats(x, (self.rs, self.buffer))
        x = x - self.rs.mean
        x = x / (self.rs.std + 1e-8)
        return x

    @staticmethod
    def push_stats(vector, buffers):
        for x in vector:
            for buffer in buffers:
                buffer.push(x)

    def __repr__(self):
        return f"CustomFilter({self.shape}, {self.rs}, {self.buffer})"


if __name__ == "__main__":
    args = parser.parse_args()
    ray.init()

    config = {
        "env": "CartPole-v0",
        "observation_filter": lambda size: CustomFilter(size),
        "num_workers": 0,
    }

    results = tune.run(
        args.run, config=config, stop={"training_iteration": args.stop_iters}
    )

    ray.shutdown()
