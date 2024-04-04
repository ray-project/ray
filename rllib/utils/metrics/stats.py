import numpy as np

from ray.rllib.utils import force_list


class Stats:
    def __init__(
        self,
        init_value=None,
        reduce="mean",
        window=None,
        ema_coeff=None,
    ):
        self.values = force_list(init_value)

        self._reduce_method = reduce
        self._window = window
        self._ema_coeff = ema_coeff

    def push(self, value):
        self.values.append(value)

    def peek(self):
        if len(self.values) == 1:
            return self.values[0]
        else:
            return self._reduced_values()

    def reduce(self):
        self.values = [self._reduced_values()]

        # Return self.
        return self

    def _reduced_values(self):
        # Reduce everything.
        if self._window is None:
            return getattr(np, self._reduce_method)(self.values)
        # Reduce only over some window into the past, drop the rest.
        elif isinstance(self._window, int):
            return getattr(np, self._reduce_method)(self.values[-self._window:])
        elif self._ema_coeff is not None:

