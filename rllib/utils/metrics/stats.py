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

        # One or both window and ema must be None.
        assert window is None or ema_coeff is None

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
        # Reduce everything to a single (init) value.
        if self._reduce_method is not None:
            self.values = [self._reduced_values()]
        # Return self.
        return self

    def _reduced_values(self):
        # No reduction. Return list as-is.
        if self._reduce_method is None:
            return self.values
        # Do EMA.
        elif self._ema_coeff is not None:
            mean_value = self.values[0]
            for v in self.values[1:]:
                mean_value = self._ema_coeff * v + (1.0 - self._ema_coeff) * mean_value
            return mean_value
        # Do some other reduction.
        else:
            reduce_meth = getattr(np, self._reduce_method)
            values = self.values if self._window is None else self.values[-self._window:]
            return reduce_meth(values)
