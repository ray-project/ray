import json
import numpy as np
import numbers


class SafeFallbackEncoder(json.JSONEncoder):
    def __init__(self, nan_str="null", **kwargs):
        super(SafeFallbackEncoder, self).__init__(**kwargs)
        self.nan_str = nan_str

    def default(self, value):
        try:
            if type(value).__module__ == np.__name__ and isinstance(value, np.ndarray):
                return value.tolist()

            if isinstance(value, np.bool_):
                return bool(value)

            if np.isnan(value):
                return self.nan_str

            if issubclass(type(value), numbers.Integral):
                return int(value)
            if issubclass(type(value), numbers.Number):
                return float(value)

            return super(SafeFallbackEncoder, self).default(value)

        except Exception:
            return str(value)  # give up, just stringify it (ok for logs)
