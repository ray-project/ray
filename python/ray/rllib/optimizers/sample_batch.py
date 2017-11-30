from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import numpy as np


class SampleBatch(dict):
    """Wrapper around a dictionary with string keys and array-like values.

    For example, {"obs": [1, 2, 3], "reward": [0, -1, 1]} is a batch of three
    samples, each with an "obs" and "reward" attribute.
    """

    def __init__(self, *args, **kwargs):
        dict.__init__(self, *args, **kwargs)
        lengths = []
        for k, v in self.items():
            assert type(k) == str, self
            lengths.append(len(v))
            if not isinstance(v, np.ndarray):
                self[k] = np.array(v)
        assert len(set(lengths)) == 1, self

    def concat(self, other):
        assert self.keys() == other.keys(), (self.keys(), other.keys())
        out = {}
        for k in self.keys():
            out[k] = np.concatenate([self[k], other[k]])
        return out

    def rows(self):
        num_rows = len(list(self.values())[0])
        for i in range(num_rows):
            row = {}
            for k in self.keys():
                row[k] = self[k][i]
            yield row
