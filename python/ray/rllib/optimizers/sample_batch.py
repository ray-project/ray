from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import numpy as np


class SampleBatch(object):
    """Wrapper around a dictionary with string keys and array-like values.

    For example, {"obs": [1, 2, 3], "reward": [0, -1, 1]} is a batch of three
    samples, each with an "obs" and "reward" attribute.
    """

    def __init__(self, *args, **kwargs):
        self.data = dict(*args, **kwargs)
        lengths = []
        for k, v in self.data.copy().items():
            assert type(k) == str, self
            lengths.append(len(v))
            if not isinstance(v, np.ndarray):
                self.data[k] = np.array(v)
        assert len(set(lengths)) == 1, "data columns must be same length"

    def concat(self, other):
        assert self.data.keys() == other.data.keys(), "must have same columns"
        out = {}
        for k in self.data.keys():
            out[k] = np.concatenate([self.data[k], other.data[k]])
        return SampleBatch(out)

    def rows(self):
        num_rows = len(list(self.data.values())[0])
        for i in range(num_rows):
            row = {}
            for k in self.data.keys():
                row[k] = self[k][i]
            yield row

    def __getitem__(self, key):
        return self.data[key]

    def __str__(self):
        return str(self.data)

    def __repr__(self):
        return str(self.data)
