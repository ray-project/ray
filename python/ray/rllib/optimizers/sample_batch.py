from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import numpy as np


class SampleBatch(dict):
    """Wrapper around a dictionary with string keys and array-like values.
    
    For example, {"obs": [1, 2, 3], "reward": [0, -1, 1]} is a batch of three
    samples, each with an "obs" and "reward" attribute.
    """

    def concat(self, other):
        assert self.keys() == other.keys(), (self.keys(), other.keys())
        out = {}
        for k in self.keys():
            out[k] = np.concatenate([self[k], other[k]])
        return out

    def rows(self):
        num_rows = len(list(self.keys())[0])
        for i in range(num_rows):
            row = {}
            for k in self.keys():
                row[k] = self[k][i]
            yield row
