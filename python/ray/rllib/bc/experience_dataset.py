from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import itertools
import pickle

import numpy as np


class ExperienceDataset(object):
    def __init__(self, dataset_path):
        self._dataset = list(itertools.chain.from_iterable(pickle.load(open(dataset_path, "rb"))))

    def sample(self, batch_size):
        indexes = np.random.choice(len(self._dataset), batch_size)
        samples = {
            'observations': [self._dataset[i][0] for i in indexes],
            'actions': [self._dataset[i][1] for i in indexes]
        }
        return samples
