# Code in this file is copied and adapted from
# https://github.com/ray-project/ray/tree/master/python/ray/rllib/es

import ray
import numpy as np

def create_shared_noise():
    """
    Create a large array of noise to be shared by all workers. Used 
    for avoiding the communication of the random perturbations delta.
    """

    seed = 12345
    count = 250000000
    noise = np.random.RandomState(seed).randn(count).astype(np.float64)
    return noise


class SharedNoiseTable(object):
    def __init__(self, noise, seed = 11):

        self.rg = np.random.RandomState(seed)
        self.noise = noise
        assert self.noise.dtype == np.float64

    def get(self, i, dim):
        return self.noise[i:i + dim]

    def sample_index(self, dim):
        return self.rg.randint(0, len(self.noise) - dim + 1)

    def get_delta(self, dim):
        idx = self.sample_index(dim)
        return idx, self.get(idx, dim)
