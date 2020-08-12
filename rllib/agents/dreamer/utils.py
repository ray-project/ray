import random
from ray.rllib.utils.framework import try_import_torch

torch, nn = try_import_torch()
class SampleDist(object):

  def __init__(self, dist, samples=100):
    self._dist = dist
    self._samples = samples

  def __getattr__(self, name):
    return getattr(self._dist, name)

  def mean(self):
    samples = self._dist.sample(self._samples)
    return torch.mean(samples, dim=0)

  def mode(self):
    sample = self._dist.sample(self._samples)
    logprob = self._dist.log_prob(sample)
    return tf.gather(sample, tf.argmax(logprob))[0]

  def entropy(self):
    sample = self._dist.sample(self._samples)
    logprob = self._dist.log_prob(sample)
    return -torch.mean(logprob, 0)