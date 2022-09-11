import torch
from typing import List
from rllib2.models.types import TensorType, TensorDict, Spec, SpecDict
from rllib.models.torch.torch_action_dist import TorchCategorical, TorchDistributionWrapper, TorchDiagGaussian

# TODO: We should utilize
# https://github.com/ray-project/ray/blob/master/rllib/models/torch/torch_action_dist.py
# with the following changes:
# 1. Remove the TorchModelV2 argument from all constructors
# 2. Make an abstraction class for Torch/TF2/Jax
# 3. We should decide whether sample/deterministic_sample or behavioral_sample/target_sample
# is better naming. IMO since we might use distributions outside of actions, I think
# the sample/deterministic_sample naming might be more reasonable

# TODO: How do we bake exploration strategy into the distribution?
# E.g., Categorical + Epsilon Greedy vs Categorical + Boltzmann sampling?
# Should we have separate classes that inherit from PiCategorical? Or should
# this be a configuration setting passed to PiCategorical?
class PiDistribution:
    # all sampling operations preserve the backpropagation. So if that's not intended
    # user needs to wrap the method call in with torch.no_grad()

    # TODO: Similar to above, do we want configs for each distribution,
    # e.g. NormalDist(free_log_std=False), NormalDist(free_log_std=True)
    # or do we want two different distribution classes 
    # e.g. class NormalDistWithFreeLogStd(NormalPiDist), class NormalDistWithFrozenLogStd(NormalPiDist)
    def __init__(self, inputs: TensorDict, config: PiDistributionConfig=None):
        pass

    def behavioral_sample(self, shape):
        raise NotImplementedError()

    def target_sample(self, shape):
        raise NotImplementedError()

    def log_prob(self, value):
        raise NotImplementedError()

    def entropy(self):
        raise NotImplementedError()

    def kl_divergence(self):
        raise NotImplementedError()

    def input_spec(self) -> SpecDict:
        """Input spec for init"""
        pass

        


class DeterministicDist(PiDistribution):
    # TODO: Shouldn't this return the argmax or something?
    # how does returning the logits provide a "sample"?
    def behavioral_sample(self, shape):
        return self.logits

    def target_sample(self, shape):
        return self.logits

    def entropy(self):
        return torch.zeros_like(self.logits)


class SquashedDeterministicDist(DeterministicDist):
    def behavioral_sample(self, shape):
        return super().behavioral_sample(shape).tanh()

    def target_sample(self, shape):
        return super().target_sample(shape).tanh()


# Problem is we pass a class to the Pi, not an instance
# so PiDistributionDict cannot be a class, but rather a function that constructs
# a class definition
def combine_distributions(dist_mapping: Mapping[str, PiDistribution]):
    class PiDistributionDict(PiDistribution):
        dist_mapping: Mapping[str, PiDistribution] = dist_mapping

class PiDistributionDict(PiDistribution):
    def __init__(self, dist_mapping: Mapping[str, PiDistribution]):
        self._dist_mapping = dist_mapping

    def behavioral_sample(self, shape):
        samples = {}
        for key, dist in self._dist_mapping.items():
            samples[key] = dist.behavioral_sample(shape)
        return samples

    def target_sample(self, shape):
        samples = {}
        for key, dist in self._dist_mapping.items():
            samples[key] = dist.target_sample(shape)
        return samples

    def log_prob(self, value):
        log_probs = {}
        for key, dist in self._dist_mapping.items():
            log_probs[key] = dist.log_prob(value[key])
        return log_probs

    def entropy(self):
        entropies = {}
        for key, dist in self._dist_mapping.items():
            entropies[key] = dist.entropy()
        return entropies
