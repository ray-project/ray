from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import tensorflow as tf

import ray
from ray.rllib.evaluation.policy_graph import PolicyGraph


class KerasPolicyGraph(PolicyGraph):
    def __init__(self, observation_space, action_space, config,
            models=None, targets=None):
        PolicyGraph.__init__(self, observation_space, action_space, config)
        self.models = models or []
        self.targets = targets or []

    def compute_apply(self, batch, *args):
        for model, target in zip(self.models, self.targets):
            model.fit(batch["obs"], batch[target], epochs=1, verbose=0)
        return {}, {}

    def get_weights(self):
        return [model.get_weights() for model in self.models]

    def set_weights(self, weights):
        return [model.set_weights(w) for model, w in zip(self.models, weights)]
