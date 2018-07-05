from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import tensorflow as tf
import numpy as np

import ray
from ray.rllib.evaluation.policy_graph import PolicyGraph


sample = lambda probs: [np.random.choice(len(pr), p=pr) for pr in probs]


class KerasPolicyGraph(PolicyGraph):
    def __init__(self, observation_space, action_space, config,
            actor=None, critic=None):
        PolicyGraph.__init__(self, observation_space, action_space, config)
        self.actor = actor
        self.critic = critic
        self.models = [self.actor, self.critic]

    def compute_actions(self, obs, *args, **kwargs):
        state = np.array(obs)
        policy = self.actor.predict(state)
        value = self.critic.predict(state)
        return sample(policy), [], {"vf_preds": value.flatten()}

    def compute_apply(self, batch, *args):
        self.actor.fit(batch["obs"], batch["adv_targets"], epochs=1, verbose=0)
        self.critic.fit(batch["obs"], batch["value_targets"], epochs=1, verbose=0)
        return {}, {}

    def get_weights(self):
        return [model.get_weights() for model in self.models]

    def set_weights(self, weights):
        return [model.set_weights(w) for model, w in zip(self.models, weights)]
