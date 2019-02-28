from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import numpy as np

from ray.rllib.evaluation.policy_graph import PolicyGraph


def _sample(probs):
    return [np.random.choice(len(pr), p=pr) for pr in probs]


class KerasPolicyGraph(PolicyGraph):
    """Initialize the Keras Policy Graph.

    This is a Policy Graph used for models with actor and critics.
    Note: This class is built for specific usage of Actor-Critic models,
    and is less general compared to TFPolicyGraph and TorchPolicyGraphs.

    Args:
        observation_space (gym.Space): Observation space of the policy.
        action_space (gym.Space): Action space of the policy.
        config (dict): Policy-specific configuration data.
        actor (Model): A model that holds the policy.
        critic (Model): A model that holds the value function.
    """

    def __init__(self,
                 observation_space,
                 action_space,
                 config,
                 actor=None,
                 critic=None):
        PolicyGraph.__init__(self, observation_space, action_space, config)
        self.actor = actor
        self.critic = critic
        self.models = [self.actor, self.critic]

    def compute_actions(self, obs, *args, **kwargs):
        state = np.array(obs)
        policy = self.actor.predict(state)
        value = self.critic.predict(state)
        return _sample(policy), [], {"vf_preds": value.flatten()}

    def learn_on_batch(self, batch, *args):
        self.actor.fit(
            batch["obs"],
            batch["adv_targets"],
            epochs=1,
            verbose=0,
            steps_per_epoch=20)
        self.critic.fit(
            batch["obs"],
            batch["value_targets"],
            epochs=1,
            verbose=0,
            steps_per_epoch=20)
        return {}, {}

    def get_weights(self):
        return [model.get_weights() for model in self.models]

    def set_weights(self, weights):
        return [model.set_weights(w) for model, w in zip(self.models, weights)]
