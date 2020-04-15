# Code in this file is copied and adapted from
# https://github.com/openai/evolution-strategies-starter.

import gym
import numpy as np

import ray
import ray.experimental.tf_utils
from ray.rllib.agents.es.es import DEFAULT_CONFIG
from ray.rllib.evaluation.sampler import _unbatch_tuple_actions
from ray.rllib.models import ModelCatalog
from ray.rllib.policy.torch_policy_template import build_torch_policy
from ray.rllib.utils.filter import get_filter
from ray.rllib.utils.framework import try_import_torch

torch, _ = try_import_torch()


class GenericPolicy(TorchPolicy):
    def __init__(self, sess, action_space, obs_space, preprocessor,
                 observation_filter, model_options, action_noise_std):
        self.action_space = action_space
        self.action_noise_std = action_noise_std
        self.preprocessor = preprocessor
        self.observation_filter = get_filter(observation_filter,
                                             self.preprocessor.shape)
        #self.inputs = tf.placeholder(tf.float32,
        #                             [None] + list(self.preprocessor.shape))

        # Policy network.
        self.dist_class, self.dist_dim = ModelCatalog.get_action_dist(
            self.action_space, model_options, dist_type="deterministic",
            framework="torch")
        self.model = ModelCatalog.get_model_v2(
            obs_space, action_space, num_outputs=self.dist_dim,
            model_config=model_options, framework="torch")

        self.variables = ray.experimental.tf_utils.TensorFlowVariables(
            self.model.outputs, self.sess)

        self.num_params = sum(
            np.prod(variable.shape.as_list())
            for _, variable in self.variables.variables.items())

    def compute_actions(self, observation, add_noise=False, update=True):
        observation = self.preprocessor.transform(observation)
        observation = self.observation_filter(observation[None], update=update)

        dist_inputs, _ = self.model(observation, [], None)
        dist = self.dist_class(dist_inputs, self.model)
        action = dist.sample()
        action = _unbatch_tuple_actions(action)
        if add_noise and isinstance(self.action_space, gym.spaces.Box):
            action += np.random.randn(*action.shape) * self.action_noise_std
        return action

    def set_weights(self, x):
        self.variables.set_flat(x)

    def get_weights(self):
        return self.variables.get_flat()


ESTorchPolicy = build_torch_policy(
    name="ESTorchPolocy",
    loss_fn=None,
    get_default_config=lambda: ray.rllib.agents.es.es.DEFAULT_CONFIG,
    

)
