# Code in this file is copied and adapted from
# https://github.com/openai/evolution-strategies-starter.

import gym
import numpy as np

import ray
import ray.experimental.tf_utils
from ray.rllib.agents.es.es_tf_policy import make_session
from ray.rllib.evaluation.sampler import _unbatch_tuple_actions
from ray.rllib.models import ModelCatalog
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.utils.filter import get_filter
from ray.rllib.utils.framework import try_import_tf

tf = try_import_tf()


class ARSTFPolicy:
    def __init__(self, obs_space, action_space, config):
        self.observation_space = obs_space
        self.action_space = action_space
        self.action_noise_std = config["action_noise_std"]
        self.preprocessor = ModelCatalog.get_preprocessor_for_space(
            self.observation_space)
        self.observation_filter = get_filter(config["observation_filter"],
                                             self.preprocessor.shape)

        self.single_threaded = config.get("single_threaded", False)
        self.sess = make_session(single_threaded=self.single_threaded)

        self.inputs = tf.placeholder(tf.float32,
                                     [None] + list(self.preprocessor.shape))

        # Policy network.
        dist_class, dist_dim = ModelCatalog.get_action_dist(
            self.action_space, config["model"], dist_type="deterministic")

        model = ModelCatalog.get_model({
            SampleBatch.CUR_OBS: self.inputs
        }, self.observation_space, self.action_space, dist_dim,
                                       config["model"])
        dist = dist_class(model.outputs, model)
        self.sampler = dist.sample()

        self.variables = ray.experimental.tf_utils.TensorFlowVariables(
            model.outputs, self.sess)

        self.num_params = sum(
            np.prod(variable.shape.as_list())
            for _, variable in self.variables.variables.items())
        self.sess.run(tf.global_variables_initializer())

    def compute_actions(self, observation, add_noise=False, update=True):
        observation = self.preprocessor.transform(observation)
        observation = self.observation_filter(observation[None], update=update)
        action = self.sess.run(
            self.sampler, feed_dict={self.inputs: observation})
        action = _unbatch_tuple_actions(action)
        if add_noise and isinstance(self.action_space, gym.spaces.Box):
            action += np.random.randn(*action.shape) * self.action_noise_std
        return action

    def set_flat_weights(self, x):
        self.variables.set_flat(x)

    def get_flat_weights(self):
        return self.variables.get_flat()
