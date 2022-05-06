# Code in this file is copied and adapted from
# https://github.com/openai/evolution-strategies-starter.

import gym
import numpy as np
import tree  # pip install dm_tree

import ray
import ray.experimental.tf_utils
from ray.rllib.algorithms.es.es_tf_policy import make_session
from ray.rllib.models import ModelCatalog
from ray.rllib.policy.policy import Policy
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.utils.filter import get_filter
from ray.rllib.utils.framework import try_import_tf
from ray.rllib.utils.spaces.space_utils import unbatch

tf1, tf, tfv = try_import_tf()


class ARSTFPolicy(Policy):
    def __init__(self, obs_space, action_space, config):
        super().__init__(obs_space, action_space, config)
        self.action_noise_std = self.config["action_noise_std"]
        self.preprocessor = ModelCatalog.get_preprocessor_for_space(
            self.observation_space
        )
        self.observation_filter = get_filter(
            self.config["observation_filter"], self.preprocessor.shape
        )

        self.single_threaded = self.config.get("single_threaded", False)
        if self.config["framework"] == "tf":
            self.sess = make_session(single_threaded=self.single_threaded)

            # Set graph-level seed.
            if config.get("seed") is not None:
                with self.sess.as_default():
                    tf1.set_random_seed(config["seed"])

            self.inputs = tf1.placeholder(
                tf.float32, [None] + list(self.preprocessor.shape)
            )
        else:
            if not tf1.executing_eagerly():
                tf1.enable_eager_execution()
            self.sess = self.inputs = None
            if config.get("seed") is not None:
                # Tf2.x.
                if config.get("framework") == "tf2":
                    tf.random.set_seed(config["seed"])
                # Tf-eager.
                elif tf1 and config.get("framework") == "tfe":
                    tf1.set_random_seed(config["seed"])

        # Policy network.
        self.dist_class, dist_dim = ModelCatalog.get_action_dist(
            self.action_space, self.config["model"], dist_type="deterministic"
        )

        self.model = ModelCatalog.get_model_v2(
            obs_space=self.preprocessor.observation_space,
            action_space=self.action_space,
            num_outputs=dist_dim,
            model_config=self.config["model"],
        )

        self.sampler = None
        if self.sess:
            dist_inputs, _ = self.model({SampleBatch.CUR_OBS: self.inputs})
            dist = self.dist_class(dist_inputs, self.model)
            self.sampler = dist.sample()
            self.variables = ray.experimental.tf_utils.TensorFlowVariables(
                dist_inputs, self.sess
            )
            self.sess.run(tf1.global_variables_initializer())
        else:
            self.variables = ray.experimental.tf_utils.TensorFlowVariables(
                [], None, self.model.variables()
            )

        self.num_params = sum(
            np.prod(variable.shape.as_list())
            for _, variable in self.variables.variables.items()
        )

    def compute_actions(self, observation, add_noise=False, update=True, **kwargs):
        # Squeeze batch dimension (we always calculate actions for only a
        # single obs).
        observation = observation[0]
        observation = self.preprocessor.transform(observation)
        observation = self.observation_filter(observation[None], update=update)

        # `actions` is a list of (component) batches.
        # Eager mode.
        if not self.sess:
            dist_inputs, _ = self.model({SampleBatch.CUR_OBS: observation})
            dist = self.dist_class(dist_inputs, self.model)
            actions = dist.sample()
            actions = tree.map_structure(lambda a: a.numpy(), actions)
        # Graph mode.
        else:
            actions = self.sess.run(self.sampler, feed_dict={self.inputs: observation})

        actions = unbatch(actions)
        if add_noise and isinstance(self.action_space, gym.spaces.Box):
            actions += np.random.randn(*actions.shape) * self.action_noise_std
        return actions, [], {}

    def compute_single_action(
        self, observation, add_noise=False, update=True, **kwargs
    ):
        action, state_outs, extra_fetches = self.compute_actions(
            [observation], add_noise=add_noise, update=update, **kwargs
        )
        return action[0], state_outs, extra_fetches

    def get_state(self):
        return {"state": self.get_flat_weights()}

    def set_state(self, state):
        return self.set_flat_weights(state["state"])

    def set_flat_weights(self, x):
        self.variables.set_flat(x)

    def get_flat_weights(self):
        return self.variables.get_flat()
