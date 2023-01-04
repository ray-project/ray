# Code in this file is copied and adapted from
# https://github.com/openai/evolution-strategies-starter.

import gymnasium as gym
import numpy as np
import tree  # pip install dm_tree
from typing import Optional

import ray
import ray.experimental.tf_utils
from ray.rllib.models import ModelCatalog
from ray.rllib.policy.policy import Policy
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.utils import deprecation_warning
from ray.rllib.utils.annotations import override
from ray.rllib.utils.filter import get_filter
from ray.rllib.utils.framework import try_import_tf
from ray.rllib.utils.spaces.space_utils import get_base_struct_from_space, unbatch

tf1, tf, tfv = try_import_tf()


def rollout(
    policy: Policy,
    env: gym.Env,
    timestep_limit: Optional[int] = None,
    add_noise: bool = False,
    offset: float = 0.0,
):
    """Do a rollout.

    If add_noise is True, the rollout will take noisy actions with
    noise drawn from that stream. Otherwise, no action noise will be added.

    Args:
        policy: RLlib Policy from which to draw actions.
        env: Environment from which to draw rewards, done, and
            next state.
        timestep_limit: Steps after which to end the rollout.
            If None, use `env.spec.max_episode_steps` or 999999.
        add_noise: Indicates whether exploratory action noise should be
            added.
        offset: Value to subtract from the reward (e.g. survival bonus
            from humanoid).
    """
    max_timestep_limit = 999999
    env_timestep_limit = (
        env.spec.max_episode_steps
        if (hasattr(env, "spec") and hasattr(env.spec, "max_episode_steps"))
        else max_timestep_limit
    )
    timestep_limit = (
        env_timestep_limit
        if timestep_limit is None
        else min(timestep_limit, env_timestep_limit)
    )
    rewards = []
    t = 0
    observation, _ = env.reset()
    for _ in range(timestep_limit or max_timestep_limit):
        ac, _, _ = policy.compute_actions(
            [observation], add_noise=add_noise, update=True
        )
        ac = ac[0]
        observation, r, terminated, truncated, _ = env.step(ac)
        if offset != 0.0:
            r -= np.abs(offset)
        rewards.append(r)
        t += 1
        if terminated or truncated:
            break
    rewards = np.array(rewards, dtype=np.float32)
    return rewards, t


def make_session(single_threaded):
    if not single_threaded:
        return tf1.Session()
    return tf1.Session(
        config=tf1.ConfigProto(
            inter_op_parallelism_threads=1, intra_op_parallelism_threads=1
        )
    )


class ESTFPolicy(Policy):
    def __init__(self, obs_space, action_space, config):
        super().__init__(obs_space, action_space, config)
        self.action_space_struct = get_base_struct_from_space(action_space)
        self.action_noise_std = self.config["action_noise_std"]
        self.preprocessor = ModelCatalog.get_preprocessor_for_space(obs_space)
        self.observation_filter = get_filter(
            self.config["observation_filter"], self.preprocessor.shape
        )
        if self.config["framework"] == "tf":
            self.sess = make_session(
                single_threaded=self.config.get("tf_single_threaded", True)
            )

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
                if tfv == 2:
                    tf.random.set_seed(config["seed"])
                # Tf1.x.
                else:
                    tf1.set_random_seed(config["seed"])

        # Policy network.
        self.dist_class, dist_dim = ModelCatalog.get_action_dist(
            self.action_space, self.config["model"], dist_type="deterministic"
        )

        self.model = ModelCatalog.get_model_v2(
            obs_space=self.preprocessor.observation_space,
            action_space=action_space,
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

    @override(Policy)
    def compute_actions(self, obs_batch=None, add_noise=False, update=True, **kwargs):
        if "observation" in kwargs:
            assert obs_batch is None, (
                "You can not use both arguments, "
                "`observation` and `obs_batch`. `observation` "
                "is deprecated."
            )
            deprecation_warning(
                old="ESTFPolicy.compute_actions(observation=...)`",
                new="ESTFPolicy.compute_actions(obs_batch=...)",
            )
            obs_batch = kwargs["observation"]
        else:
            assert obs_batch is not None
        # Squeeze batch dimension (we always calculate actions for only a
        # single obs).
        observation = obs_batch[0]
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

        if add_noise:
            actions = tree.map_structure(
                self._add_noise, actions, self.action_space_struct
            )
        # Convert `flat_actions` to a list of lists of action components
        # (list of single actions).
        actions = unbatch(actions)
        return actions, [], {}

    def compute_single_action(
        self, observation, add_noise=False, update=True, **kwargs
    ):
        action, state_outs, extra_fetches = self.compute_actions(
            [observation], add_noise=add_noise, update=update, **kwargs
        )
        return action[0], state_outs, extra_fetches

    def _add_noise(self, single_action, single_action_space):
        if isinstance(
            single_action_space, gym.spaces.Box
        ) and single_action_space.dtype.name.startswith("float"):
            single_action += (
                np.random.randn(*single_action.shape) * self.action_noise_std
            )
        return single_action

    def get_state(self):
        return {"state": self.get_flat_weights()}

    def set_state(self, state):
        return self.set_flat_weights(state["state"])

    def set_flat_weights(self, x):
        self.variables.set_flat(x)

    def get_flat_weights(self):
        return self.variables.get_flat()
