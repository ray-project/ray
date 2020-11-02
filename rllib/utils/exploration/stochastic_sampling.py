import gym
import numpy as np
import tree
from typing import Union

from ray.rllib.models.action_dist import ActionDistribution
from ray.rllib.models.modelv2 import ModelV2
from ray.rllib.utils.annotations import override
from ray.rllib.utils.exploration.exploration import Exploration
from ray.rllib.utils.exploration.random import Random
from ray.rllib.utils.framework import get_variable, try_import_tf, \
    try_import_torch, TensorType

tf1, tf, tfv = try_import_tf()
torch, _ = try_import_torch()


class StochasticSampling(Exploration):
    """An exploration that simply samples from a distribution.

    The sampling can be made deterministic by passing explore=False into
    the call to `get_exploration_action`.
    Also allows for scheduled parameters for the distributions, such as
    lowering stddev, temperature, etc.. over time.
    """

    def __init__(self,
                 action_space: gym.spaces.Space,
                 *,
                 framework: str,
                 model: ModelV2,
                 random_timesteps: int = 0,
                 **kwargs):
        """Initializes a StochasticSampling Exploration object.

        Args:
            action_space (gym.spaces.Space): The gym action space used by the
                environment.
            framework (str): One of None, "tf", "torch".
            model (ModelV2): The ModelV2 used by the owning Policy.
            random_timesteps (int): The number of timesteps for which to act
                completely randomly. Only after this number of timesteps,
                actual samples will be drawn to get exploration actions.
        """
        assert framework is not None
        super().__init__(
            action_space, model=model, framework=framework, **kwargs)

        # Create the Random exploration module (used for the first n
        # timesteps).
        self.random_timesteps = random_timesteps
        self.random_exploration = Random(
            action_space, model=self.model, framework=self.framework, **kwargs)

        # The current timestep value (tf-var or python int).
        self.last_timestep = get_variable(
            np.array(0, np.int64),
            framework=self.framework,
            tf_name="timestep",
            dtype=np.int64)

    @override(Exploration)
    def get_exploration_action(self,
                               *,
                               action_distribution: ActionDistribution,
                               timestep: Union[int, TensorType],
                               explore: bool = True):
        if self.framework == "torch":
            return self._get_torch_exploration_action(action_distribution,
                                                      timestep, explore)
        else:
            return self._get_tf_exploration_action_op(action_distribution,
                                                      timestep, explore)

    def _get_tf_exploration_action_op(self, action_dist, timestep, explore):
        ts = timestep if timestep is not None else self.last_timestep + 1

        stochastic_actions = tf.cond(
            pred=tf.convert_to_tensor(ts < self.random_timesteps),
            true_fn=lambda: (
                self.random_exploration.get_tf_exploration_action_op(
                    action_dist,
                    explore=True)[0]),
            false_fn=lambda: action_dist.sample(),
        )
        deterministic_actions = action_dist.deterministic_sample()

        action = tf.cond(
            tf.constant(explore) if isinstance(explore, bool) else explore,
            true_fn=lambda: stochastic_actions,
            false_fn=lambda: deterministic_actions)

        def logp_false_fn():
            batch_size = tf.shape(tree.flatten(action)[0])[0]
            return tf.zeros(shape=(batch_size, ), dtype=tf.float32)

        logp = tf.cond(
            tf.math.logical_and(
                explore, tf.convert_to_tensor(ts >= self.random_timesteps)),
            true_fn=lambda: action_dist.sampled_action_logp(),
            false_fn=logp_false_fn)

        # Increment `last_timestep` by 1 (or set to `timestep`).
        if self.framework in ["tf2", "tfe"]:
            if timestep is None:
                self.last_timestep.assign_add(1)
            else:
                self.last_timestep.assign(timestep)
            return action, logp
        else:
            assign_op = (tf1.assign_add(self.last_timestep, 1)
                         if timestep is None else tf1.assign(
                             self.last_timestep, timestep))
            with tf1.control_dependencies([assign_op]):
                return action, logp

    def _get_torch_exploration_action(self, action_dist: ActionDistribution,
                                      timestep: Union[TensorType, int],
                                      explore: Union[TensorType, bool]):
        # Set last timestep or (if not given) increase by one.
        self.last_timestep = timestep if timestep is not None else \
            self.last_timestep + 1

        # Apply exploration.
        if explore:
            # Random exploration phase.
            if self.last_timestep < self.random_timesteps:
                action, logp = \
                    self.random_exploration.get_torch_exploration_action(
                        action_dist, explore=True)
            # Take a sample from our distribution.
            else:
                action = action_dist.sample()
                logp = action_dist.sampled_action_logp()

        # No exploration -> Return deterministic actions.
        else:
            action = action_dist.deterministic_sample()
            logp = torch.zeros_like(action_dist.sampled_action_logp())

        return action, logp
