import gym
import numpy as np

from ray.rllib.utils.annotations import override
from ray.rllib.utils.exploration.exploration import Exploration
from ray.rllib.utils.framework import try_import_tf, try_import_torch
from ray.rllib.utils.schedules import ConstantSchedule, PiecewiseSchedule

tf = try_import_tf()
torch, _ = try_import_torch()


class RandomActions(Exploration):
    """
    Outputs random actions for some timesteps.
    """
    
    def __init__(self,
                 action_space,
                 timesteps=int(1e5),
                 framework="tf"):
        """
        Args:
            action_space (Space): The gym action space used by the environment.
            timesteps (int): The time steps for which to output random actions.
            framework (Optional[str]): One of None, "tf", "torch".
        """
        assert framework is not None
        super().__init__(
            action_space=action_space,
            framework=framework)
        
        # The current time_step value (tf-var or python int).
        self.last_time_step = get_variable(
            0, framework=framework, tf_name="timestep")
    
    @override(Exploration)
    def get_exploration_action(self,
                               action,
                               model=None,
                               action_dist=None,
                               explore=True,
                               timestep=None):
        if self.framework == "tf":
            return self._get_tf_exploration_action_op(action, explore,
                                                      timestep)
        else:
            return self._get_torch_exploration_action(action, explore,
                                                      timestep)
    
    def _get_torch_exploration_action(self, action, explore, timestep):
        # Set last time step.
        self.last_time_step = timestep
        
        # Produce a random action.
        if explore:
            # Add IID Gaussian noise for exploration, TD3-style.
            normal_sample = torch.random_normal(
                tf.shape(action), stddev=stddev)
            exploration_action = torch.clip_by_value(
                action + normal_sample,
                self.action_space.low * torch.ones_like(action),
                self.action_space.high * torch.ones_like(action))
            return exploration_action
        # Return the unaltered action.
        else:
            return action
    
    def _get_tf_exploration_action_op(self, action, explore, timestep):
        """
        Tf helper method to produce the tf op for an epsilon exploration
            action.

        Args:
            action (tf.Tensor): The already sampled action (non-exploratory
                case) as tf op.

        Returns:
            tf.Tensor: The tf exploration-action op.
        """
        # Add IID Gaussian noise for exploration, TD3-style.
        normal_sample = tf.random_normal(tf.shape(action), stddev=stddev)
        exploration_action = tf.cond(
            pred=explore,
            true_fn=lambda: tf.clip_by_value(
                action + normal_sample,
                self.action_space.low * tf.ones_like(action),
                self.action_space.high * tf.ones_like(action)),
            false_fn=lambda: action)
        
        # Increment `last_timestep` by 1 (or set to `timestep`).
        assign_op = \
            tf.assign_add(self.last_timestep, 1) if timestep is None else \
                tf.assign(self.last_timestep, timestep)
        
        with tf.control_dependencies([assign_op]):
            return exploration_action
    
    @override(Exploration)
    def get_info(self):
        """
        Returns:
            Union[float,tf.Tensor[float]]: The current stddev value.
        """
        return self.last_timestep
    
    @override(Exploration)
    def get_state(self):
        return [self.last_timestep]
    
    @override(Exploration)
    def set_state(self, state):
        if self.framework == "tf" and tf.executing_eagerly() is False:
            update_op = tf.assign(self.last_timestep, state)
            with tf.control_dependencies([update_op]):
                return tf.no_op()
        self.last_timestep = state
    
    @override(Exploration)
    def reset_state(self):
        return self.set_state(0)
    
    @classmethod
    @override(Exploration)
    def merge_states(cls, exploration_objects):
        timesteps = [e.get_state() for e in exploration_objects]
        if exploration_objects[0].framework == "tf":
            return tf.reduce_sum(timesteps)
        return np.sum(timesteps)
