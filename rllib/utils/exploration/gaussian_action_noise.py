import gym
import numpy as np

from ray.rllib.utils.annotations import override
from ray.rllib.utils.exploration.exploration import Exploration
from ray.rllib.utils.framework import try_import_tf, try_import_torch, \
    get_variable
from ray.rllib.utils.schedules import ConstantSchedule, PiecewiseSchedule

tf = try_import_tf()
torch, _ = try_import_torch()


class GaussianActionNoise(Exploration):
    """
    
    """

    def __init__(self,
                 action_space,
                 initial_stddev=0.1,
                 final_stddev=0.002,
                 timesteps=int(1e5),
                 num_workers=None,
                 worker_index=None,
                 framework="tf"):
        """
        Args:
            action_space (Space): The gym action space used by the environment.
            initial_stddev (float): The initial stddev to use to add Gaussian
                noise to actions.
            final_stddev (float): The final stddev to use to add Gaussian
                noise to actions (after `timesteps`).
            timesteps (int): The time step after which stddev should
                always be `final_stddev`.
            num_workers (Optional[int]): The overall number of workers used.
            worker_index (Optional[int]): The index of the Worker using this
                Exploration.
            framework (Optional[str]): One of None, "tf", "torch".
        """
        assert isinstance(action_space, gym.spaces.Box) and \
            action_space.dtype in [np.float32, np.float64]
        assert framework is not None
        super().__init__(
            action_space=action_space,
            num_workers=num_workers,
            worker_index=worker_index,
            framework=framework)

        self.stddev_schedule = PiecewiseSchedule(
            endpoints=[(0, initial_stddev), (timesteps, final_stddev)],
            outside_value=final_stddev,
            framework=self.framework)

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
        # TODO(sven): This is hardcoded. Put a meaningful error, in case model
        # API is not as required.
        q_values = model.q_value_head(model._last_output)[0]
        if self.framework == "tf":
            return self._get_tf_exploration_action_op(action, explore,
                                                      timestep, q_values)
        else:
            return self._get_torch_exploration_action(action, explore, timestep)

    def _get_torch_exploration_action(self, action, explore, timestep):
        # Set last time step.
        self.last_time_step = timestep

        # Add Gaussian noise to the action.
        if explore:
            # Get the current stddev.
            stddev = self.stddev_schedule(self.last_timestep)

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

    def _get_tf_exploration_action_op(self, action, explore, timestep,
                                      q_values):
        """
        Tf helper method to produce the tf op for an epsilon exploration
            action.

        Args:
            action (tf.Tensor): The already sampled action (non-exploratory
                case) as tf op.

        Returns:
            tf.Tensor: The tf exploration-action op.
        """
        # Get the current stddev.
        stddev = tf.convert_to_tensor(
            self.stddev_schedule(timestep if timestep is not None else
                                 self.last_timestep))

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
        return [self.stddev_schedule(self.last_timestep)]

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
