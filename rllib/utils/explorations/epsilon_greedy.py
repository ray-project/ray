import gym
import numpy as np
import random

from ray.rllib.utils.annotations import override
from ray.rllib.utils.explorations.exploration import Exploration
from ray.rllib.utils.framework import try_import_tf
#from ray.rllib.utils.from_config import from_config
from ray.rllib.utils.schedules import LinearSchedule

tf = try_import_tf()


class EpsilonGreedy(Exploration):
    """
    An epsilon-greedy Exploration class that produces exploration actions
    when given a Model's output and a current epsilon value (based on some
    Schedule).
    """
    def __init__(
            self,
            action_space,
            initial_epsilon=1.0,
            final_epsilon=0.1,
            final_timestep=int(1e5),
            framework=None
    ):
        """
        Args:
            action_space (Space): The gym action space used by the environment.
            initial_epsilon (float): The initial epsilon value to use.
            final_epsilon (float): The final epsilon value to use.
            final_timestep (int): The time step after which epsilon should
                always be `final_epsilon`.
            framework (Optional[str]): One of None, "tf", "torch".
        """
        # For now, require Discrete action space (may loosen this restriction
        # in the future).
        assert isinstance(action_space, gym.spaces.Discrete)
        super().__init__(action_space=action_space, framework=framework)

        self.action_space = action_space
        # Create a Schedule object.
        self.epsilon_schedule = LinearSchedule(
            initial_p=initial_epsilon, final_p=final_epsilon,
            schedule_timesteps=final_timestep)
        # The latest (current) time_step value received.
        self.last_time_step = 0

    @override(Exploration)
    def get_action(self, time_step, model_output, model=None, action_dist=None,
                   action_sample=None):
        if self.framework == "tf":
            return self._get_tf_action_op(time_step, model_output)

        self.last_time_step = time_step
        # Get the current epsilon.
        epsilon = self.epsilon_schedule.value(time_step)

        # "Epsilon-case": Return a random action.
        if random.random() < epsilon:
            return np.random.randint(0, model_output.shape[1], size=[
                model_output.shape[0]
            ]), np.ones(model_output.shape[0])
        # Return the greedy (argmax) action.
        else:
            return np.argmax(model_output, axis=1), \
                   np.ones(model_output.shape[0])

    def _get_tf_action_op(self, time_step, model_output):
        """
        Tf helper method to produce the tf op for an epsilon exploration
            action.

        Args:
            time_step (int): The current (sampling) time step.
            model_output (any): The Model's output Tensor(s).

        Returns:
            tf.Tensor: The tf exploration-action op.
        """
        epsilon = self.epsilon_schedule.value(time_step)
        cond =  tf.cond(
            condition=tf.random_uniform() < epsilon,
            true_fn=lambda: tf.random_uniform(
                shape=tf.shape(model_output), maxval=model_output.shape[1],
                dtype=tf.int32
            ),
            false_fn=lambda: tf.argmax(model_output, axis=1),
        ), tf.ones(model_output.shape[0])

        # Update `last_time_step` and return action op.
        update_op = tf.compat.v1.assign_add(self.last_time_step, 1)
        with tf.control_dependencies([update_op]):
            return cond

    @override(Exploration)
    def get_state(self):
        return self.last_time_step

    @override(Exploration)
    def set_state(self, exploration_state):
        self.last_time_step = exploration_state

    @override(Exploration)
    def reset_state(self):
        self.last_time_step = 0

    @classmethod
    @override(Exploration)
    def merge_states(cls, exploration_states):
        return np.mean(exploration_states)
