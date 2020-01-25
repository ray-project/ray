import gym
import numpy as np
import random

from ray.rllib.utils.annotations import override
from ray.rllib.utils.explorations.exploration import Exploration
from ray.rllib.utils.framework import try_import_tf, get_variable
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
            raw_model_output_key="q_values",
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
        # The key to use to lookup the raw-model output from the
        # incoming dict.
        self.raw_model_output_key = raw_model_output_key
        # The latest (current) time_step value received.
        self.last_time_step = get_variable(
            0, framework=framework, tf_name="time-step")

    @override(Exploration)
    def get_action(self, action, model=None, action_dist=None):
        #model_output = model_output[self.raw_model_output_key]

        if self.framework == "tf":
            return self._get_tf_action_op(action)

        self.last_time_step += 1
        # Get the current epsilon.
        epsilon = self.epsilon_schedule.value(self.last_time_step)

        # "Epsilon-case": Return a random action.
        if random.random() < epsilon:
            return np.random.randint(0, self.action_space.n, size=[
                action.shape[0]
            ])  #, np.ones(action.shape[0])
        # Return the action.
        else:
            return action  #np.argmax(model_output, axis=1), \
                   #np.ones(model_output.shape[0])

    def _get_tf_action_op(self, action):
        """
        Tf helper method to produce the tf op for an epsilon exploration
            action.

        Args:
            #time_step (int): The current (sampling) time step.
            model_output (any): The Model's output Tensor(s).

        Returns:
            tf.Tensor: The tf exploration-action op.
        """
        epsilon = self.epsilon_schedule.value(self.last_time_step)

        cond = tf.cond(
            pred=tf.random_uniform(shape=()) < epsilon,
            true_fn=lambda: tf.random_uniform(
                shape=tf.shape(action), maxval=self.action_space.n,
                dtype=action.dtype
            ),
            false_fn=lambda: action,  # tf.argmax(action, axis=1),
        )  #, tf.ones(action.shape[0])

        # Update `last_time_step` and return action op.
        update_op = tf.assign_add(self.last_time_step, 1)
        with tf.control_dependencies([update_op]):
            return cond

    @override(Exploration)
    def get_state(self):
        return self.last_time_step

    @override(Exploration)
    def set_state(self, exploration_state):
        if self.framework == "tf":
            update_op = tf.assign(self.last_time_step, exploration_state)
            with tf.control_dependencies([update_op]):
                return tf.no_op()
        self.last_time_step = exploration_state

    @override(Exploration)
    def reset_state(self):
        self.set_state(0)

    @classmethod
    @override(Exploration)
    def merge_states(cls, exploration_states):
        return np.mean(exploration_states)
