import gym
import numpy as np
import random

from ray.rllib.utils.annotations import override
from ray.rllib.utils.explorations.exploration import Exploration
from ray.rllib.utils.framework import try_import_tf, get_variable
from ray.rllib.utils.schedules import ConstantSchedule, LinearSchedule

tf = try_import_tf()


class EpsilonGreedy(Exploration):
    """
    An epsilon-greedy Exploration class that produces exploration actions
    when given a Model's output and a current epsilon value (based on some
    Schedule).
    """

    def __init__(self,
                 action_space,
                 initial_epsilon=1.0,
                 final_epsilon=0.1,
                 final_timestep=int(1e5),
                 raw_model_output_key="q_values",
                 worker_info=None,
                 framework=None):
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
        super().__init__(action_space=action_space,
                         worker_info=worker_info,
                         framework=framework)

        # Use a fixed, different epsilon per worker. See: Ape-X paper.
        idx = self.worker_info.get("worker_index", 0)
        num = self.worker_info.get("num_workers", 0)
        if num > 0:
            if idx >= 0:
                exponent = (1 + idx / float(num - 1) * 7)
                self.epsilon_schedule = ConstantSchedule(0.4**exponent)
            # Local worker should have zero exploration so that eval rollouts
            # run properly.
            else:
                self.epsilon_schedule = ConstantSchedule(0.0)
        else:
            self.epsilon_schedule = LinearSchedule(
                initial_p=initial_epsilon,
                final_p=final_epsilon,
                schedule_timesteps=final_timestep)

        # The key to use to lookup the raw-model output from the
        # incoming dict.
        self.raw_model_output_key = raw_model_output_key

        # The current time_step value (tf-var or python int).
        self.last_time_step = get_variable(
            0, framework=framework, tf_name="time-step")

    @override(Exploration)
    def get_exploration_action(self,
                               action,
                               model=None,
                               action_dist=None,
                               is_exploring=True,
                               time_step=None):
        # TODO(sven): This is hardcoded. Put a meaningful error, in case model API is not as required.
        q_values = model.q_value_head(model._last_output)[0]
        if self.framework == "tf":
            return self._get_tf_exploration_action_op(action, is_exploring,
                                                      time_step, q_values)

        # Set last time step or (if not given) increase by one.
        self.last_time_step = time_step if time_step is not None else \
            self.last_time_step + 1

        if is_exploring:
            # Get the current epsilon.
            epsilon = self.epsilon_schedule.value(self.last_time_step)

            batch_size = len(q_values)
            # Mask out actions, whose Q-values are -inf, so that we don't
            # even consider them for exploration.
            random_valid_action_logits = np.where(
                q_values == float("-inf"),
                np.ones_like(q_values) * float("-inf"), np.ones_like(q_values))

            random_actions = np.squeeze(
                # TODO
                np.multinomial(random_valid_action_logits, 1),
                axis=1)

            return np.where(
                np.random.random(tf.stack([batch_size])) < epsilon,
                random_actions, action)

        # Return the action.
        else:
            return action

    def _get_tf_exploration_action_op(self, action, is_exploring, time_step,
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
        epsilon = self.epsilon_schedule.value(
            time_step if time_step is not None else self.last_time_step)

        batch_size = tf.shape(action)[0]

        # Maske out actions with q-value=-inf so that we don't
        # even consider them for exploration.
        random_valid_action_logits = tf.where(
            tf.equal(q_values, tf.float32.min),
            tf.ones_like(q_values) * tf.float32.min, tf.ones_like(q_values))
        random_actions = tf.squeeze(
            tf.multinomial(random_valid_action_logits, 1), axis=1)

        chose_random = tf.random_uniform(
            tf.stack([batch_size]), minval=0, maxval=1, dtype=epsilon.dtype) \
            < epsilon

        exploration_action = tf.cond(
            pred=is_exploring,
            true_fn=lambda: tf.where(chose_random, random_actions, action),
            false_fn=lambda: action,
        )
        # Increment `last_time_step` by 1 (or set to `time_step`).
        assign_op = \
            tf.assign_add(self.last_time_step, 1) if time_step is None else \
            tf.assign(self.last_time_step, time_step)
        with tf.control_dependencies([assign_op]):
            return exploration_action

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
    def reset_state(self, tf_session=None):
        self.set_state(0, tf_session=tf_session)

    @classmethod
    @override(Exploration)
    def merge_states(cls, exploration_objects):
        states = [e.get_state() for e in exploration_objects]
        if exploration_objects[0].framework == "tf":
            return tf.reduce_sum(states)
        return np.sum(states)
