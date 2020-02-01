import gym
import numpy as np

from ray.rllib.utils.annotations import override
from ray.rllib.utils.explorations.exploration import Exploration
from ray.rllib.utils.framework import try_import_tf, try_import_torch, \
    get_variable
from ray.rllib.utils.schedules import ConstantSchedule, PiecewiseSchedule

tf = try_import_tf()
torch, _ = try_import_torch()


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
                 epsilon_timesteps=int(1e5),
                 raw_model_output_key="q_values",
                 fixed_per_worker_epsilon=False,
                 worker_info=None,
                 framework="tf"):
        """
        Args:
            action_space (Space): The gym action space used by the environment.
            initial_epsilon (float): The initial epsilon value to use.
            final_epsilon (float): The final epsilon value to use.
            epsilon_timesteps (int): The time step after which epsilon should
                always be `final_epsilon`.
            framework (Optional[str]): One of None, "tf", "torch".
        """
        # For now, require Discrete action space (may loosen this restriction
        # in the future).
        assert isinstance(action_space, gym.spaces.Discrete)
        assert framework is not None
        super().__init__(
            action_space=action_space,
            worker_info=worker_info,
            framework=framework)

        self.epsilon_schedule = None
        # Use a fixed, different epsilon per worker. See: Ape-X paper.
        if fixed_per_worker_epsilon is True:
            idx = self.worker_info.get("worker_index", 0)
            num = self.worker_info.get("num_workers", 0)
            if num > 0:
                if idx >= 0:
                    exponent = (1 + idx / float(num - 1) * 7)
                    self.epsilon_schedule = ConstantSchedule(0.4**exponent)
                # Local worker should have zero exploration so that eval
                # rollouts run properly.
                else:
                    self.epsilon_schedule = ConstantSchedule(0.0)
        if self.epsilon_schedule is None:
            self.epsilon_schedule = PiecewiseSchedule(
                endpoints=[(0, initial_epsilon), (epsilon_timesteps,
                                                  final_epsilon)],
                outside_value=final_epsilon,
                framework=self.framework)

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
                               explore=True,
                               time_step=None):
        # TODO(sven): This is hardcoded. Put a meaningful error, in case model
        # API is not as required.
        q_values = model.q_value_head(model._last_output)[0]
        if self.framework == "tf":
            return self._get_tf_exploration_action_op(action, explore,
                                                      time_step, q_values)

        # Set last time step or (if not given) increase by one.
        self.last_time_step = time_step if time_step is not None else \
            self.last_time_step + 1

        if explore:
            # Get the current epsilon.
            epsilon = self.epsilon_schedule(self.last_time_step)

            batch_size = q_values.size()[0]
            # Mask out actions, whose Q-values are -inf, so that we don't
            # even consider them for exploration.
            random_valid_action_logits = torch.where(
                q_values == float("-inf"),
                torch.ones_like(q_values) * float("-inf"),
                torch.ones_like(q_values))

            random_actions = torch.squeeze(
                torch.multinomial(random_valid_action_logits, 1), axis=1)

            return torch.where(
                torch.empty((batch_size,)).uniform_() < epsilon,
                random_actions, action)

        # Return the action.
        else:
            return action

    def _get_tf_exploration_action_op(self, action, explore, time_step,
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
        epsilon = tf.convert_to_tensor(
            self.epsilon_schedule(time_step if time_step is not None else
                                  self.last_time_step))

        batch_size = tf.shape(action)[0]

        # Maske out actions with q-value=-inf so that we don't
        # even consider them for exploration.
        random_valid_action_logits = tf.where(
            tf.equal(q_values, tf.float32.min),
            tf.ones_like(q_values) * tf.float32.min, tf.ones_like(q_values))
        random_actions = tf.squeeze(
            tf.multinomial(random_valid_action_logits, 1), axis=1)

        chose_random = tf.random_uniform(
            tf.stack([batch_size]),
            minval=0, maxval=1, dtype=epsilon.dtype) \
            < epsilon

        exploration_action = tf.cond(
            pred=explore,
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
    def get_info(self):
        """
        Returns:
            Union[float,tf.Tensor[float]]: The current epsilon value.
        """
        return [self.epsilon_schedule(self.last_time_step)]

    @override(Exploration)
    def get_state(self):
        return [self.last_time_step]

    @override(Exploration)
    def set_state(self, state):
        if self.framework == "tf" and tf.executing_eagerly() is False:
            update_op = tf.assign(self.last_time_step, state)
            with tf.control_dependencies([update_op]):
                return tf.no_op()
        self.last_time_step = state

    @override(Exploration)
    def reset_state(self):
        return self.set_state(0)

    @classmethod
    @override(Exploration)
    def merge_states(cls, exploration_objects):
        time_steps = [e.get_state() for e in exploration_objects]
        if exploration_objects[0].framework == "tf":
            return tf.reduce_sum(time_steps)
        return np.sum(time_steps)
