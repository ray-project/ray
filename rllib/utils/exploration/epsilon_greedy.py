import logging
import numpy as np
import tree
import random
from typing import Union, Optional

from ray.rllib.models.torch.torch_action_dist \
    import TorchMultiActionDistribution
from ray.rllib.models.action_dist import ActionDistribution
from ray.rllib.utils.annotations import DeveloperAPI, override
from ray.rllib.utils.exploration.exploration import Exploration, TensorType
from ray.rllib.utils.framework import try_import_tf, try_import_torch, \
    get_variable
from ray.rllib.utils.from_config import from_config
from ray.rllib.utils.numpy import LARGE_INTEGER
from ray.rllib.utils.schedules import ConstantSchedule, Schedule, PiecewiseSchedule
from ray.rllib.utils.torch_ops import FLOAT_MIN

logger = logging.getLogger(__name__)

tf1, tf, tfv = try_import_tf()
torch, _ = try_import_torch()


class EpsilonGreedy(Exploration):
    """Epsilon-greedy Exploration class that produces exploration actions.

    When given a Model's output and a current epsilon value (based on some
    Schedule), it produces a random action (if rand(1) < eps) or
    uses the model-computed one (if rand(1) >= eps).
    """

    def __init__(self,
                 action_space,
                 *,
                 framework: str,
                 initial_epsilon: float = 1.0,
                 final_epsilon: float = 0.05,
                 epsilon_timesteps: int = int(1e5),
                 epsilon_schedule: Optional[Schedule] = None,
                 **kwargs):
        """Create an EpsilonGreedy exploration class.

        Args:
            initial_epsilon (float): The initial epsilon value to use.
            final_epsilon (float): The final epsilon value to use.
            epsilon_timesteps (int): The time step after which epsilon should
                always be `final_epsilon`.
            epsilon_schedule (Optional[Schedule]): An optional Schedule object
                to use (instead of constructing one from the given parameters).
        """
        assert framework is not None
        super().__init__(
            action_space=action_space, framework=framework, **kwargs)
        self.reset_schedule(initial_epsilon=initial_epsilon,
                            final_epsilon=final_epsilon,
                            epsilon_timesteps=epsilon_timesteps,
                            epsilon_schedule=epsilon_schedule)
        # The current timestep value (tf-var or python int).
        self.last_timestep = get_variable(
            np.array(0, np.int32),
            framework=framework,
            tf_name="timestep",
            dtype=np.int32)
        self._deterministic_sample = None

        # Build the tf-info-op.
        if self.framework in ["tf2", "tf", "tfe"]:
            self._tf_info_op = self.get_info()

    @DeveloperAPI
    def reset_schedule(self,
                       initial_epsilon=None,
                       final_epsilon=None,
                       epsilon_timesteps=None,
                       epsilon_schedule=None
                       ):
        """ Allows to re-initialize the values of the schedule.
        If None is provided in any of the parameters, the previous value
        (the one passed to the __init__) is assumed if available.
            initial_epsilon (Optional[float]): The initial epsilon value to use.
            final_epsilon (Optional[float]): The final epsilon value to use.
            epsilon_timesteps (Optional[int]): The time step after which epsilon should
                always be `final_epsilon`.
            epsilon_schedule (Optional[Schedule]): An optional Schedule object
                to use (instead of constructing one from the given parameters).
        """
        if initial_epsilon is not None:
            self.initial_epsilon = initial_epsilon
        if final_epsilon is not None:
            self.final_epsilon = final_epsilon
        if epsilon_timesteps is not None:
            self.epsilon_timesteps = epsilon_timesteps
        self.epsilon_schedule = \
            from_config(Schedule, epsilon_schedule, framework=self.framework) or \
            PiecewiseSchedule(
                endpoints=[
                    (0, self.initial_epsilon), (self.epsilon_timesteps, self.final_epsilon)],
                outside_value=self.final_epsilon,
                framework=self.framework)
        logger.info(f"Exploration using the Schedule type {type(self.epsilon_schedule).__name__}")
        if isinstance(self.epsilon_schedule, ConstantSchedule):
            logger.info(f"Exploration Schedule constant value {self.epsilon_schedule.value(1)}")

    @override(Exploration)
    def deterministic_sample(self):
        return self._deterministic_sample

    @override(Exploration)
    def get_exploration_action(self,
                               *,
                               action_distribution: ActionDistribution,
                               timestep: Union[int, TensorType],
                               explore: bool = True):

        if self.framework in ["tf2", "tf", "tfe"]:
            return self._get_tf_exploration_action_op(action_distribution,
                                                      explore, timestep)
        else:
            return self._get_torch_exploration_action(action_distribution,
                                                      explore, timestep)

    def _get_tf_exploration_action_op(self,
                                      action_distribution: ActionDistribution,
                                      explore: Union[bool, TensorType],
                                      timestep: Union[int, TensorType]):
        """TF method to produce the tf op for an epsilon exploration action.

        Args:
            action_distribution (ActionDistribution): The instantiated
                ActionDistribution object to work with when creating
                exploration actions.

        Returns:
            tf.Tensor: The tf exploration-action op.
        """
        # TODO: Support MultiActionDistr for tf.
        q_values = action_distribution.inputs
        epsilon = self.epsilon_schedule(timestep if timestep is not None else
                                        self.last_timestep)

        # Get the exploit action as the one with the highest logit value.
        self._deterministic_sample = tf.argmax(q_values, axis=1)

        batch_size = tf.shape(q_values)[0]
        # Mask out actions with q-value=-inf so that we don't even consider
        # them for exploration.
        random_valid_action_logits = tf.where(
            tf.equal(q_values, tf.float32.min),
            tf.ones_like(q_values) * tf.float32.min, tf.ones_like(q_values))
        random_actions = tf.squeeze(
            tf.random.categorical(random_valid_action_logits, 1), axis=1)

        chose_random = tf.random.uniform(
            tf.stack([batch_size]), minval=0, maxval=1,
            dtype=tf.float32) < epsilon

        action = tf.cond(
            pred=tf.constant(explore, dtype=tf.bool)
            if isinstance(explore, bool) else explore,
            true_fn=(
                lambda: tf.where(chose_random, random_actions, self._deterministic_sample)
            ),
            false_fn=lambda: self._deterministic_sample)

        if self.framework in ["tf2", "tfe"]:
            self.last_timestep = timestep
            return action, tf.zeros_like(action, dtype=tf.float32)
        else:
            assign_op = tf1.assign(self.last_timestep, timestep)
            with tf1.control_dependencies([assign_op]):
                return action, tf.zeros_like(action, dtype=tf.float32)

    def _get_torch_exploration_action(
            self, action_distribution: ActionDistribution, explore: bool,
            timestep: Union[int, TensorType]):
        """Torch method to produce an epsilon exploration action.

        Args:
            action_distribution (ActionDistribution): The instantiated
                ActionDistribution object to work with when creating
                exploration actions.

        Returns:
            torch.Tensor: The exploration-action.
        """
        q_values = action_distribution.inputs
        self.last_timestep = timestep
        self._deterministic_sample = action_distribution.deterministic_sample()
        batch_size = q_values.size()[0]
        action_logp = torch.zeros(batch_size, dtype=torch.float)

        # Explore.
        if explore:
            # Get the current epsilon.
            epsilon = self.epsilon_schedule(self.last_timestep)
            if isinstance(action_distribution, TorchMultiActionDistribution):
                self._deterministic_sample = tree.flatten(self._deterministic_sample)
                for i in range(batch_size):
                    if random.random() < epsilon:
                        # TODO: (bcahlit) Mask out actions
                        random_action = tree.flatten(
                            self.action_space.sample())
                        for j in range(len(self._deterministic_sample)):
                            self._deterministic_sample[j][i] = torch.tensor(
                                random_action[j])
                self._deterministic_sample = tree.unflatten_as(
                    action_distribution.action_space_struct, self._deterministic_sample)

                return self._deterministic_sample, action_logp

            else:
                # Mask out actions, whose Q-values are -inf, so that we don't
                # even consider them for exploration.
                random_valid_action_logits = torch.where(
                    q_values <= FLOAT_MIN,
                    torch.ones_like(q_values) * 0.0, torch.ones_like(q_values))
                # A random action.
                random_actions = torch.squeeze(
                    torch.multinomial(random_valid_action_logits, 1), axis=1)

                # Pick either random or greedy.
                action = torch.where(
                    torch.empty(
                        (batch_size, )).uniform_().to(self.device) < epsilon,
                    random_actions, self._deterministic_sample)

                return action, action_logp
        # Return the deterministic "sample" (argmax) over the logits.
        else:
            return self._deterministic_sample, action_logp

    @override(Exploration)
    def get_info(self, sess: Optional["tf.Session"] = None):
        if sess:
            return sess.run(self._tf_info_op)
        eps = self.epsilon_schedule(self.last_timestep)
        return {"cur_epsilon": eps}

    @override(Exploration)
    def restore_info(self, info: dict):
        self.reset_schedule(initial_epsilon=info.get("cur_epsilon"))
