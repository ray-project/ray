from typing import Union

from ray.rllib.models.action_dist import ActionDistribution
from ray.rllib.utils.annotations import override
from ray.rllib.utils.exploration.exploration import Exploration, TensorType
from ray.rllib.utils.framework import try_import_tf, try_import_torch, \
    get_variable
from ray.rllib.utils.from_config import from_config
from ray.rllib.utils.numpy import LARGE_INTEGER
from ray.rllib.utils.schedules import Schedule, PiecewiseSchedule

tf = try_import_tf()
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
                 initial_epsilon=1.0,
                 final_epsilon=0.05,
                 epsilon_timesteps=int(1e5),
                 epsilon_schedule=None,
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

        self.epsilon_schedule = \
            from_config(Schedule, epsilon_schedule, framework=framework) or \
            PiecewiseSchedule(
                endpoints=[
                    (0, initial_epsilon), (epsilon_timesteps, final_epsilon)],
                outside_value=final_epsilon,
                framework=self.framework)

        # The current timestep value (tf-var or python int).
        self.last_timestep = get_variable(
            0, framework=framework, tf_name="timestep")

        # Build the tf-info-op.
        if self.framework == "tf":
            self._tf_info_op = self.get_info()

    @override(Exploration)
    def get_exploration_action(self,
                               *,
                               action_distribution: ActionDistribution,
                               timestep: Union[int, TensorType],
                               explore: bool = True):

        q_values = action_distribution.inputs
        if self.framework == "tf":
            return self._get_tf_exploration_action_op(q_values, explore,
                                                      timestep)
        else:
            return self._get_torch_exploration_action(q_values, explore,
                                                      timestep)

    def _get_tf_exploration_action_op(self, q_values, explore, timestep):
        """TF method to produce the tf op for an epsilon exploration action.

        Args:
            q_values (Tensor): The Q-values coming from some q-model.

        Returns:
            tf.Tensor: The tf exploration-action op.
        """
        epsilon = self.epsilon_schedule(timestep if timestep is not None else
                                        self.last_timestep)

        # Get the exploit action as the one with the highest logit value.
        exploit_action = tf.argmax(q_values, axis=1)

        batch_size = tf.shape(q_values)[0]
        # Mask out actions with q-value=-inf so that we don't even consider
        # them for exploration.
        random_valid_action_logits = tf.where(
            tf.equal(q_values, tf.float32.min),
            tf.ones_like(q_values) * tf.float32.min, tf.ones_like(q_values))
        random_actions = tf.squeeze(
            tf.multinomial(random_valid_action_logits, 1), axis=1)

        chose_random = tf.random_uniform(
            tf.stack([batch_size]),
            minval=0, maxval=1, dtype=tf.float32) \
            < epsilon

        action = tf.cond(
            pred=tf.constant(explore, dtype=tf.bool)
            if isinstance(explore, bool) else explore,
            true_fn=(
                lambda: tf.where(chose_random, random_actions, exploit_action)
            ),
            false_fn=lambda: exploit_action)

        assign_op = tf.assign(self.last_timestep, timestep)
        with tf.control_dependencies([assign_op]):
            return action, tf.zeros_like(action, dtype=tf.float32)

    def _get_torch_exploration_action(self, q_values, explore, timestep):
        """Torch method to produce an epsilon exploration action.

        Args:
            q_values (Tensor): The Q-values coming from some Q-model.

        Returns:
            torch.Tensor: The exploration-action.
        """
        self.last_timestep = timestep
        _, exploit_action = torch.max(q_values, 1)
        action_logp = torch.zeros_like(exploit_action)

        # Explore.
        if explore:
            # Get the current epsilon.
            epsilon = self.epsilon_schedule(self.last_timestep)
            batch_size = q_values.size()[0]
            # Mask out actions, whose Q-values are -inf, so that we don't
            # even consider them for exploration.
            random_valid_action_logits = torch.where(
                q_values == -float(LARGE_INTEGER),
                torch.ones_like(q_values) * 0.0, torch.ones_like(q_values))
            # A random action.
            random_actions = torch.squeeze(
                torch.multinomial(random_valid_action_logits, 1), axis=1)
            # Pick either random or greedy.
            action = torch.where(
                torch.empty(
                    (batch_size, )).uniform_().to(self.device) < epsilon,
                random_actions, exploit_action)

            return action, action_logp
        # Return the deterministic "sample" (argmax) over the logits.
        else:
            return exploit_action, action_logp

    @override(Exploration)
    def get_info(self, sess=None):
        if sess:
            return sess.run(self._tf_info_op)
        eps = self.epsilon_schedule(self.last_timestep)
        return {"cur_epsilon": eps}
