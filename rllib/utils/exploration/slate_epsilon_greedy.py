from typing import Union

from ray.rllib.models.action_dist import ActionDistribution
from ray.rllib.utils.annotations import override
from ray.rllib.utils.exploration.epsilon_greedy import EpsilonGreedy
from ray.rllib.utils.exploration.exploration import TensorType
from ray.rllib.utils.framework import try_import_tf, try_import_torch
from ray.rllib.utils.torch_utils import FLOAT_MIN

tf1, tf, tfv = try_import_tf()
torch, _ = try_import_torch()


class SlateEpsilonGreedy(EpsilonGreedy):
    @override(EpsilonGreedy)
    def _get_tf_exploration_action_op(
        self,
        action_distribution: ActionDistribution,
        explore: Union[bool, TensorType],
        timestep: Union[int, TensorType],
    ) -> "tf.Tensor":

        per_slate_q_values = action_distribution.inputs
        all_slates = action_distribution.all_slates

        exploit_action = action_distribution.deterministic_sample()

        batch_size = tf.shape(per_slate_q_values)[0]
        action_logp = tf.zeros(batch_size, dtype=tf.float32)

        # Get the current epsilon.
        epsilon = self.epsilon_schedule(
            timestep if timestep is not None else self.last_timestep
        )
        # Mask out actions, whose Q-values are -inf, so that we don't
        # even consider them for exploration.
        random_valid_action_logits = tf.where(
            tf.equal(per_slate_q_values, tf.float32.min),
            tf.ones_like(per_slate_q_values) * tf.float32.min,
            tf.ones_like(per_slate_q_values),
        )
        # A random action.
        random_indices = tf.squeeze(
            tf.random.categorical(random_valid_action_logits, 1), axis=1
        )
        random_actions = tf.gather(all_slates, random_indices)

        choose_random = (
            tf.random.uniform(
                tf.stack([batch_size]), minval=0, maxval=1, dtype=tf.float32
            )
            < epsilon
        )

        # Pick either random or greedy.
        action = tf.cond(
            pred=tf.constant(explore, dtype=tf.bool)
            if isinstance(explore, bool)
            else explore,
            true_fn=(lambda: tf.where(choose_random, random_actions, exploit_action)),
            false_fn=lambda: exploit_action,
        )

        if self.framework in ["tf2", "tfe"] and not self.policy_config["eager_tracing"]:
            self.last_timestep = timestep
            return action, action_logp
        else:
            assign_op = tf1.assign(self.last_timestep, tf.cast(timestep, tf.int64))
            with tf1.control_dependencies([assign_op]):
                return action, action_logp

    @override(EpsilonGreedy)
    def _get_torch_exploration_action(
        self,
        action_distribution: ActionDistribution,
        explore: bool,
        timestep: Union[int, TensorType],
    ) -> "torch.Tensor":

        per_slate_q_values = action_distribution.inputs
        all_slates = self.model.slates

        exploit_indices = action_distribution.deterministic_sample()
        exploit_action = all_slates[exploit_indices]

        batch_size = per_slate_q_values.size()[0]
        action_logp = torch.zeros(batch_size, dtype=torch.float)

        self.last_timestep = timestep

        # Explore.
        if explore:
            # Get the current epsilon.
            epsilon = self.epsilon_schedule(self.last_timestep)
            # Mask out actions, whose Q-values are -inf, so that we don't
            # even consider them for exploration.
            random_valid_action_logits = torch.where(
                per_slate_q_values <= FLOAT_MIN,
                torch.ones_like(per_slate_q_values) * 0.0,
                torch.ones_like(per_slate_q_values),
            )
            # A random action.
            random_indices = torch.squeeze(
                torch.multinomial(random_valid_action_logits, 1), axis=1
            )
            random_actions = all_slates[random_indices]

            # Pick either random or greedy.
            action = torch.where(
                torch.empty((batch_size,)).uniform_().to(self.device) < epsilon,
                random_actions,
                exploit_action,
            )

            return action, action_logp
        # Return the deterministic "sample" (argmax) over the logits.
        else:
            return exploit_action, action_logp
