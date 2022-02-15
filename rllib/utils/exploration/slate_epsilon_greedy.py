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
