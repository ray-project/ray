from typing import Union

from ray.rllib.models.action_dist import ActionDistribution
from ray.rllib.utils.annotations import override
from ray.rllib.utils.exploration.exploration import TensorType
from ray.rllib.utils.exploration.soft_q import SoftQ
from ray.rllib.utils.framework import try_import_tf, try_import_torch

tf1, tf, tfv = try_import_tf()
torch, _ = try_import_torch()


class SlateSoftQ(SoftQ):
    @override(SoftQ)
    def get_exploration_action(
        self,
        action_distribution: ActionDistribution,
        timestep: Union[int, TensorType],
        explore: bool = True,
    ):
        assert (
            self.framework == "torch"
        ), "ERROR: SlateSoftQ only supports torch so far!"

        cls = type(action_distribution)

        # Re-create the action distribution with the correct temperature
        # applied.
        action_distribution = cls(
            action_distribution.inputs, self.model, temperature=self.temperature
        )
        batch_size = action_distribution.inputs.size()[0]
        action_logp = torch.zeros(batch_size, dtype=torch.float)

        self.last_timestep = timestep

        # Explore.
        if explore:
            # Return stochastic sample over (q-value) logits.
            action = action_distribution.sample()
        # Return the deterministic "sample" (argmax) over (q-value) logits.
        else:
            action = action_distribution.deterministic_sample()

        return action, action_logp
