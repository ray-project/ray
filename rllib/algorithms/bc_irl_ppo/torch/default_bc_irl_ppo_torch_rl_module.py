from ray.rllib.algorithms.bc_irl_ppo.bc_irl_ppo_catalog import BCIRLPPOCatalog
from ray.rllib.algorithms.bc_irl_ppo.default_bc_irl_ppo_rl_module import (
    DefaultBCIRLRewardRLModule,
)
from ray.rllib.core.columns import Columns
from ray.rllib.core.models.base import ENCODER_OUT
from ray.rllib.core.rl_module import RLModule
from ray.rllib.core.rl_module.torch.torch_rl_module import TorchRLModule
from ray.rllib.utils.annotations import override
from ray.rllib.utils.framework import try_import_torch

torch, nn = try_import_torch()


class DefaultBCIRLRewardTorchRLModule(TorchRLModule, DefaultBCIRLRewardRLModule):
    def __init__(self, *args, **kwargs):
        # Extract the catalog class.
        catalog_class = kwargs.pop("catalog_class", None)
        # If no catalg class is defined fall back on the default one.
        if catalog_class is None:
            catalog_class = BCIRLPPOCatalog
        # Initialize with the defined catalog class the `TorchRLModule`.
        super().__init__(*args, **kwargs, catalog_class=catalog_class)

    @override(RLModule)
    def _forward(self, batch):
        """Defines the general forward pass.

        Note, this forward pass is used in training, exploration, and inference.
        Because the `RLModule` calls in its `forward` method implicitly the
        defined method here, a functional call via `torch.func.functional_call`
        becomes possible.
        """
        inputs = {}
        output = {}

        if self.reward_type.value == "action":
            # One-hot-encode actions from discrete action spaces.
            # TODO (simon): Implement also box action-spaces.
            actions = nn.functional.one_hot(
                batch[Columns.ACTIONS].long(), self.action_space.n
            ).float()
            # Concatenate state, action, and next state in the input.
            inputs[Columns.OBS] = torch.concatenate(
                [batch[Columns.OBS], actions, batch[Columns.NEXT_OBS]], dim=-1
            )
        elif self.reward_type.value == "curr_next_state":
            # Concatenate state, and next state in the input.
            inputs[Columns.OBS] = torch.concatenate(
                [batch[Columns.OBS], batch[Columns.NEXT_OBS]], dim=-1
            )
        elif self.reward_type.value == "next_state":
            # Use the next state as input to the forward pass. Note, the
            # state-only reward has been shown to lead to rewards that
            # generalize better Fu et al. (2017).
            inputs[Columns.OBS] = batch[Columns.NEXT_OBS]

        # Run a single forward pass on the encoder.
        encoder_outs = self.rf_encoder(inputs)
        # Run the forward pass on the head of the reward-function to receive the
        # estimated reward.
        output[Columns.REWARDS] = self.rf(encoder_outs[ENCODER_OUT]).squeeze(dim=-1)
        return output
