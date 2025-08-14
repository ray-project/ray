import gymnasium as gym
from typing import Any, Dict, Optional

from ray.rllib.algorithms.iql.default_iql_rl_module import DefaultIQLRLModule
from ray.rllib.algorithms.iql.iql_learner import VF_PREDS_NEXT, QF_TARGET_PREDS
from ray.rllib.algorithms.sac.torch.default_sac_torch_rl_module import (
    DefaultSACTorchRLModule,
)
from ray.rllib.core.columns import Columns
from ray.rllib.core.models.base import ENCODER_OUT
from ray.rllib.core.rl_module.apis.value_function_api import ValueFunctionAPI
from ray.rllib.utils.annotations import override
from ray.rllib.utils.framework import try_import_torch
from ray.rllib.utils.typing import TensorType

torch, nn = try_import_torch()


class DefaultIQLTorchRLModule(DefaultSACTorchRLModule, DefaultIQLRLModule):

    framework: str = "torch"

    @override(DefaultSACTorchRLModule)
    def _forward_train(self, batch: Dict, **kwargs) -> Dict[str, Any]:

        # Right now, IQL runs only with continuous action spaces.
        # TODO (simon): Implement it also for discrete action spaces.
        if not isinstance(self.action_space, gym.spaces.Box):
            raise ValueError(
                f"Unsupported action space type: {type(self.action_space)}. "
                "Only continuous action spaces are supported."
            )

        # Call the forward pass of the SAC module.
        output = super()._forward_train(batch, **kwargs)

        # Create batches for the forward passes of the target Q-networks and the
        # value function.
        batch_curr = {
            Columns.OBS: batch[Columns.OBS],
            Columns.ACTIONS: batch[Columns.ACTIONS],
        }
        batch_next = {Columns.OBS: batch[Columns.NEXT_OBS]}

        # These target q-values are needed for the value loss and actor loss.
        output[QF_TARGET_PREDS] = self._qf_forward_train_helper(
            batch_curr, encoder=self.target_qf_encoder, head=self.target_qf
        )
        # If a twin-Q architecture is used run its target Q-network.
        if self.twin_q:
            output[QF_TARGET_PREDS] = torch.min(
                output[QF_TARGET_PREDS],
                self._qf_forward_train_helper(
                    batch_curr, encoder=self.target_qf_twin_encoder, head=self.qf_twin
                ),
            )

        # Compute values for the current observations.
        output[Columns.VF_PREDS] = self.compute_values(batch_curr)
        # The values of the next observations are needed for the critic loss.
        output[VF_PREDS_NEXT] = self.compute_values(batch_next)

        return output

    @override(ValueFunctionAPI)
    def compute_values(
        self,
        batch: Dict[str, Any],
        embeddings: Optional[Any] = None,
    ) -> TensorType:
        # If no embeddings are provided make a forward pass on the encoder.
        if embeddings is None:
            embeddings = self.vf_encoder(batch)[ENCODER_OUT]

        # Value head.
        vf_out = self.vf(embeddings)
        # Squeeze out last dimension (single node value head).
        return vf_out.squeeze(-1)
