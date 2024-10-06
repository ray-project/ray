from typing import Any, Dict, Optional

from ray.rllib.core.columns import Columns
from ray.rllib.core.models.base import ACTOR, CRITIC, ENCODER_OUT
from ray.rllib.core.rl_module.apis.value_function_api import ValueFunctionAPI
from ray.rllib.core.rl_module.rl_module import RLModule
from ray.rllib.core.rl_module.torch import TorchRLModule
from ray.rllib.utils.annotations import override
from ray.rllib.utils.framework import try_import_torch
from ray.rllib.utils.typing import TensorType

torch, nn = try_import_torch()


class MARWILTorchRLModule(TorchRLModule):
    @override(RLModule)
    def setup(self):
        # Build models from catalog.
        self.encoder = self.catalog.build_actor_critic_encoder(framework=self.framework)
        self.pi = self.catalog.build_pi_head(framework=self.framework)

        # Build the value head.
        self.vf = self.catalog.build_vf_head(framework=self.framework)

    @override(RLModule)
    def _forward(self, batch: Dict[str, Any], **kwargs) -> Dict[str, Any]:
        """Default forward pass (used for inference and exploration)."""
        output = {}

        # Encoder forward pass.
        encoder_outs = self.encoder(batch)
        if Columns.STATE_OUT in encoder_outs:
            output[Columns.STATE_OUT] = encoder_outs[Columns.STATE_OUT]

        # Actions.
        output[Columns.ACTION_DIST_INPUTS] = self.pi(encoder_outs[ENCODER_OUT][ACTOR])

        return output

    def _forward_train(self, batch: Dict[str, Any], **kwargs) -> Dict[str, Any]:
        """Train forward pass (keep embeddings for possible shared value func. call)."""
        output = {}
        encoder_outs = self.encoder(batch)
        output[Columns.EMBEDDINGS] = encoder_outs[ENCODER_OUT][CRITIC]
        if Columns.STATE_OUT in encoder_outs:
            output[Columns.STATE_OUT] = encoder_outs[Columns.STATE_OUT]
        output[Columns.ACTION_DIST_INPUTS] = self.pi(encoder_outs[ENCODER_OUT][ACTOR])
        return output

    @override(ValueFunctionAPI)
    def compute_values(
        self,
        batch: Dict[str, Any],
        embeddings: Optional[Any] = None,
    ) -> TensorType:
        if embeddings is None:
            # Separate vf-encoder.
            if hasattr(self.encoder, "critic_encoder"):
                if self.is_stateful():
                    # The recurrent encoders expect a `(state_in, h)`  key in the
                    # input dict while the key returned is `(state_in, critic, h)`.
                    batch[Columns.STATE_IN] = batch[Columns.STATE_IN][CRITIC]
                embeddings = self.encoder.critic_encoder(batch)[ENCODER_OUT]
            # Shared encoder.
            else:
                embeddings = self.encoder(batch)[ENCODER_OUT][CRITIC]
        # Value head.
        vf_out = self.vf(embeddings)
        # Squeeze out last dimension (single node value head).
        return vf_out.squeeze(-1)
