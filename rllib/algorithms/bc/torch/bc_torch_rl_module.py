from typing import Any, Dict

from ray.rllib.core import Columns
from ray.rllib.core.models.base import ENCODER_OUT
from ray.rllib.core.rl_module.rl_module import RLModule
from ray.rllib.core.rl_module.torch.torch_rl_module import TorchRLModule
from ray.rllib.utils.annotations import override


class BCTorchRLModule(TorchRLModule):
    @override(RLModule)
    def setup(self):
        # __sphinx_doc_begin__
        # Build models from catalog
        self.encoder = self.catalog.build_encoder(framework=self.framework)
        self.pi = self.catalog.build_pi_head(framework=self.framework)

    @override(RLModule)
    def _forward(self, batch: Dict, **kwargs) -> Dict[str, Any]:
        """Generic BC forward pass (for all phases of training/evaluation)."""
        output = {}

        # State encodings.
        encoder_outs = self.encoder(batch)
        if Columns.STATE_OUT in encoder_outs:
            output[Columns.STATE_OUT] = encoder_outs[Columns.STATE_OUT]

        # Actions.
        action_logits = self.pi(encoder_outs[ENCODER_OUT])
        output[Columns.ACTION_DIST_INPUTS] = action_logits

        return output
