from typing import Any, Dict

from ray.rllib.core import Columns
from ray.rllib.core.models.base import ENCODER_OUT
from ray.rllib.core.rl_module.rl_module import RLModule
from ray.rllib.core.rl_module.torch.torch_rl_module import TorchRLModule
from ray.rllib.utils.annotations import override


class BCTorchRLModule(TorchRLModule):
    @override(RLModule)
    def setup(self):
        if self.catalog is None and hasattr(self, "_catalog_ctor_error"):
            raise self._catalog_ctor_error
        # __sphinx_doc_begin__
        # Build models from catalog.
        self.encoder = self.catalog.build_encoder(framework=self.framework)
        self.pi = self.catalog.build_pi_head(framework=self.framework)

    @override(RLModule)
    def _forward(self, batch: Dict, **kwargs) -> Dict[str, Any]:
        """Generic BC forward pass (for all phases of training/evaluation)."""
        output = {}

        # Encoder forward pass.
        encoder_outs = self.encoder(batch)
        if Columns.STATE_OUT in encoder_outs:
            output[Columns.STATE_OUT] = encoder_outs[Columns.STATE_OUT]

        # Actions.
        output[Columns.ACTION_DIST_INPUTS] = self.pi(encoder_outs[ENCODER_OUT])

        return output
