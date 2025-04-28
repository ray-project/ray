import abc
from typing import Any, Dict

from ray.rllib.algorithms.bc.bc_catalog import BCCatalog
from ray.rllib.core.columns import Columns
from ray.rllib.core.models.base import ENCODER_OUT
from ray.rllib.core.rl_module.rl_module import RLModule
from ray.rllib.core.rl_module.torch.torch_rl_module import TorchRLModule
from ray.rllib.utils.annotations import override
from ray.util.annotations import DeveloperAPI


@DeveloperAPI
class DefaultBCTorchRLModule(TorchRLModule, abc.ABC):
    """The default TorchRLModule used, if no custom RLModule is provided.

    Builds an encoder net based on the observation space.
    Builds a pi head based on the action space.

    Passes observations from the input batch through the encoder, then the pi head to
    compute action logits.
    """

    def __init__(self, *args, **kwargs):
        catalog_class = kwargs.pop("catalog_class", None)
        if catalog_class is None:
            catalog_class = BCCatalog
        super().__init__(*args, **kwargs, catalog_class=catalog_class)

    @override(RLModule)
    def setup(self):
        # Build model components (encoder and pi head) from catalog.
        super().setup()
        self._encoder = self.catalog.build_encoder(framework=self.framework)
        self._pi_head = self.catalog.build_pi_head(framework=self.framework)

    @override(TorchRLModule)
    def _forward(self, batch: Dict, **kwargs) -> Dict[str, Any]:
        """Generic BC forward pass (for all phases of training/evaluation)."""
        # Encoder embeddings.
        encoder_outs = self._encoder(batch)
        # Action dist inputs.
        return {
            Columns.ACTION_DIST_INPUTS: self._pi_head(encoder_outs[ENCODER_OUT]),
        }
