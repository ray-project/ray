from typing import Any, Dict, Optional

from ray.rllib.core.models.base import ENCODER_OUT
from ray.rllib.core.rl_module.apis.value_function_api import ValueFunctionAPI
from ray.rllib.core.rl_module.torch import TorchRLModule
from ray.rllib.examples.algorithms.mappo.shared_critic_catalog import (
    SharedCriticCatalog,
)
from ray.rllib.examples.algorithms.mappo.shared_critic_rl_module import (
    SharedCriticRLModule,
)
from ray.rllib.utils.annotations import override
from ray.rllib.utils.framework import try_import_torch
from ray.rllib.utils.typing import TensorType
from ray.util.annotations import DeveloperAPI

torch, nn = try_import_torch()


@DeveloperAPI
class SharedCriticTorchRLModule(TorchRLModule, SharedCriticRLModule):
    def __init__(self, *args, **kwargs):
        catalog_class = kwargs.pop("catalog_class", None)
        if catalog_class is None:
            catalog_class = SharedCriticCatalog
        super().__init__(*args, **kwargs, catalog_class=catalog_class)

    @override(ValueFunctionAPI)
    def compute_values(
        self,
        batch: Dict[str, Any],
        embeddings: Optional[Any] = None,
    ) -> TensorType:
        if embeddings is None:
            embeddings = self.encoder(batch)[ENCODER_OUT]
        vf_out = self.vf(embeddings)
        # Multi-agent value head: keep the last dimension (one node per agent).
        return vf_out
