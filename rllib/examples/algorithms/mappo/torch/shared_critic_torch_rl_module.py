import typing
from typing import Any, Optional

from ray.rllib.core.models.base import ENCODER_OUT
from ray.rllib.core.rl_module.apis.value_function_api import ValueFunctionAPI
from ray.rllib.core.rl_module.torch import TorchRLModule
from ray.rllib.utils.annotations import override
from ray.rllib.utils.framework import try_import_torch
from ray.rllib.utils.typing import TensorType
from ray.util.annotations import DeveloperAPI

from ray.rllib.examples.algorithms.mappo.shared_critic_rl_module import (
    SharedCriticRLModule,
)
from ray.rllib.examples.algorithms.mappo.shared_critic_catalog import (
    SharedCriticCatalog,
)

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
        batch: typing.Dict[str, Any],
        embeddings: Optional[Any] = None,
    ) -> TensorType:
        if embeddings is None:
            embeddings = self.encoder(batch)[ENCODER_OUT]
        vf_out = self.vf(embeddings)
        # Don't squeeze out last dimension (multi node value head).
        return vf_out
