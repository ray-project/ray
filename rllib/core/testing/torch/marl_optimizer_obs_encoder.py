from ray.rllib.core.optim.marl_optimizer import MultiAgentRLOptimizer

from typing import Any, Mapping, Union
from ray.rllib.core.rl_module.rl_module import ModuleID
from ray.rllib.core.optim.rl_optimizer import RLOptimizer
from ray.rllib.utils.annotations import override
from ray.rllib.utils.typing import TensorType


class MARLOptimWObsEncoder(MultiAgentRLOptimizer):
    def __init__(
        self, rl_optimizers: Mapping[ModuleID, RLOptimizer], encoder_optimizer
    ):
        super().__init__(rl_optimizers)
        self.encoder_optimizer = encoder_optimizer

    @override(MultiAgentRLOptimizer)
    def compute_loss(
        self,
        fwd_out: Mapping[ModuleID, Mapping[str, Any]],
        batch: Mapping[ModuleID, Mapping[str, Any]],
    ) -> Union[TensorType, Mapping[str, Any]]:
        loss_rl_modules = super().compute_loss(fwd_out, batch)
        return loss_rl_modules
