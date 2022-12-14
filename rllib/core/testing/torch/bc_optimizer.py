from typing import Any, List, Mapping

import torch

from ray.rllib.core.optim.rl_optimizer import RLOptimizer
from ray.rllib.utils.annotations import override
from ray.rllib.utils.nested_dict import NestedDict


class BCTorchOptimizer(RLOptimizer):
    def __init__(self, module, config):
        super().__init__(module, config)

    @override(RLOptimizer)
    def compute_loss(
        self, batch: NestedDict, fwd_out: Mapping[str, Any]
    ) -> Mapping[str, Any]:
        """Compute a loss"""
        action_dist = fwd_out["action_dist"]
        actions = batch["actions"]
        return -action_dist.log_prob(actions.view(-1)).mean()

    @override(RLOptimizer)
    def _configure_optimizers(self) -> List[torch.optim.Optimizer]:
        return {
            "module": torch.optim.Adam(
                self.module.parameters(), lr=self._config.get("lr", 1e-3)
            )
        }

    @override(RLOptimizer)
    def get_state(self):
        return {key: optim.state_dict() for key, optim in self.get_optimizers().items()}

    @override(RLOptimizer)
    def set_state(self, state: Mapping[Any, Any]) -> None:
        assert set(state.keys()) == set(self.get_state().keys()) or not state
        for key, optim_dict in state.items():
            self.get_optimizers()[key].load_state_dict(optim_dict)
