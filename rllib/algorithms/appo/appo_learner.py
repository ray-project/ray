import abc
from collections import defaultdict
from dataclasses import dataclass
from typing import Any, Dict, Mapping

from ray.rllib.algorithms.impala.impala_learner import (
    ImpalaLearner,
    ImpalaHyperparameters,
)
from ray.rllib.core.rl_module.marl_module import ModuleID
from ray.rllib.utils.annotations import override
from ray.rllib.utils.typing import TensorType


LEARNER_RESULTS_KL_KEY = "mean_kl_loss"
LEARNER_RESULTS_CURR_KL_COEFF_KEY = "curr_kl_coeff"
OLD_ACTION_DIST_KEY = "old_action_dist"
OLD_ACTION_DIST_LOGITS_KEY = "old_action_dist_logits"


@dataclass
class AppoHyperparameters(ImpalaHyperparameters):
    """Hyperparameters for the APPOLearner sub-classes (framework specific).

    These should never be set directly by the user. Instead, use the APPOConfig
    class to configure your algorithm.
    See `ray.rllib.algorithms.appo.appo::APPOConfig::training()` for more details on the
    individual properties.
    """

    use_kl_loss: bool = None
    kl_coeff: float = None
    kl_target: float = None
    clip_param: float = None
    tau: float = None


class AppoLearner(ImpalaLearner):
    """Adds KL coeff updates via `additional_updates_per_module()` to Impala logic.

    Framework-specific sub-classes must override `_update_module_target_networks()`
    and `_update_module_kl_coeff()`
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # We need to make sure kl_coeff are available as framework tensors that are
        # registered as part of the graph so that upon update the graph can be updated
        # (e.g. in TF with eager tracing).
        self.curr_kl_coeffs_per_module = defaultdict(
            lambda: self._get_kl_variable(self.hps.kl_coeff)
        )

    @override(ImpalaLearner)
    def remove_module(self, module_id: str):
        super().remove_module(module_id)
        self.curr_kl_coeffs_per_module.pop(module_id)

    @override(ImpalaLearner)
    def additional_update_per_module(
        self, module_id: ModuleID, sampled_kls: Dict[ModuleID, float], **kwargs
    ) -> Mapping[str, Any]:
        """Updates the target networks and KL loss coefficients (per module).

        Args:
            module_id:
        """
        self._update_module_target_networks(module_id)
        if self.hps.use_kl_loss:
            self._update_module_kl_coeff(module_id, sampled_kls)
        return {}

    @abc.abstractmethod
    def _update_module_target_networks(self, module_id: ModuleID) -> None:
        """Update the target policy of each module with the current policy.

        Do that update via polyak averaging.

        Args:
            module_id: The module ID, whose target network(s) need to be updated.
        """

    @abc.abstractmethod
    def _update_module_kl_coeff(
        self, module_id: ModuleID, sampled_kls: Dict[ModuleID, float]
    ) -> None:
        """Dynamically update the KL loss coefficients of each module with.

        The update is completed using the mean KL divergence between the action
        distributions current policy and old policy of each module. That action
        distribution is computed during the most recent update/call to `compute_loss`.

        Args:
            module_id: The module whose KL loss coefficient to update.
            sampled_kls: Mapping from Module ID to this module's KL divergence between
                the action distributions of the current (most recently updated) module
                and the old module version.
        """

    @abc.abstractmethod
    def _get_kl_variable(self, value: float) -> TensorType:
        """Returns the kl_coeff (framework specific) tensor variable.

        This is a framework specific method that should be implemented by the
        framework specific sub-class.

        Args:
            value: The initial value for the kl_coeff variable.
        """
