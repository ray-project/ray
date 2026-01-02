"""
TQC Learner base class.

Extends SAC Learner with quantile-specific loss computation.
"""

from ray.rllib.algorithms.sac.sac_learner import SACLearner
from ray.rllib.core.learner.learner import Learner
from ray.rllib.utils.annotations import override
from ray.rllib.utils.typing import ModuleID

# Loss keys for TQC
QF_LOSS_KEY = "qf_loss"
QF_MEAN_KEY = "qf_mean"
QF_MAX_KEY = "qf_max"
QF_MIN_KEY = "qf_min"
QUANTILES_KEY = "quantiles"
TD_ERROR_MEAN_KEY = "td_error_mean"


class TQCLearner(SACLearner):
    """Base Learner class for TQC algorithm.

    TQC extends SAC with distributional critics using quantile regression.
    The main differences are:
    - Uses quantile Huber loss instead of standard Huber/MSE loss
    - Computes target Q-values by sorting and truncating top quantiles
    """

    @override(Learner)
    def build(self) -> None:
        """Builds the TQC learner."""
        # Call parent build (handles alpha/entropy coefficient)
        super().build()

    def _get_n_target_quantiles(self, module_id: ModuleID) -> int:
        """Returns the number of target quantiles after truncation.

        Args:
            module_id: The module ID.

        Returns:
            Number of quantiles to use for target computation.
        """
        config = self.config.get_config_for_module(module_id)
        n_quantiles = config.n_quantiles
        n_critics = config.n_critics
        top_quantiles_to_drop = config.top_quantiles_to_drop_per_net

        total_quantiles = n_quantiles * n_critics
        quantiles_to_drop = top_quantiles_to_drop * n_critics

        return total_quantiles - quantiles_to_drop
