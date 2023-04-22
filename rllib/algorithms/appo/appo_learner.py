import abc
from collections import defaultdict
from dataclasses import dataclass
from typing import Any, Dict, Mapping

import numpy as np

from ray.rllib.algorithms.impala.impala_learner import (
    ImpalaLearner,
    ImpalaHyperparameters,
)
from ray.rllib.core.rl_module.marl_module import ModuleID
from ray.rllib.utils.annotations import override
from ray.rllib.utils.framework import get_variable


LEARNER_RESULTS_KL_KEY = "mean_kl_loss"
LEARNER_RESULTS_CURR_KL_COEFF_KEY = "curr_kl_coeff"
OLD_ACTION_DIST_KEY = "old_action_dist"
OLD_ACTION_DIST_LOGITS_KEY = "old_action_dist_logits"


@dataclass
class AppoHyperparameters(ImpalaHyperparameters):
    """Hyper-parameters for APPO.

    Attributes:
        rollout_frag_or_episode_len: The length of a rollout fragment or episode.
            Used when making SampleBatches time major for computing loss.
        recurrent_seq_len: The length of a recurrent sequence. Used when making
            SampleBatches time major for computing loss.
        discount_factor: The discount factor to use for computing returns.
        vtrace_clip_rho_threshold: The rho threshold to use for clipping the
            importance weights.
        vtrace_clip_pg_rho_threshold: The rho threshold to use for clipping the
            importance weights when computing the policy_gradient loss.
        vtrace_drop_last_ts: Whether to drop the last timestep when computing the loss.
            This is useful for stabilizing the loss.
            NOTE: This shouldn't be True when training on environments where the rewards
            come at the end of the episode.
        vf_loss_coeff: The amount to weight the value function loss by when computing
            the total loss.
        entropy_coeff: The amount to weight the average entropy of the actions in the
            SampleBatch towards the total_loss for module updates. The higher this
            coefficient, the more that the policy network will be encouraged to output
            distributions with higher entropy/std deviation, which will encourage
            greater exploration.
        kl_target: The target kl divergence loss coefficient to use for the KL loss.
        kl_coeff: The coefficient to weight the KL divergence between the old policy
            and the target policy towards the total loss for module updates.
        tau: The factor by which to update the target policy network towards
                the current policy network. Can range between 0 and 1.
                e.g. updated_param = tau * current_param + (1 - tau) * target_param

    """

    use_kl_loss: bool = False
    kl_target: float = 0.01
    kl_coeff: float = 0.1
    clip_param: float = 0.2
    tau: float = 1.0


class AppoLearner(ImpalaLearner):
    """Adds KL coeff updates via `additional_updates_per_module()` to Impala logic.

    Framework-specific sub-classes must override `_update_module_target_networks()`
    and `_update_module_kl_coeff()`
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # Create framework-specific variables (simple python vars for torch).
        self.kl_coeffs = defaultdict(
            lambda: get_variable(
                self._hps.kl_coeff,
                framework=self.framework,
                trainable=False,
                dtype=np.float32,
            )
        )

    @override(ImpalaLearner)
    def remove_module(self, module_id: str):
        super().remove_module(module_id)
        self.kl_coeffs.pop(module_id)

    @override(ImpalaLearner)
    def additional_update_per_module(
        self, module_id: ModuleID, sampled_kls: Dict[ModuleID, float], **kwargs
    ) -> Mapping[str, Any]:
        """Updates the target networks and KL loss coefficients (per module).

        Args:
            module_id:
        """
        self._update_module_target_networks(module_id)
        if self._hps.use_kl_loss:
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
            sampled_kls: The KL divergence between the action distributions of
                the current policy and old policy of each module.

        """
