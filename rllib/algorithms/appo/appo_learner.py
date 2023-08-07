import abc
from dataclasses import dataclass
from typing import Any, Mapping

from ray.rllib.algorithms.impala.impala_learner import (
    ImpalaLearner,
    ImpalaLearnerHyperparameters,
)
from ray.rllib.core.rl_module.marl_module import ModuleID
from ray.rllib.utils.annotations import override
from ray.rllib.utils.lambda_defaultdict import LambdaDefaultDict
from ray.rllib.utils.metrics import LAST_TARGET_UPDATE_TS, NUM_TARGET_UPDATES
from ray.rllib.utils.schedules.scheduler import Scheduler


LEARNER_RESULTS_KL_KEY = "mean_kl_loss"
LEARNER_RESULTS_CURR_KL_COEFF_KEY = "curr_kl_coeff"
OLD_ACTION_DIST_KEY = "old_action_dist"
OLD_ACTION_DIST_LOGITS_KEY = "old_action_dist_logits"


@dataclass
class AppoLearnerHyperparameters(ImpalaLearnerHyperparameters):
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
    target_update_frequency_ts: int = None


class AppoLearner(ImpalaLearner):
    """Adds KL coeff updates via `additional_update_for_module()` to Impala logic.

    Framework-specific sub-classes must override `_update_module_target_networks()`
    and `_update_module_kl_coeff()`
    """

    @override(ImpalaLearner)
    def build(self):
        super().build()

        # The current kl coefficients per module as (framework specific) tensor
        # variables.
        self.curr_kl_coeffs_per_module: LambdaDefaultDict[
            ModuleID, Scheduler
        ] = LambdaDefaultDict(
            lambda module_id: self._get_tensor_variable(
                self.hps.get_hps_for_module(module_id).kl_coeff
            )
        )

    @override(ImpalaLearner)
    def remove_module(self, module_id: str):
        super().remove_module(module_id)
        self.curr_kl_coeffs_per_module.pop(module_id)

    @override(ImpalaLearner)
    def additional_update_for_module(
        self,
        *,
        module_id: ModuleID,
        hps: AppoLearnerHyperparameters,
        timestep: int,
        last_update: int,
        mean_kl_loss_per_module: dict,
        **kwargs,
    ) -> Mapping[str, Any]:
        """Updates the target networks and KL loss coefficients (per module).

        Args:
            module_id:
        """
        # TODO (avnish) Using steps trained here instead of sampled ... I'm not sure
        #  why the other implementation uses sampled.
        #  The difference in steps sampled/trained is pretty
        #  much always going to be larger than self.config.num_sgd_iter *
        #  self.config.minibatch_buffer_size unless the number of steps collected
        #  is really small. The thing is that the default rollout fragment length
        #  is 50, so the minibatch buffer size * num_sgd_iter is going to be
        #  have to be 50 to even meet the threshold of having delayed target
        #  updates.
        #  We should instead have the target / kl threshold update be based off
        #  of the train_batch_size * some target update frequency * num_sgd_iter.
        results = super().additional_update_for_module(
            module_id=module_id, hps=hps, timestep=timestep
        )

        if (timestep - last_update) >= hps.target_update_frequency_ts:
            self._update_module_target_networks(module_id, hps)
            results[NUM_TARGET_UPDATES] = 1
            results[LAST_TARGET_UPDATE_TS] = timestep
        else:
            results[NUM_TARGET_UPDATES] = 0
            results[LAST_TARGET_UPDATE_TS] = last_update

        if hps.use_kl_loss and module_id in mean_kl_loss_per_module:
            results.update(
                self._update_module_kl_coeff(
                    module_id, hps, mean_kl_loss_per_module[module_id]
                )
            )

        return results

    @abc.abstractmethod
    def _update_module_target_networks(
        self, module_id: ModuleID, hps: AppoLearnerHyperparameters
    ) -> None:
        """Update the target policy of each module with the current policy.

        Do that update via polyak averaging.

        Args:
            module_id: The module ID, whose target network(s) need to be updated.
            hps: The hyperparameters specific to the given `module_id`.
        """

    @abc.abstractmethod
    def _update_module_kl_coeff(
        self, module_id: ModuleID, hps: AppoLearnerHyperparameters, sampled_kl: float
    ) -> Mapping[str, Any]:
        """Dynamically update the KL loss coefficients of each module with.

        The update is completed using the mean KL divergence between the action
        distributions current policy and old policy of each module. That action
        distribution is computed during the most recent update/call to `compute_loss`.

        Args:
            module_id: The module whose KL loss coefficient to update.
            hps: The hyperparameters specific to the given `module_id`.
            sampled_kl: The computed KL loss for the given Module
                (KL divergence between the action distributions of the current
                (most recently updated) module and the old module version).
        """
