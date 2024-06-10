import abc

from ray.rllib.algorithms.appo.appo import APPOConfig
from ray.rllib.algorithms.impala.impala_learner import ImpalaLearner
from ray.rllib.utils.annotations import override
from ray.rllib.utils.lambda_defaultdict import LambdaDefaultDict
from ray.rllib.utils.metrics import LAST_TARGET_UPDATE_TS, NUM_TARGET_UPDATES
from ray.rllib.utils.schedules.scheduler import Scheduler
from ray.rllib.utils.typing import ModuleID


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
                self.config.get_config_for_module(module_id).kl_coeff
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
        config: APPOConfig,
        timestep: int,
        last_update: int,
        mean_kl_loss_per_module: dict,
        **kwargs,
    ) -> None:
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
        super().additional_update_for_module(
            module_id=module_id, config=config, timestep=timestep
        )

        # TODO (Sven): DQN uses `config.target_network_update_freq`. Can we
        #  choose a standard here?
        last_update_ts_key = (module_id, LAST_TARGET_UPDATE_TS)
        if (
            timestep - self.metrics.peek(last_update_ts_key, default=0)
            >= config.target_update_frequency
        ):
            self._update_module_target_networks(module_id, config)
            # Increase lifetime target network update counter by one.
            self.metrics.log_value((module_id, NUM_TARGET_UPDATES), 1, reduce="sum")
            # Update the (single-value -> window=1) last updated timestep metric.
            self.metrics.log_value(last_update_ts_key, timestep, window=1)

        if config.use_kl_loss and module_id in mean_kl_loss_per_module:
            self._update_module_kl_coeff(
                module_id, config, mean_kl_loss_per_module[module_id]
            )

    @abc.abstractmethod
    def _update_module_target_networks(
        self, module_id: ModuleID, config: APPOConfig
    ) -> None:
        """Update the target policy of each module with the current policy.

        Do that update via polyak averaging.

        Args:
            module_id: The module ID, whose target network(s) need to be updated.
            config: The AlgorithmConfig specific to the given `module_id`.
        """

    @abc.abstractmethod
    def _update_module_kl_coeff(
        self, module_id: ModuleID, config: APPOConfig, sampled_kl: float
    ) -> None:
        """Dynamically update the KL loss coefficients of each module with.

        The update is completed using the mean KL divergence between the action
        distributions current policy and old policy of each module. That action
        distribution is computed during the most recent update/call to `compute_loss`.

        Args:
            module_id: The module whose KL loss coefficient to update.
            config: The AlgorithmConfig specific to the given `module_id`.
            sampled_kl: The computed KL loss for the given Module
                (KL divergence between the action distributions of the current
                (most recently updated) module and the old module version).
        """
