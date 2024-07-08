import abc
from typing import Any, Dict

from ray.rllib.algorithms.appo.appo import APPOConfig
from ray.rllib.algorithms.impala.impala_learner import ImpalaLearner
from ray.rllib.core.learner.learner import Learner
from ray.rllib.utils.annotations import override
from ray.rllib.utils.lambda_defaultdict import LambdaDefaultDict
from ray.rllib.utils.metrics import (
    LAST_TARGET_UPDATE_TS,
    NUM_ENV_STEPS_SAMPLED_LIFETIME,
    NUM_MODULE_STEPS_TRAINED,
    NUM_TARGET_UPDATES,
)
from ray.rllib.utils.schedules.scheduler import Scheduler
from ray.rllib.utils.typing import ModuleID


class AppoLearner(ImpalaLearner):
    """Adds KL coeff updates via `after_gradient_based_update()` to Impala logic.

    Framework-specific sub-classes must override `_update_module_kl_coeff()`.
    """

    @override(ImpalaLearner)
    def build(self):
        super().build()

        # Initially sync target networks (w/ tau=1.0 -> full overwrite).
        self.module.foreach_module(
            lambda mid, module: module.sync_target_networks(tau=1.0)
        )

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

    @override(Learner)
    def after_gradient_based_update(self, *, timesteps: Dict[str, Any]) -> None:
        """Updates the target Q Networks."""
        super().after_gradient_based_update(timesteps=timesteps)

        timestep = timesteps.get(NUM_ENV_STEPS_SAMPLED_LIFETIME, 0)

        # TODO (sven): Maybe we should have a `after_gradient_based_update`
        #  method per module?
        for module_id, module in self.module._rl_modules.items():
            config = self.config.get_config_for_module(module_id)

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

            last_update_ts_key = (module_id, LAST_TARGET_UPDATE_TS)
            # TODO (Sven): DQN uses `config.target_network_update_freq`. Can we
            #  choose a standard here?
            if (
                timestep - self.metrics.peek(last_update_ts_key, default=0)
                >= config.target_update_frequency
            ):
                module.sync_target_networks(tau=config.tau)
                # Increase lifetime target network update counter by one.
                self.metrics.log_value((module_id, NUM_TARGET_UPDATES), 1, reduce="sum")
                # Update the (single-value -> window=1) last updated timestep metric.
                self.metrics.log_value(last_update_ts_key, timestep, window=1)

            if (
                config.use_kl_loss
                and self.metrics.peek((module_id, NUM_MODULE_STEPS_TRAINED), default=0)
                > 0
            ):
                self._update_module_kl_coeff(module_id=module_id, config=config)

    @abc.abstractmethod
    def _update_module_kl_coeff(self, module_id: ModuleID, config: APPOConfig) -> None:
        """Dynamically update the KL loss coefficients of each module with.

        The update is completed using the mean KL divergence between the action
        distributions current policy and old policy of each module. That action
        distribution is computed during the most recent update/call to `compute_loss`.

        Args:
            module_id: The module whose KL loss coefficient to update.
            config: The AlgorithmConfig specific to the given `module_id`.
        """
