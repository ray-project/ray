import abc
from typing import Any, Dict, Optional

from ray.rllib.algorithms.appo.appo import APPOConfig
from ray.rllib.algorithms.impala.impala_learner import IMPALALearner
from ray.rllib.core.learner.learner import Learner
from ray.rllib.core.learner.utils import update_target_network
from ray.rllib.core.rl_module.apis.target_network_api import TargetNetworkAPI
from ray.rllib.core.rl_module.marl_module import MultiAgentRLModuleSpec
from ray.rllib.core.rl_module.rl_module import SingleAgentRLModuleSpec
from ray.rllib.utils.annotations import override
from ray.rllib.utils.lambda_defaultdict import LambdaDefaultDict
from ray.rllib.utils.metrics import (
    LAST_TARGET_UPDATE_TS,
    NUM_ENV_STEPS_SAMPLED_LIFETIME,
    NUM_MODULE_STEPS_TRAINED,
    NUM_TARGET_UPDATES,
)
from ray.rllib.utils.schedules.scheduler import Scheduler
from ray.rllib.utils.typing import ModuleID, ShouldModuleBeUpdatedFn


class APPOLearner(IMPALALearner):
    """Adds KL coeff updates via `after_gradient_based_update()` to IMPALA logic.

    Framework-specific subclasses must override `_update_module_kl_coeff()`.
    """

    @override(IMPALALearner)
    def build(self):
        super().build()

        # Make target networks.
        self.module.foreach_module(
            lambda mid, mod: (
                mod.make_target_networks()
                if isinstance(mod, TargetNetworkAPI)
                else None
            )
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

    @override(Learner)
    def add_module(
        self,
        *,
        module_id: ModuleID,
        module_spec: SingleAgentRLModuleSpec,
        config_overrides: Optional[Dict] = None,
        new_should_module_be_updated: Optional[ShouldModuleBeUpdatedFn] = None,
    ) -> MultiAgentRLModuleSpec:
        marl_spec = super().add_module(module_id=module_id)
        # Create target networks for added Module, if applicable.
        if isinstance(self.module[module_id].unwrapped(), TargetNetworkAPI):
            self.module[module_id].unwrapped().make_target_networks()
        return marl_spec

    @override(IMPALALearner)
    def remove_module(self, module_id: str) -> MultiAgentRLModuleSpec:
        marl_spec = super().remove_module(module_id)
        self.curr_kl_coeffs_per_module.pop(module_id)
        return marl_spec

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
            if timestep - self.metrics.peek(
                last_update_ts_key, default=0
            ) >= config.target_network_update_freq and isinstance(
                module.unwrapped(), TargetNetworkAPI
            ):
                for (
                    main_net,
                    target_net,
                ) in module.unwrapped().get_target_network_pairs():
                    update_target_network(
                        main_net=main_net,
                        target_net=target_net,
                        tau=config.tau,
                    )
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


AppoLearner = APPOLearner
