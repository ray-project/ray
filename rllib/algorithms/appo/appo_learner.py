import abc
from collections import defaultdict
from typing import Any, Dict, Optional

from ray.rllib.algorithms.appo.appo import APPOConfig
from ray.rllib.algorithms.appo.utils import CircularBuffer
from ray.rllib.algorithms.impala.impala_learner import IMPALALearner
from ray.rllib.core.learner.learner import Learner
from ray.rllib.core.learner.utils import update_target_network
from ray.rllib.core.rl_module.apis import TargetNetworkAPI, ValueFunctionAPI
from ray.rllib.core.rl_module.multi_rl_module import MultiRLModuleSpec
from ray.rllib.core.rl_module.rl_module import RLModuleSpec
from ray.rllib.utils.annotations import override
from ray.rllib.utils.lambda_defaultdict import LambdaDefaultDict
from ray.rllib.utils.metrics import (
    LAST_TARGET_UPDATE_TS,
    NUM_ENV_STEPS_TRAINED_LIFETIME,
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
        self._last_update_ts_by_mid = defaultdict(int)

        self._learner_thread_in_queue = CircularBuffer(
            num_batches=self.config.circular_buffer_num_batches,
            iterations_per_batch=self.config.circular_buffer_iterations_per_batch,
        )

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
        module_spec: RLModuleSpec,
        config_overrides: Optional[Dict] = None,
        new_should_module_be_updated: Optional[ShouldModuleBeUpdatedFn] = None,
    ) -> MultiRLModuleSpec:
        marl_spec = super().add_module(
            module_id=module_id,
            module_spec=module_spec,
            config_overrides=config_overrides,
            new_should_module_be_updated=new_should_module_be_updated,
        )
        # Create target networks for added Module, if applicable.
        if isinstance(self.module[module_id].unwrapped(), TargetNetworkAPI):
            self.module[module_id].unwrapped().make_target_networks()
        return marl_spec

    @override(IMPALALearner)
    def remove_module(self, module_id: str) -> MultiRLModuleSpec:
        marl_spec = super().remove_module(module_id)
        self.curr_kl_coeffs_per_module.pop(module_id)
        return marl_spec

    @override(Learner)
    def after_gradient_based_update(self, *, timesteps: Dict[str, Any]) -> None:
        """Updates the target Q Networks."""
        super().after_gradient_based_update(timesteps=timesteps)

        # TODO (sven): Maybe we should have a `after_gradient_based_update`
        #  method per module?
        curr_timestep = timesteps.get(NUM_ENV_STEPS_TRAINED_LIFETIME, 0)
        for module_id, module in self.module._rl_modules.items():
            config = self.config.get_config_for_module(module_id)

            if isinstance(module.unwrapped(), TargetNetworkAPI) and (
                curr_timestep - self._last_update_ts_by_mid[module_id]
                >= (
                    config.target_network_update_freq
                    * config.circular_buffer_num_batches
                    * config.circular_buffer_iterations_per_batch
                    * config.train_batch_size_per_learner
                )
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
                self.metrics.log_value(
                    (module_id, NUM_TARGET_UPDATES), 1, reduce="lifetime_sum"
                )
                # Update the (single-value -> window=1) last updated timestep metric.
                self._last_update_ts_by_mid[module_id] = curr_timestep
                self.metrics.log_value(
                    (module_id, LAST_TARGET_UPDATE_TS), curr_timestep, reduce="max"
                )

            if (
                config.use_kl_loss
                and self.metrics.peek((module_id, NUM_MODULE_STEPS_TRAINED), default=0)
                > 0
            ):
                self._update_module_kl_coeff(module_id=module_id, config=config)

    @classmethod
    @override(Learner)
    def rl_module_required_apis(cls) -> list[type]:
        # In order for a PPOLearner to update an RLModule, it must implement the
        # following APIs:
        return [TargetNetworkAPI, ValueFunctionAPI]

    @abc.abstractmethod
    def _update_module_kl_coeff(self, module_id: ModuleID, config: APPOConfig) -> None:
        """Dynamically update the KL loss coefficients of each module.

        The update is completed using the mean KL divergence between the action
        distributions current policy and old policy of each module. That action
        distribution is computed during the most recent update/call to `compute_loss`.

        Args:
            module_id: The module whose KL loss coefficient to update.
            config: The AlgorithmConfig specific to the given `module_id`.
        """


AppoLearner = APPOLearner
