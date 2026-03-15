from typing import Any, Dict

from ray.rllib.algorithms.ppo.ppo import (
    LEARNER_RESULTS_CURR_ENTROPY_COEFF_KEY,
    LEARNER_RESULTS_KL_KEY,
)
from ray.rllib.algorithms.ppo.ppo_learner import PPOLearner
from ray.rllib.connectors.learner import GeneralAdvantageEstimation
from ray.rllib.core.learner.learner import Learner
from ray.rllib.examples.algorithms.mappo.connectors.general_advantage_estimation import (
    SHARED_CRITIC_ID,
    MAPPOGAEConnector,
)
from ray.rllib.utils.annotations import override
from ray.rllib.utils.metrics import NUM_ENV_STEPS_SAMPLED_LIFETIME
from ray.rllib.utils.numpy import convert_to_numpy


class MAPPOLearner(PPOLearner):
    """Learner for MAPPO: extends PPOLearner with shared-critic GAE and
    per-module update logic that skips the shared critic for policy-specific
    metrics (entropy scheduling, KL coefficient adaptation).
    """

    @override(PPOLearner)
    def build(self) -> None:
        super().build()
        if (
            self._learner_connector is not None
            and self.config.add_default_connectors_to_learner_pipeline
        ):
            # PPOLearner.build() appended a per-module GeneralAdvantageEstimation.
            # Replace it with MAPPO's shared-critic variant.
            self._learner_connector.remove(GeneralAdvantageEstimation)
            self._learner_connector.append(
                MAPPOGAEConnector(
                    gamma=self.config.gamma,
                    lambda_=self.config.lambda_,
                )
            )

    @classmethod
    @override(PPOLearner)
    def rl_module_required_apis(cls) -> list[type]:
        # MAPPO actors don't implement ValueFunctionAPI; only the shared
        # critic does, so we can't enforce it globally.
        return []

    @override(PPOLearner)
    def after_gradient_based_update(
        self,
        *,
        timesteps: Dict[str, Any],
    ) -> None:
        # Call Learner (grandparent) directly -- we replicate PPOLearner's
        # per-module loop but skip the shared critic module.
        Learner.after_gradient_based_update(self, timesteps=timesteps)

        for module_id in self.module._rl_modules:
            if module_id == SHARED_CRITIC_ID:
                continue
            config = self.config.get_config_for_module(module_id)

            new_entropy_coeff = self.entropy_coeff_schedulers_per_module[
                module_id
            ].update(timestep=timesteps.get(NUM_ENV_STEPS_SAMPLED_LIFETIME, 0))
            self.metrics.log_value(
                (module_id, LEARNER_RESULTS_CURR_ENTROPY_COEFF_KEY),
                new_entropy_coeff,
                window=1,
            )
            if (
                config.use_kl_loss
                and (module_id, LEARNER_RESULTS_KL_KEY) in self.metrics
            ):
                kl_loss = convert_to_numpy(
                    self.metrics.peek((module_id, LEARNER_RESULTS_KL_KEY))
                )
                self._update_module_kl_coeff(
                    module_id=module_id,
                    config=config,
                    kl_loss=kl_loss,
                )
