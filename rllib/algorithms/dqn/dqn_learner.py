from typing import Any, Dict, Optional

from ray.rllib.connectors.common.add_observations_from_episodes_to_batch import (
    AddObservationsFromEpisodesToBatch,
)
from ray.rllib.connectors.learner.add_next_observations_from_episodes_to_train_batch import (  # noqa
    AddNextObservationsFromEpisodesToTrainBatch,
)
from ray.rllib.core.learner.learner import Learner
from ray.rllib.core.learner.utils import update_target_network
from ray.rllib.core.rl_module.apis import QNetAPI, TargetNetworkAPI
from ray.rllib.core.rl_module.multi_rl_module import MultiRLModuleSpec
from ray.rllib.core.rl_module.rl_module import RLModuleSpec
from ray.rllib.utils.annotations import (
    override,
    OverrideToImplementCustomLogic_CallToSuperRecommended,
)
from ray.rllib.utils.metrics import (
    LAST_TARGET_UPDATE_TS,
    NUM_ENV_STEPS_SAMPLED_LIFETIME,
    NUM_TARGET_UPDATES,
)
from ray.rllib.utils.typing import ModuleID, ShouldModuleBeUpdatedFn


# Now, this is double defined: In `SACRLModule` and here. I would keep it here
# or push it into the `Learner` as these are recurring keys in RL.
ATOMS = "atoms"
QF_LOSS_KEY = "qf_loss"
QF_LOGITS = "qf_logits"
QF_MEAN_KEY = "qf_mean"
QF_MAX_KEY = "qf_max"
QF_MIN_KEY = "qf_min"
QF_NEXT_PREDS = "qf_next_preds"
QF_TARGET_NEXT_PREDS = "qf_target_next_preds"
QF_TARGET_NEXT_PROBS = "qf_target_next_probs"
QF_PREDS = "qf_preds"
QF_PROBS = "qf_probs"
TD_ERROR_MEAN_KEY = "td_error_mean"


class DQNLearner(Learner):
    @OverrideToImplementCustomLogic_CallToSuperRecommended
    @override(Learner)
    def build(self) -> None:
        super().build()

        # Make target networks.
        self.module.foreach_module(
            lambda mid, mod: (
                mod.make_target_networks()
                if isinstance(mod, TargetNetworkAPI)
                else None
            )
        )

        # Prepend the "add-NEXT_OBS-from-episodes-to-train-batch" connector piece (right
        # after the corresponding "add-OBS-..." default piece).
        self._learner_connector.insert_after(
            AddObservationsFromEpisodesToBatch,
            AddNextObservationsFromEpisodesToTrainBatch(),
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

    @override(Learner)
    def after_gradient_based_update(self, *, timesteps: Dict[str, Any]) -> None:
        """Updates the target Q Networks."""
        super().after_gradient_based_update(timesteps=timesteps)

        timestep = timesteps.get(NUM_ENV_STEPS_SAMPLED_LIFETIME, 0)

        # TODO (sven): Maybe we should have a `after_gradient_based_update`
        #  method per module?
        for module_id, module in self.module._rl_modules.items():
            config = self.config.get_config_for_module(module_id)
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

    @classmethod
    @override(Learner)
    def rl_module_required_apis(cls) -> list[type]:
        # In order for a PPOLearner to update an RLModule, it must implement the
        # following APIs:
        return [QNetAPI, TargetNetworkAPI]
