import abc
from typing import Any, Dict

from ray.rllib.core.learner.learner import Learner
from ray.rllib.connectors.common.add_observations_from_episodes_to_batch import (
    AddObservationsFromEpisodesToBatch,
)
from ray.rllib.connectors.learner.add_next_observations_from_episodes_to_train_batch import (  # noqa
    AddNextObservationsFromEpisodesToTrainBatch,
)
from ray.rllib.utils.annotations import (
    override,
    OverrideToImplementCustomLogic_CallToSuperRecommended,
)
from ray.rllib.utils.metrics import (
    LAST_TARGET_UPDATE_TS,
    NUM_ENV_STEPS_SAMPLED_LIFETIME,
    NUM_TARGET_UPDATES,
)


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


class DQNRainbowLearner(Learner):
    @OverrideToImplementCustomLogic_CallToSuperRecommended
    @override(Learner)
    def build(self) -> None:
        super().build()

        # Initially sync target networks (w/ tau=1.0 -> full overwrite).
        self.module.foreach_module(
            lambda mid, module: module.sync_target_networks(tau=1.0)
        )

        # Prepend a NEXT_OBS from episodes to train batch connector piece (right
        # after the observation default piece).
        if self.config.add_default_connectors_to_learner_pipeline:
            self._learner_connector.insert_after(
                AddObservationsFromEpisodesToBatch,
                AddNextObservationsFromEpisodesToTrainBatch(),
            )

    @override(Learner)
    def after_gradient_based_update(self, *, timesteps: Dict[str, Any]) -> None:
        """Updates the target Q Networks."""
        super().after_gradient_based_update(timesteps=timesteps)

        timestep = timesteps.get(NUM_ENV_STEPS_SAMPLED_LIFETIME, 0)

        # TODO (sven): Maybe we should have a `after_gradient_based_update`
        #  method per module?
        for module_id, module in self.module._rl_modules.items():
            config = self.config.get_config_for_module(module_id)
            # TODO (Sven): APPO uses `config.target_update_frequency`. Can we
            #  choose a standard here?
            last_update_ts_key = (module_id, LAST_TARGET_UPDATE_TS)
            if (
                timestep - self.metrics.peek(last_update_ts_key, default=0)
                >= config.target_network_update_freq
            ):
                module.sync_target_networks(tau=config.tau)
                # Increase lifetime target network update counter by one.
                self.metrics.log_value((module_id, NUM_TARGET_UPDATES), 1, reduce="sum")
                # Update the (single-value -> window=1) last updated timestep metric.
                self.metrics.log_value(last_update_ts_key, timestep, window=1)

    @abc.abstractmethod
    def _reset_noise(self) -> None:
        """Resets the noise in the `Algorithm.training_step()`

        Note, this can be overridden by the user to reset the noise at different
        points in the training loop.
        """
