import abc

from typing import Any, Dict, TYPE_CHECKING

from ray.rllib.core.learner.learner import Learner
from ray.rllib.utils.annotations import override
from ray.rllib.utils.metrics import LAST_TARGET_UPDATE_TS, NUM_TARGET_UPDATES
from ray.rllib.utils.typing import ModuleID

if TYPE_CHECKING:
    from ray.rllib.algorithms.dqn.dqn import DQNConfig

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
TD_ERROR_KEY = "td_error"
TD_ERROR_MEAN_KEY = "td_error_mean"


class DQNRainbowLearner(Learner):
    @override(Learner)
    def additional_update_for_module(
        self,
        *,
        module_id: ModuleID,
        config: "DQNConfig",
        timestep: int,
        last_update: int,
        **kwargs
    ) -> Dict[str, Any]:
        """Updates the target Q Networks."""
        results = super().additional_update_for_module(
            module_id=module_id,
            config=config,
            timestep=timestep,
        )

        # TODO (Sven): APPO uses `config.target_update_frequency`. Can we
        # choose a standard here?
        if (timestep - last_update) >= config.target_network_update_freq:
            self._update_module_target_networks(module_id, config)
            results[NUM_TARGET_UPDATES] = 1
            results[LAST_TARGET_UPDATE_TS] = timestep
        else:
            results[NUM_TARGET_UPDATES] = 0
            results[LAST_TARGET_UPDATE_TS] = last_update

        return results

    @abc.abstractmethod
    def _update_module_target_networks(
        self, module_id: ModuleID, config: "DQNConfig"
    ) -> None:
        """Update the target Q network(s) of each module with the current Q network.

        The update is made via Polyak averaging.

        Args:
            module_id: The module ID whose target Q network(s) should be updated.
            config: The `AlgorithmConfig` specific in the given `module_id`.
        """

    @abc.abstractmethod
    def _reset_noise(self) -> None:
        """Resets the noise in the `Algorithm.training_step()`

        Note, this can be overridden by the user to reset the noise at different
        points in the training loop.
        """
