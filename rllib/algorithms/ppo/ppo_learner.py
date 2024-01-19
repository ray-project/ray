import abc
from typing import Any, Dict

import numpy as np

from ray.rllib.algorithms.ppo.ppo import (
    LEARNER_RESULTS_CURR_ENTROPY_COEFF_KEY,
    PPOConfig,
)
from ray.rllib.core.learner.learner import Learner
from ray.rllib.evaluation.postprocessing import Postprocessing
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.utils.annotations import override
from ray.rllib.utils.lambda_defaultdict import LambdaDefaultDict
from ray.rllib.utils.numpy import convert_to_numpy
from ray.rllib.utils.postprocessing.value_predictions import compute_value_targets
from ray.rllib.utils.postprocessing.episodes import (
    add_one_ts_to_episodes_and_truncate,
    remove_last_ts_from_data,
    remove_last_ts_from_episodes_and_restore_truncateds,
)
from ray.rllib.utils.postprocessing.zero_padding import unpad_data_if_necessary
from ray.rllib.utils.schedules.scheduler import Scheduler
from ray.rllib.utils.typing import ModuleID


class PPOLearner(Learner):
    @override(Learner)
    def build(self) -> None:
        super().build()

        # Dict mapping module IDs to the respective entropy Scheduler instance.
        self.entropy_coeff_schedulers_per_module: Dict[
            ModuleID, Scheduler
        ] = LambdaDefaultDict(
            lambda module_id: Scheduler(
                fixed_value_or_schedule=(
                    self.config.get_config_for_module(module_id).entropy_coeff
                ),
                framework=self.framework,
                device=self._device,
            )
        )

        # Set up KL coefficient variables (per module).
        # Note that the KL coeff is not controlled by a Scheduler, but seeks
        # to stay close to a given kl_target value in our implementation of
        # `self.additional_update_for_module()`.
        self.curr_kl_coeffs_per_module: Dict[ModuleID, Scheduler] = LambdaDefaultDict(
            lambda module_id: self._get_tensor_variable(
                self.config.get_config_for_module(module_id).kl_coeff
            )
        )

    @override(Learner)
    def _preprocess_train_data(
        self,
        *,
        batch,
        episodes,
    ):
        batch = batch or {}
        if not episodes:
            return batch, episodes

        # Make all episodes one ts longer in order to just have a single batch
        # (and distributed forward pass) for both vf predictions AND the bootstrap
        # vf computations.
        orig_truncateds = add_one_ts_to_episodes_and_truncate(episodes)
        episode_lens_p1 = [len(e) for e in episodes]

        # Call the learner connector (on the artificially elongated episodes)
        # in order to get the batch to pass through the module for vf (and
        # bootstrapped vf) computations.
        batch_for_vf = self._learner_connector(
            rl_module=self.module["default_policy"],  # TODO: make multi-agent capable
            data={},
            episodes=episodes,
        )
        # Perform the value model's forward pass.
        vf_preds = convert_to_numpy(self._compute_values(batch_for_vf))
        # Remove all zero-padding again, if applicable for the upcoming
        # GAE computations.
        vf_preds = unpad_data_if_necessary(episode_lens_p1, vf_preds)
        # Compute value targets.
        value_targets = compute_value_targets(
            values=vf_preds,
            rewards=unpad_data_if_necessary(
                episode_lens_p1, batch_for_vf[SampleBatch.REWARDS]
            ),
            terminateds=unpad_data_if_necessary(
                episode_lens_p1, batch_for_vf[SampleBatch.TERMINATEDS]
            ),
            truncateds=unpad_data_if_necessary(
                episode_lens_p1, batch_for_vf[SampleBatch.TRUNCATEDS]
            ),
            gamma=self.config.gamma,
            lambda_=self.config.lambda_,
        )

        # Remove the extra timesteps again from vf_preds and value targets. Now that
        # the GAE computation is done, we don't need this last timestep anymore in any
        # of our data.
        (
            batch[SampleBatch.VF_PREDS],
            batch[Postprocessing.VALUE_TARGETS],
        ) = remove_last_ts_from_data(
            episode_lens_p1,
            vf_preds,
            value_targets,
        )
        advantages = batch[Postprocessing.VALUE_TARGETS] - batch[SampleBatch.VF_PREDS]
        # Standardize advantages (used for more stable and better weighted
        # policy gradient computations).
        batch[Postprocessing.ADVANTAGES] = (advantages - advantages.mean()) / max(
            1e-4, advantages.std()
        )

        # Remove the extra (artificial) timesteps again at the end of all episodes.
        remove_last_ts_from_episodes_and_restore_truncateds(
            episodes,
            orig_truncateds,
        )

        return batch, episodes

    @override(Learner)
    def remove_module(self, module_id: str):
        super().remove_module(module_id)
        self.entropy_coeff_schedulers_per_module.pop(module_id, None)
        self.curr_kl_coeffs_per_module.pop(module_id, None)

    @override(Learner)
    def additional_update_for_module(
        self,
        *,
        module_id: ModuleID,
        config: "PPOConfig",
        timestep: int,
        sampled_kl_values: dict,
    ) -> Dict[str, Any]:
        results = super().additional_update_for_module(
            module_id=module_id,
            config=config,
            timestep=timestep,
            sampled_kl_values=sampled_kl_values,
        )

        # Update entropy coefficient via our Scheduler.
        new_entropy_coeff = self.entropy_coeff_schedulers_per_module[module_id].update(
            timestep=timestep
        )
        results.update({LEARNER_RESULTS_CURR_ENTROPY_COEFF_KEY: new_entropy_coeff})

        return results

    @abc.abstractmethod
    def _compute_values(self, batch) -> np._typing.NDArray:
        """Computes the values using the value function module given a batch of data.

        Args:
            batch: The input batch to pass through our RLModule (value function
                encoder and vf-head).

        Returns:
            The batch (numpy) of value function outputs (already squeezed over the last
            dimension (which should have shape (1,) b/c of the single value output
            node).
        """
