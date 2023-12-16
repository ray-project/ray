import abc
from typing import Any, Dict

import numpy as np

from ray.rllib.algorithms.ppo.ppo import (
    LEARNER_RESULTS_CURR_ENTROPY_COEFF_KEY,
    PPOConfig,
)
from ray.rllib.core.learner.learner import Learner
from ray.rllib.evaluation.postprocessing import Postprocessing
from ray.rllib.evaluation.postprocessing_v2 import compute_value_targets
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.utils.annotations import override
from ray.rllib.utils.lambda_defaultdict import LambdaDefaultDict
from ray.rllib.utils.numpy import convert_to_numpy
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
        orig_truncateds = self._add_ts_to_episodes_and_truncate(episodes)
        episode_lens_p1 = [len(e) for e in episodes]

        # Call the learner connector (on the artificially elongated episodes)
        # in order to get the batch to pass through the module for vf (and
        # bootstrapped vf) computations.
        batch_for_vf = self._learner_connector(
            rl_module=self.module["default_policy"],  # TODO: make multi-agent capable
            input_={},
            episodes=episodes,
        )
        # Perform the value model's forward pass.
        vf_preds = convert_to_numpy(self._compute_values(batch_for_vf))
        # Remove all zero-padding again, if applicable for the upcoming
        # GAE computations.
        vf_preds = self._unpad_data_if_necessary(episode_lens_p1, vf_preds)
        # Compute value targets.
        value_targets = compute_value_targets(
            values=vf_preds,
            rewards=self._unpad_data_if_necessary(
                episode_lens_p1, batch_for_vf[SampleBatch.REWARDS]
            ),
            terminateds=self._unpad_data_if_necessary(
                episode_lens_p1, batch_for_vf[SampleBatch.TERMINATEDS]
            ),
            truncateds=self._unpad_data_if_necessary(
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
        ) = self._remove_last_ts_from_data(
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
        self._remove_last_ts_from_episodes_and_restore_truncateds(
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

    @staticmethod
    def _add_ts_to_episodes_and_truncate(episodes):
        """Adds an artificial timestep to an episode at the end.

        Useful for value function bootstrapping, where it is required to compute
        a forward pass for the very last timestep within the episode,
        i.e. using the following input dict: {
          obs=[final obs],
          state=[final state output],
          prev. reward=[final reward],
          etc..
        }
        """
        orig_truncateds = []
        for episode in episodes:
            # Make sure the episode is already in numpy format.
            assert episode.is_finalized
            orig_truncateds.append(episode.is_truncated)

            # Add timestep.
            episode.t += 1
            # Use the episode API that allows appending (possibly complex) structs
            # to the data.
            episode.observations.append(episode.observations[-1])
            # = tree.map_structure(
            # lambda s: np.concatenate([s, [s[-1]]]),
            # episode.observations,
            # )
            episode.infos.append(episode.infos[-1])
            episode.actions.append(episode.actions[-1])  # = tree.map_structure(
            # lambda s: np.concatenate([s, [s[-1]]]),
            # episode.actions,
            # )
            episode.rewards.append(0.0)  # = np.append(episode.rewards, 0.0)
            for v in list(episode.extra_model_outputs.values()):
                v.append(v[-1])
            # episode.extra_model_outputs = tree.map_structure(
            #    lambda s: np.concatenate([s, [s[-1]]], axis=0),
            #    episode.extra_model_outputs,
            # )
            # Artificially make this episode truncated for the upcoming
            # GAE computations.
            if not episode.is_done:
                episode.is_truncated = True
            # Validate to make sure, everything is in order.
            episode.validate()

        return orig_truncateds

    @staticmethod
    def _remove_last_ts_from_episodes_and_restore_truncateds(episodes, orig_truncateds):
        # Fix episodes (remove the extra timestep).
        for episode, orig_truncated in zip(episodes, orig_truncateds):
            # Reduce timesteps by 1.
            episode.t -= 1
            episode.observations.pop()
            episode.infos.pop()
            episode.actions.pop()
            episode.rewards.pop()
            for v in episode.extra_model_outputs.values():
                v.pop()
            # Fix the truncateds flag again.
            episode.is_truncated = orig_truncated

    @staticmethod
    def _unpad_data_if_necessary(episode_lens, data):
        # If data des NOT have time dimension, return right away.
        if len(data.shape) == 1:
            return data

        # Assert we only have B and T dimensions (meaning this function only operates
        # on single-float data, such as value function predictions).
        assert len(data.shape) == 2

        new_data = []
        row_idx = 0

        T = data.shape[1]
        for len_ in episode_lens:
            # Calculate how many full rows this array occupies and how many elements are
            # in the last, potentially partial row.
            num_rows, col_idx = divmod(len_, T)

            # If the array spans multiple full rows, fully include these rows.
            for i in range(num_rows):
                new_data.append(data[row_idx])
                row_idx += 1

            # If there are elements in the last, potentially partial row, add this
            # partial row as well.
            if col_idx > 0:
                new_data.append(data[row_idx, :col_idx])

                # Move to the next row for the next array (skip the zero-padding zone).
                row_idx += 1

        return np.concatenate(new_data)

    @staticmethod
    def _remove_last_ts_from_data(episode_lens, *data):
        slices = []
        sum = 0
        for len_ in episode_lens:
            slices.append(slice(sum, sum + len_ - 1))
            sum += len_
        ret = []
        for d in data:
            ret.append(np.concatenate([d[s] for s in slices]))
        return tuple(ret)
