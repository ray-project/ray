import abc
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Union

import numpy as np
import tree  # pip install dm_tree

from ray.rllib.connectors.connector_v2 import ConnectorV2
from ray.rllib.connectors.connector_context_v2 import ConnectorContextV2
from ray.rllib.core.learner.learner import Learner, LearnerHyperparameters
from ray.rllib.core.rl_module.rl_module import ModuleID
from ray.rllib.evaluation.postprocessing_v2 import compute_advantages, Postprocessing
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.utils.annotations import override
from ray.rllib.utils.lambda_defaultdict import LambdaDefaultDict
from ray.rllib.utils.numpy import convert_to_numpy
from ray.rllib.utils.schedules.scheduler import Scheduler
from ray.rllib.utils.typing import EpisodeType


LEARNER_RESULTS_VF_LOSS_UNCLIPPED_KEY = "vf_loss_unclipped"
LEARNER_RESULTS_VF_EXPLAINED_VAR_KEY = "vf_explained_var"
LEARNER_RESULTS_KL_KEY = "mean_kl_loss"
LEARNER_RESULTS_CURR_KL_COEFF_KEY = "curr_kl_coeff"
LEARNER_RESULTS_CURR_ENTROPY_COEFF_KEY = "curr_entropy_coeff"


@dataclass
class PPOLearnerHyperparameters(LearnerHyperparameters):
    """Hyperparameters for the PPOLearner sub-classes (framework specific).

    These should never be set directly by the user. Instead, use the PPOConfig
    class to configure your algorithm.
    See `ray.rllib.algorithms.ppo.ppo::PPOConfig::training()` for more details on the
    individual properties.
    """

    use_kl_loss: bool = None
    kl_coeff: float = None
    kl_target: float = None
    use_critic: bool = None
    clip_param: float = None
    vf_clip_param: float = None
    entropy_coeff: float = None
    entropy_coeff_schedule: Optional[List[List[Union[int, float]]]] = None
    vf_loss_coeff: float = None
    gamma: float = None
    lambda_: float = None


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
                    self.hps.get_hps_for_module(module_id).entropy_coeff
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
                self.hps.get_hps_for_module(module_id).kl_coeff
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

        # Make all episodes one ts longer in order to just have a single batch
        # for both vf predictions AND the bootstrap vf computations.
        orig_truncateds = [episode.is_truncated for episode in episodes]
        self._add_ts_to_episodes(episodes)

        # Call the learner connector (on the artificially elongated episodes)
        # in order to get the batch to pass through the module for vf (and
        # bootstrapped vf) computations.
        batch_for_vf = self._learner_connector(
            input_={},
            episodes=episodes,
            ctx=self._learner_connector_ctx,
        )
        # Perform the value model's forward pass.
        vf_preds = convert_to_numpy(self._compute_values(batch_for_vf))
        # Compute advantages.
        advantages, value_targets = compute_advantages(
            values=vf_preds,
            rewards=batch_for_vf[SampleBatch.REWARDS],
            terminateds=batch_for_vf[SampleBatch.TERMINATEDS],
            truncateds=batch_for_vf[SampleBatch.TRUNCATEDS],
            gamma=self.hps.gamma,
            lambda_=self.hps.lambda_,
        )
        # Compute value targets as sum of advantages + vf predictions.
        #value_targets = advantages + vf_preds

        # Remove the extra timesteps again from vf_preds and advantages and
        # un-zero-pad, if applicable.
        # This makes sure that the new computed data (advantages, value targets, etc..)
        # is in the plain 1D format (no time axis) as other custom connector created
        # data would be in during the upcoming pass through the learner connector.
        orig_shape = vf_preds.shape
        episode_lens = [len(e) for e in episodes]
        (
            batch[SampleBatch.VF_PREDS],
            advantages,
            batch[Postprocessing.VALUE_TARGETS],
        ) = (
            self._remove_last_values_2d_and_unpad(
                episode_lens, vf_preds, advantages, value_targets
            ) if len(orig_shape) == 2 else self._remove_last_values_1d(
                episode_lens, vf_preds, advantages, value_targets
            )
        )
        # Standardize advantages (used for more stable and better weighted
        # policy gradient computations).
        batch[Postprocessing.ADVANTAGES] = (
            (advantages - advantages.mean()) / max(1e-4, advantages.std())
        )

        # Remove the extra (artificial) timesteps again at the end of the episodes.
        self._remove_ts_episodes(episodes, orig_truncateds)

        return batch, episodes

    @override(Learner)
    def remove_module(self, module_id: str):
        super().remove_module(module_id)
        self.curr_kl_coeffs_per_module.pop(module_id)
        self.entropy_coeff_schedulers_per_module.pop(module_id)

    @override(Learner)
    def additional_update_for_module(
        self,
        *,
        module_id: ModuleID,
        hps: PPOLearnerHyperparameters,
        timestep: int,
        sampled_kl_values: dict,
    ) -> Dict[str, Any]:
        results = super().additional_update_for_module(
            module_id=module_id,
            hps=hps,
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
    def _add_ts_to_episodes(episodes):
        """Adds an additional (artificial) timestep to an episode.

        Useful for value function bootstrapping, where it is required to compute
        a forward pass for the very last timestep within the episode,
        i.e. using the following input dict: {
          obs=[final obs],
          state=[final state output],
          prev. reward=[final reward],
          etc..
        }
        """
        for episode in episodes:
            # Make sure the episode is already in numpy format.
            assert episode.is_numpy
            # Add timestep.
            episode.t += 1
            episode.observations = tree.map_structure(
                lambda s: np.concatenate([s, [s[-1]]]),
                episode.observations,
            )
            # Infos are always lists.
            episode.infos.append(episode.infos[-1])
            episode.actions = tree.map_structure(
                lambda s: np.concatenate([s, [s[-1]]]),
                episode.actions,
            )
            episode.rewards = np.concatenate([episode.rewards, [episode.rewards[-1]]])
            episode.extra_model_outputs = tree.map_structure(
                lambda s: np.concatenate([s, [s[-1]]], axis=0),
                episode.extra_model_outputs,
            )
            # Artificially make this episode truncated for the upcoming
            # GAE computations.
            if not episode.is_done:
                episode.is_truncated = True

    @staticmethod
    def _remove_ts_episodes(episodes, truncateds):
        # Fix episodes (remove the extra timestep).
        for episode, truncated in zip(episodes, truncateds):
            episode.t -= 1
            # Fix the truncateds flag again.
            episode.is_truncated = truncated
            episode.observations = tree.map_structure(
                lambda s: s[:-1],
                episode.observations,
            )
            # Infos are always lists.
            episode.infos = episode.infos[:-1]
            episode.actions = tree.map_structure(lambda s: s[:-1], episode.actions)
            episode.rewards = episode.rewards[:-1]
            episode.extra_model_outputs = tree.map_structure(
                lambda s: s[:-1],
                episode.extra_model_outputs,
            )

    @staticmethod
    def _remove_last_values_1d(episode_lens, *data):
        slices = []
        sum = 0
        for len_ in episode_lens:
            slices.append(slice(sum, sum + len_ - 1))
            sum += len_
        ret = []
        for d in data:
            ret.append(np.concatenate([d[s] for s in slices]))
        return tuple(ret)

    @staticmethod
    def _remove_last_values_2d_and_unpad(episode_lens, *data_zero_padded):
        ret = []
        for data in data_zero_padded:
            new_data = []
            row_idx = 0
            T = data.shape[1]
            for len_ in episode_lens:
                # Calculate how many full rows this array occupies and how many elements are
                # in the last, potentially partial row.
                num_rows, col_idx = divmod(len_, T)
                if col_idx == 0:
                    num_rows -= 1
                col_idx -= 1
    
                # If the array spans multiple full rows, fully include these rows.
                for i in range(num_rows):
                    new_data.append(data[row_idx + i])
                row_idx += num_rows
    
                # If there are elements in the last, potentially partial row, find the
                # column index and replace the last element with 0.
                #data[row_idx, col_idx] = 0.0
                new_data.append(data[row_idx, :col_idx])
    
                # Move to the next row for the next array.
                row_idx += 1

            ret.append(np.concatenate(new_data))

        return tuple(ret)
