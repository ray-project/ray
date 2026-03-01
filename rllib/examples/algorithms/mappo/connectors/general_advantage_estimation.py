from typing import Any, Dict, List

import numpy as np

from ray.rllib.connectors.common.numpy_to_tensor import NumpyToTensor
from ray.rllib.connectors.connector_v2 import ConnectorV2
from ray.rllib.core.columns import Columns
from ray.rllib.core.rl_module.apis import SelfSupervisedLossAPI
from ray.rllib.core.rl_module.multi_rl_module import MultiRLModule
from ray.rllib.evaluation.postprocessing import Postprocessing
from ray.rllib.utils.annotations import override
from ray.rllib.utils.framework import try_import_torch
from ray.rllib.utils.numpy import convert_to_numpy
from ray.rllib.utils.postprocessing.value_predictions import compute_value_targets
from ray.rllib.utils.postprocessing.zero_padding import (
    split_and_zero_pad_n_episodes,
    unpad_data_if_necessary,
)
from ray.rllib.utils.typing import EpisodeType

torch, nn = try_import_torch()

SHARED_CRITIC_ID = "shared_critic"


class MAPPOGAEConnector(ConnectorV2):
    """Learner connector that computes GAE using a shared centralized critic.

    Concatenates observations from all agent modules (sorted by module ID for
    determinism), feeds them through the shared critic to obtain joint value
    predictions, then runs per-agent GAE and stores advantages / value targets
    back into the batch.
    """

    def __init__(
        self,
        input_observation_space=None,
        input_action_space=None,
        *,
        gamma,
        lambda_,
    ):
        super().__init__(input_observation_space, input_action_space)
        self.gamma = gamma
        self.lambda_ = lambda_
        self._numpy_to_tensor_connector = None

    def _get_agent_module_ids(
        self, rl_module: MultiRLModule, batch: Dict[str, Any]
    ) -> List[str]:
        """Return a stable, sorted list of agent module IDs present in the batch.

        Excludes the shared critic and any SelfSupervisedLossAPI modules.
        Validates that every expected agent is present (required for the
        concatenated-observation critic architecture).
        """
        expected = sorted(
            mid
            for mid in rl_module.keys()
            if mid != SHARED_CRITIC_ID
            and not isinstance(rl_module[mid].unwrapped(), SelfSupervisedLossAPI)
        )
        present = [
            mid
            for mid in expected
            if mid in batch
            and isinstance(batch[mid], dict)
            and Columns.OBS in batch[mid]
        ]
        if len(present) != len(expected):
            missing = set(expected) - set(present)
            raise ValueError(
                f"MAPPO's shared critic requires all agent observations in every "
                f"batch. Missing modules: {missing}."
            )
        return present

    @override(ConnectorV2)
    def __call__(
        self,
        *,
        rl_module: MultiRLModule,
        episodes: List[EpisodeType],
        batch: Dict[str, Any],
        **kwargs,
    ):
        device = None

        sa_episodes_list = list(
            self.single_agent_episode_iterator(episodes, agents_that_stepped_only=False)
        )

        obs_mids = self._get_agent_module_ids(rl_module, batch)

        # Build critic batch: concatenate all agent observations along last dim.
        critic_batch = {}
        critic_batch[Columns.OBS] = torch.cat(
            [batch[k][Columns.OBS] for k in obs_mids], dim=-1
        )

        # Combine loss masks from all agents (logical AND: valid only when
        # ALL agents are valid at that timestep).
        if any(Columns.LOSS_MASK in batch[mid] for mid in obs_mids):
            masks = [
                batch[mid][Columns.LOSS_MASK]
                for mid in obs_mids
                if Columns.LOSS_MASK in batch[mid]
            ]
            combined = masks[0]
            for m in masks[1:]:
                combined = combined & m
            critic_batch[Columns.LOSS_MASK] = combined

        # Shared critic forward pass -> per-agent value predictions.
        vf_preds_tensor = rl_module[SHARED_CRITIC_ID].compute_values(critic_batch)
        device = vf_preds_tensor.device
        vf_preds = {mid: vf_preds_tensor[..., i] for i, mid in enumerate(obs_mids)}

        # Per-agent GAE computation.
        for module_id, module_vf_preds in vf_preds.items():
            module = rl_module[module_id]
            module_vf_preds = convert_to_numpy(module_vf_preds)

            episode_lens = [
                len(e) for e in sa_episodes_list if e.module_id in [None, module_id]
            ]

            module_vf_preds = unpad_data_if_necessary(episode_lens, module_vf_preds)

            module_value_targets = compute_value_targets(
                values=module_vf_preds,
                rewards=unpad_data_if_necessary(
                    episode_lens,
                    convert_to_numpy(batch[module_id][Columns.REWARDS]),
                ),
                terminateds=unpad_data_if_necessary(
                    episode_lens,
                    convert_to_numpy(batch[module_id][Columns.TERMINATEDS]),
                ),
                truncateds=unpad_data_if_necessary(
                    episode_lens,
                    convert_to_numpy(batch[module_id][Columns.TRUNCATEDS]),
                ),
                gamma=self.gamma,
                lambda_=self.lambda_,
            )
            assert module_value_targets.shape[0] == sum(episode_lens)

            module_advantages = module_value_targets - module_vf_preds
            module_advantages = (module_advantages - module_advantages.mean()) / max(
                1e-4, module_advantages.std()
            )

            if module.is_stateful():
                module_advantages = np.stack(
                    split_and_zero_pad_n_episodes(
                        module_advantages,
                        episode_lens=episode_lens,
                        max_seq_len=module.model_config["max_seq_len"],
                    ),
                    axis=0,
                )
                module_value_targets = np.stack(
                    split_and_zero_pad_n_episodes(
                        module_value_targets,
                        episode_lens=episode_lens,
                        max_seq_len=module.model_config["max_seq_len"],
                    ),
                    axis=0,
                )
            batch[module_id][Postprocessing.ADVANTAGES] = module_advantages
            batch[module_id][Postprocessing.VALUE_TARGETS] = module_value_targets

        # Aggregate per-agent GAE results into the critic batch.
        critic_batch[Postprocessing.VALUE_TARGETS] = np.stack(
            [batch[mid][Postprocessing.VALUE_TARGETS] for mid in obs_mids], axis=-1
        )
        critic_batch[Postprocessing.ADVANTAGES] = np.stack(
            [batch[mid][Postprocessing.ADVANTAGES] for mid in obs_mids], axis=-1
        )
        batch[SHARED_CRITIC_ID] = critic_batch

        # Convert numpy GAE results back to tensors.
        if self._numpy_to_tensor_connector is None:
            self._numpy_to_tensor_connector = NumpyToTensor(
                as_learner_connector=True, device=device
            )
        tensor_results = self._numpy_to_tensor_connector(
            rl_module=rl_module,
            batch={
                mid: {
                    Postprocessing.ADVANTAGES: module_batch[Postprocessing.ADVANTAGES],
                    Postprocessing.VALUE_TARGETS: module_batch[
                        Postprocessing.VALUE_TARGETS
                    ],
                }
                for mid, module_batch in batch.items()
                if (mid == SHARED_CRITIC_ID) or (mid in vf_preds)
            },
            episodes=episodes,
        )
        for mid, module_batch in tensor_results.items():
            batch[mid].update(module_batch)

        return batch
