from typing import Any, List, Dict

import numpy as np

from ray.rllib.connectors.connector_v2 import ConnectorV2
from ray.rllib.connectors.common.numpy_to_tensor import NumpyToTensor
from ray.rllib.core.columns import Columns
from ray.rllib.core.rl_module.apis import SelfSupervisedLossAPI
from ray.rllib.core.rl_module.multi_rl_module import MultiRLModule
from ray.rllib.evaluation.postprocessing import Postprocessing
from ray.rllib.utils.annotations import override
from ray.rllib.utils.numpy import convert_to_numpy
from ray.rllib.utils.postprocessing.value_predictions import compute_value_targets
from ray.rllib.utils.postprocessing.zero_padding import (
    split_and_zero_pad_n_episodes,
    unpad_data_if_necessary,
)
from ray.rllib.utils.typing import EpisodeType


SHARED_CRITIC_ID = "shared_critic"


class MAPPOGAEConnector(ConnectorV2):
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
        # Internal numpy-to-tensor connector to translate GAE results (advantages and
        # vf targets) into tensors.
        self._numpy_to_tensor_connector = None

    @override(ConnectorV2)
    def __call__(
        self,
        *,
        rl_module: MultiRLModule,
        episodes: List[EpisodeType],
        batch: Dict[str, Any],
        **kwargs,
    ):
        # Device to place all GAE result tensors (advantages and value targets) on.
        device = None

        # Extract all single-agent episodes.
        sa_episodes_list = list(
            self.single_agent_episode_iterator(episodes, agents_that_stepped_only=False)
        )
        # Perform the value nets' forward passes.
        # TODO (sven): We need to check here in the pipeline already, whether a module
        #  should even be updated or not (which we usually do after(!) the Learner
        #  pipeline). This is an open TODO to move this filter into a connector as well.
        #  For now, we'll just check, whether `mid` is in batch and skip if it isn't.
        # For MAPPO, we can't check ValueFunctionAPI, so we check for the presence of an observation and a lack of self-supervision instead.
        vf_preds = rl_module.foreach_module(
            func=lambda mid, module: (
                rl_module[SHARED_CRITIC_ID].compute_values(batch[mid])
                if (mid in batch)
                and (Columns.OBS in batch[mid])
                and (not isinstance(module, SelfSupervisedLossAPI))
                else None
            ),
            return_dict=True,
        )
        # Loop through all modules and perform each one's GAE computation.
        for module_id, module_vf_preds in vf_preds.items():
            # Skip those outputs of RLModules that are not implementers of
            # `ValueFunctionAPI`.
            if module_vf_preds is None:
                continue

            module = rl_module[module_id]
            device = module_vf_preds.device
            # Convert to numpy for the upcoming GAE computations.
            module_vf_preds = convert_to_numpy(module_vf_preds)

            # Collect (single-agent) episode lengths for this particular module.
            episode_lens = [
                len(e) for e in sa_episodes_list if e.module_id in [None, module_id]
            ]

            # Remove all zero-padding again, if applicable, for the upcoming
            # GAE computations.
            module_vf_preds = unpad_data_if_necessary(episode_lens, module_vf_preds)
            # Compute value targets.
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
            # Drop vf-preds, not needed in loss. Note that in the DefaultPPORLModule,
            # vf-preds are recomputed with each `forward_train` call anyway to compute
            # the vf loss.
            # Standardize advantages (used for more stable and better weighted
            # policy gradient computations).
            module_advantages = (module_advantages - module_advantages.mean()) / max(
                1e-4, module_advantages.std()
            )

            # Zero-pad the new computations, if necessary.
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

        # Convert all GAE results to tensors.
        if self._numpy_to_tensor_connector is None:
            self._numpy_to_tensor_connector = NumpyToTensor(
                as_learner_connector=True, device=device
            )
        tensor_results = self._numpy_to_tensor_connector(
            rl_module=rl_module,
            batch={
                mid: {
                    Postprocessing.ADVANTAGES: module_batch[Postprocessing.ADVANTAGES],
                    Postprocessing.VALUE_TARGETS: (
                        module_batch[Postprocessing.VALUE_TARGETS]
                    ),
                }
                for mid, module_batch in batch.items()
                if vf_preds[mid] is not None
            },
            episodes=episodes,
        )
        # Move converted tensors back to `batch`.
        for mid, module_batch in tensor_results.items():
            batch[mid].update(module_batch)

        return batch
