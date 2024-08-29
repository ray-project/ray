from typing import Any, List, Dict, Optional

from ray.rllib.connectors.connector_v2 import ConnectorV2
from ray.rllib.connectors.common.numpy_to_tensor import NumpyToTensor
from ray.rllib.core.columns import Columns
from ray.rllib.core.rl_module.apis.value_function_api import ValueFunctionAPI
from ray.rllib.core.rl_module.rl_module import RLModule
from ray.rllib.evaluation.postprocessing import Postprocessing
from ray.rllib.utils.numpy import convert_to_numpy
from ray.rllib.utils.postprocessing.value_predictions import compute_value_targets
from ray.rllib.utils.postprocessing.zero_padding import (
    split_and_zero_pad,
    unpad_data_if_necessary,
)
from ray.rllib.utils.typing import EpisodeType


class GeneralAdvantageEstimation(ConnectorV2):
    """Learner ConnectorV2 piece computing GAE advatages and value targets on episodes.

    Note that the incoming `episodes` may be SingleAgent- or MultiAgentEpisodes and may
    be episode chunks (not starting from reset or ending prematurely).

    `episodes` must already be elongated by one artificial timestep at the
    end  (last obs, actions, states, etc.. repeated, last reward=0.0, etc..),
    making it possible to combine the per-timestep value computations with the
    necessary "bootstrap" value computations at the episode (chunk) truncation points.

    The GAE computation is performed in a very efficient way through using the arriving
    `batch` as forward batch for the value function, extracting the bootstrap values
    (at the artificially added time steos) and all other value predictions (all other
    timesteps), performing GAE, and adding the results back into `batch` (under
    Postprocessing.ADVANTAGES and Postprocessing.VALUE_TARGETS.
    """
    def __init__(
        self,
        input_observation_space=None,
        input_action_space=None,
        *,
        gamma,
        lambda_,
    ):
        """Initializes a GeneralAdvantageEstimation instance.
        
        Args:
            gamma: The discount factor gamma.
            lambda_: The lambda parameter for General Advantage Estimation (GAE).
                Defines the exponential weight used between actually measured rewards
                vs value function estimates over multiple time steps. Specifically,
                `lambda_` balances short-term, low-variance estimates with longer-term,
                high-variance returns. A `lambda_` or 0.0 makes the GAE rely only on
                immediate rewards (and vf predictions from there on, reducing variance,
                but increasing bias), while a `lambda_` of 1.0 only incorporates vf
                predictions at the truncation points of the given episodes or episode
                chunks (reducing bias but increasing variance).
        """
        super().__init__(input_observation_space, input_action_space)
        self.gamma = gamma
        self.lambda_ = lambda_

        # Internal numpy-to-tensor connector to translate GAE results (advantages and
        # vf targets) into tensors.
        self._numpy_to_tensor_connector = None

    def __call__(
        self,
        *,
        rl_module: RLModule,
        episodes: List[EpisodeType],
        batch: Dict[str, Any],
        **kwargs,
    ):
        sa_episodes_list = list(
            self.single_agent_episode_iterator(
                episodes, agents_that_stepped_only=False
            )
        )
        # Perform the value model's forward pass.
        vf_preds = rl_module.foreach_module(
            func=lambda mid, module: (
                module.compute_values(batch[mid])
                if isinstance(module, ValueFunctionAPI)
                else None
            ),
            return_dict=True,
        )
        device = next(iter(vf_preds.values())).device
        vf_preds = convert_to_numpy(vf_preds)

        for module_id, module_vf_preds in vf_preds.items():
            # Skip those outputs of RLModules that are not implementers of
            # `ValueFunctionAPI`.
            if module_vf_preds is None:
                continue

            # Collect (single-agent) episode lengths.
            episode_lens = [
                len(e)
                for e in sa_episodes_list
                if e.module_id is None or e.module_id == module_id
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

            module_advantages = module_value_targets - module_vf_preds
            # Drop vf-preds, not needed in loss. Note that in the PPORLModule, vf-preds
            # are recomputed with each `forward_train` call anyway.
            # Standardize advantages (used for more stable and better weighted
            # policy gradient computations).
            module_advantages = (module_advantages - module_advantages.mean()) / max(
                1e-4, module_advantages.std()
            )

            # Zero-pad the new computations, if necessary.
            if rl_module[module_id].is_stateful():
                module_advantages = split_and_zero_pad(
                    module_advantages,
                    T=rl_module[module_id].config.model_config_dict["max_seq_len"],
                )
                module_value_targets = split_and_zero_pad(
                    module_value_targets,
                    T=rl_module[module_id].config.model_config_dict["max_seq_len"],
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
            },
            episodes=episodes,
        )
        # Move converted tensors back to `batch`.
        for mid, module_batch in tensor_results.items():
            batch[mid].update(module_batch)

        return batch
