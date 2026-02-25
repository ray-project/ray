from typing import Any, Dict, List

import numpy as np

from ray.rllib.connectors.common.numpy_to_tensor import NumpyToTensor
from ray.rllib.connectors.connector_v2 import ConnectorV2
from ray.rllib.core.columns import Columns
from ray.rllib.core.rl_module.apis.value_function_api import ValueFunctionAPI
from ray.rllib.core.rl_module.multi_rl_module import MultiRLModule
from ray.rllib.evaluation.postprocessing import Postprocessing
from ray.rllib.utils.annotations import override
from ray.rllib.utils.numpy import convert_to_numpy
from ray.rllib.utils.postprocessing.value_predictions import (
    compute_value_targets_with_bootstrap,
)
from ray.rllib.utils.postprocessing.zero_padding import (
    split_and_zero_pad_n_episodes,
    unpad_data_if_necessary,
)
from ray.rllib.utils.typing import EpisodeType


class GeneralAdvantageEstimation(ConnectorV2):
    """Learner ConnectorV2 piece computing GAE advantages and value targets on episodes.

    This ConnectorV2:
    - Operates on a list of Episode objects (single- or multi-agent).
    - Should be used only in the Learner pipeline and as one of the last pieces (due
    to the fact that it requires the batch for the value functions to be already
    complete).
    - Does NOT require episodes to be artificially elongated beforehand. Bootstrap
    values for truncated / in-progress episodes are computed via a second, small
    forward pass on the final observation of each such episode.

    The GAE computation is performed per-episode using explicit bootstrap values:
    - Terminated episodes: bootstrap = 0.
    - Truncated / in-progress episodes: bootstrap = V(last_obs), obtained from the
      second forward pass.

    Results are added back into `batch` under Postprocessing.ADVANTAGES and
    Postprocessing.VALUE_TARGETS.
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
        """
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

        # Perform the value nets' forward passes on the main batch.
        # TODO (sven): We need to check here in the pipeline already, whether a module
        #  should even be updated or not (which we usually do after(!) the Learner
        #  pipeline). This is an open TODO to move this filter into a connector as well.
        #  For now, we'll just check, whether `mid` is in batch and skip if it isn't.
        vf_preds = rl_module.foreach_module(
            func=lambda mid, module: (
                module.compute_values(batch[mid])
                if mid in batch and isinstance(module, ValueFunctionAPI)
                else None
            ),
            return_dict=True,
        )

        # Build bootstrap obs batches: for each module, collect the last observation
        # of each non-terminated episode. These are already stored in the episode
        # (episode.observations[-1] = s_L); no episode mutation needed.
        bootstrap_obs_per_module = {}  # module_id -> np.ndarray (N_bootstrap, obs_dim)
        bootstrap_mask_per_module = (
            {}
        )  # module_id -> list[bool] (True = needs bootstrap)
        for module_id, module_vf_preds in vf_preds.items():
            if module_vf_preds is None:
                continue
            module = rl_module[module_id]
            if module.is_stateful():
                # Stateful (LSTM) modules need the last hidden state for the bootstrap
                # forward pass, which requires additional handling. Fall back to the
                # old path (not implemented here); skip separate bootstrap pass.
                bootstrap_obs_per_module[module_id] = None
                bootstrap_mask_per_module[module_id] = None
                continue

            eps_for_module = [
                e for e in sa_episodes_list if e.module_id in [None, module_id]
            ]
            obs_list = []
            mask = []
            for ep in eps_for_module:
                if not ep.is_terminated:
                    obs_list.append(ep.get_observations(-1))
                    mask.append(True)
                else:
                    mask.append(False)
            bootstrap_obs_per_module[module_id] = (
                np.stack(obs_list, axis=0) if obs_list else None
            )
            bootstrap_mask_per_module[module_id] = mask

        # Run the bootstrap forward pass for non-stateful modules.
        # This is a small pass (N_non_terminated x obs_dim) compared to the main batch.
        bootstrap_vf_preds_per_module = {}
        for module_id, bootstrap_obs in bootstrap_obs_per_module.items():
            if bootstrap_obs is None:
                bootstrap_vf_preds_per_module[module_id] = None
                continue
            # Lazily create the numpy-to-tensor connector (needs a device).
            module = rl_module[module_id]
            if self._numpy_to_tensor_connector is None:
                # We'll set the device after the first successful VF forward pass below.
                pass
            # Convert bootstrap obs to a tensor batch and run compute_values.
            if self._numpy_to_tensor_connector is not None:
                bs_tensor_batch = self._numpy_to_tensor_connector(
                    rl_module=rl_module,
                    batch={module_id: {Columns.OBS: bootstrap_obs}},
                    episodes=episodes,
                )
                bs_vf = module.compute_values(bs_tensor_batch[module_id])
                bootstrap_vf_preds_per_module[module_id] = convert_to_numpy(bs_vf)
            else:
                # _numpy_to_tensor_connector not yet initialised; will be set below
                # after the first module is processed.
                bootstrap_vf_preds_per_module[module_id] = None

        # Loop through all modules and perform each one's GAE computation.
        for module_id, module_vf_preds in vf_preds.items():
            # Skip those outputs of RLModules that are not implementers of
            # `ValueFunctionAPI`.
            if module_vf_preds is None:
                continue

            module = rl_module[module_id]
            device = module_vf_preds.device

            # Initialise the numpy-to-tensor connector on first use.
            if self._numpy_to_tensor_connector is None:
                self._numpy_to_tensor_connector = NumpyToTensor(
                    as_learner_connector=True, device=device
                )
                # Now that the connector is available, redo any bootstrap passes that
                # were skipped above (i.e., the first module).
                for mid2, bs_obs in bootstrap_obs_per_module.items():
                    if (
                        bs_obs is not None
                        and bootstrap_vf_preds_per_module[mid2] is None
                    ):
                        bs_tensor_batch = self._numpy_to_tensor_connector(
                            rl_module=rl_module,
                            batch={mid2: {Columns.OBS: bs_obs}},
                            episodes=episodes,
                        )
                        bs_vf = rl_module[mid2].compute_values(bs_tensor_batch[mid2])
                        bootstrap_vf_preds_per_module[mid2] = convert_to_numpy(bs_vf)

            # Convert main VF preds to numpy.
            module_vf_preds_np = convert_to_numpy(module_vf_preds)

            # Collect episode lengths for this module.
            eps_for_module = [
                e for e in sa_episodes_list if e.module_id in [None, module_id]
            ]
            episode_lens = [len(e) for e in eps_for_module]

            # Remove zero-padding (for stateful/LSTM modules) before GAE.
            module_vf_preds_np = unpad_data_if_necessary(
                episode_lens, module_vf_preds_np
            )
            rewards_np = unpad_data_if_necessary(
                episode_lens,
                convert_to_numpy(batch[module_id][Columns.REWARDS]),
            )
            terminateds_np = unpad_data_if_necessary(
                episode_lens,
                convert_to_numpy(batch[module_id][Columns.TERMINATEDS]),
            )

            # Build per-episode bootstrap values.
            bs_preds = bootstrap_vf_preds_per_module.get(module_id)
            bs_mask = bootstrap_mask_per_module.get(module_id)
            if bs_preds is not None and bs_mask is not None:
                # Non-stateful path: use the bootstrap forward pass results.
                bs_idx = 0
                bootstrap_values = []
                for needs_bootstrap in bs_mask:
                    if needs_bootstrap:
                        bootstrap_values.append(float(bs_preds[bs_idx]))
                        bs_idx += 1
                    else:
                        bootstrap_values.append(0.0)
            else:
                # Stateful / fallback path: use 0 as bootstrap (conservative).
                # TODO: implement proper LSTM bootstrap using STATE_OUT.
                bootstrap_values = [
                    0.0 if ep.is_terminated else 0.0 for ep in eps_for_module
                ]

            # Per-episode GAE.
            all_targets = []
            pos = 0
            for i, ep in enumerate(eps_for_module):
                L = episode_lens[i]
                ep_values = module_vf_preds_np[pos : pos + L]
                ep_rewards = rewards_np[pos : pos + L]
                ep_terminateds = terminateds_np[pos : pos + L]
                targets = compute_value_targets_with_bootstrap(
                    values=ep_values,
                    rewards=ep_rewards,
                    terminateds=ep_terminateds,
                    bootstrap_value=bootstrap_values[i],
                    gamma=self.gamma,
                    lambda_=self.lambda_,
                )
                all_targets.append(targets)
                pos += L

            module_value_targets = np.concatenate(all_targets)
            assert module_value_targets.shape[0] == sum(episode_lens)

            module_advantages = module_value_targets - module_vf_preds_np
            # Standardize advantages (used for more stable and better weighted
            # policy gradient computations).
            module_advantages = (module_advantages - module_advantages.mean()) / max(
                1e-4, module_advantages.std()
            )

            # Zero-pad the new computations, if necessary (stateful modules).
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
                if vf_preds.get(mid) is not None
            },
            episodes=episodes,
        )
        # Move converted tensors back to `batch`.
        for mid, module_batch in tensor_results.items():
            batch[mid].update(module_batch)

        return batch
