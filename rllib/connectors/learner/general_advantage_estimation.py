from typing import Any, Dict, List, Optional

import numpy as np
import tree  # pip install dm_tree

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

    def _init_numpy_to_tensor_connector(self, vf_preds: Dict[str, Any]):
        """Initialize the numpy-to-tensor connector.

        NumpyToTensor is used to convert the GAE results (advantages and vf targets) into tensors.
        We therefore
        """
        device = next((v.device for v in vf_preds.values() if v is not None), None)
        assert device is not None, "Device is not set"

        self._numpy_to_tensor_connector = NumpyToTensor(
            as_learner_connector=True, device=device
        )

    @override(ConnectorV2)
    def __call__(
        self,
        *,
        rl_module: MultiRLModule,
        episodes: List[EpisodeType],
        batch: Dict[str, Any],
        shared_data: Optional[dict] = None,
        **kwargs,
    ):
        sa_episodes_list = list(
            self.single_agent_episode_iterator(episodes, agents_that_stepped_only=False)
        )

        # Main VF forward pass on the full training batch.
        vf_preds = rl_module.foreach_module(
            func=lambda mid, module: (
                module.compute_values(batch[mid])
                if mid in batch and isinstance(module, ValueFunctionAPI)
                else None
            ),
            return_dict=True,
        )
        # Filter Non-VF modules (modules that are not ValueFunctionAPI return None above)
        vf_preds = {mid: preds for mid, preds in vf_preds.items() if preds is not None}

        # Eagerly initialize the numpy-to-tensor connector (needed for the bootstrap
        # forward pass below). Device is taken from the first available VF prediction.
        if self._numpy_to_tensor_connector is None:
            self._init_numpy_to_tensor_connector(vf_preds)

        # Pre-compute per-module episode lists (used for both bootstrap and GAE).
        eps_per_module = {
            mid: [e for e in sa_episodes_list if e.module_id in (None, mid)]
            for mid in vf_preds.keys()
        }

        # Compute bootstrap values for non-terminated episodes via a small VF pass.
        bootstrap_values = self._compute_bootstrap_values(
            rl_module, vf_preds, eps_per_module, episodes, shared_data
        )

        # Loop through all modules and perform each one's GAE computation.
        for module_id, module_vf_preds in vf_preds.items():
            if module_vf_preds is None:
                continue

            module = rl_module[module_id]
            eps = eps_per_module[module_id]
            episode_lens = [len(e) for e in eps]

            # Convert to numpy, removing zero-padding (for stateful modules).
            module_vf_preds_np = unpad_data_if_necessary(
                episode_lens, convert_to_numpy(module_vf_preds)
            )
            rewards_np = unpad_data_if_necessary(
                episode_lens,
                convert_to_numpy(batch[module_id][Columns.REWARDS]),
            )
            terminateds_np = unpad_data_if_necessary(
                episode_lens,
                convert_to_numpy(batch[module_id][Columns.TERMINATEDS]),
            )

            # Per-episode GAE.
            all_targets = []
            pos = 0
            for i, ep in enumerate(eps):
                L = episode_lens[i]
                targets = compute_value_targets_with_bootstrap(
                    values=module_vf_preds_np[pos : pos + L],
                    rewards=rewards_np[pos : pos + L],
                    terminateds=terminateds_np[pos : pos + L],
                    bootstrap_value=bootstrap_values[module_id][i],
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

            # Re-apply zero-padding for stateful modules.
            if module.is_stateful():
                max_seq_len = module.model_config["max_seq_len"]
                module_advantages = np.stack(
                    split_and_zero_pad_n_episodes(
                        module_advantages,
                        episode_lens=episode_lens,
                        max_seq_len=max_seq_len,
                    ),
                    axis=0,
                )
                module_value_targets = np.stack(
                    split_and_zero_pad_n_episodes(
                        module_value_targets,
                        episode_lens=episode_lens,
                        max_seq_len=max_seq_len,
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
                    Postprocessing.ADVANTAGES: batch[mid][Postprocessing.ADVANTAGES],
                    Postprocessing.VALUE_TARGETS: batch[mid][
                        Postprocessing.VALUE_TARGETS
                    ],
                }
                for mid, preds in vf_preds.items()
                if preds is not None
            },
            episodes=episodes,
        )
        for mid, module_batch in tensor_results.items():
            batch[mid].update(module_batch)

        return batch

    def _compute_bootstrap_values(
        self, rl_module, vf_preds, eps_per_module, episodes, shared_data
    ):
        """Compute per-episode bootstrap values for each module.

        Returns a dict mapping module_id -> list[float] of bootstrap values (one per
        episode). Terminated episodes get 0.0; non-terminated episodes get V(last_obs)
        from a small forward pass.
        """
        result = {}

        for module_id, eps in eps_per_module.items():
            module = rl_module[module_id]

            # Collect last observations (and states for stateful modules)
            # for non-terminated episodes.
            is_stateful = module.is_stateful()
            obs_list = []
            state_list = []
            needs_bootstrap = []
            for ep in eps:
                if not ep.is_terminated:
                    # Use frame-stacked bootstrap obs if pre-computed by
                    # FrameStackingLearner; otherwise use the raw last observation.
                    if "stacked_bootstrap_obs" in shared_data:
                        obs = shared_data["stacked_bootstrap_obs"][ep.id_]
                    else:
                        obs = ep.get_observations(-1)
                    obs_list.append(obs)
                    if is_stateful:
                        state_list.append(
                            ep.get_extra_model_outputs(
                                key=Columns.STATE_OUT, indices=-1
                            )
                        )
                    needs_bootstrap.append(True)
                else:
                    needs_bootstrap.append(False)

            if not obs_list:
                # All episodes terminated; bootstrap values are all zero.
                result[module_id] = [0.0] * len(eps)
                continue

            # Stack bootstrap obs and run a batched VF forward pass.
            if is_stateful:
                # Stateful modules expect (B, T, ...) obs with T=1 for a
                # single-step bootstrap.
                bootstrap_obs = tree.map_structure(
                    lambda *s: np.stack(s, axis=0)[:, np.newaxis], *obs_list
                )
                bootstrap_states = tree.map_structure(
                    lambda *s: np.stack(s, axis=0), *state_list
                )
                bs_batch = {
                    module_id: {
                        Columns.OBS: bootstrap_obs,
                        Columns.STATE_IN: bootstrap_states,
                    }
                }
            else:
                bootstrap_obs = tree.map_structure(
                    lambda *s: np.stack(s, axis=0), *obs_list
                )
                bs_batch = {module_id: {Columns.OBS: bootstrap_obs}}

            bs_tensor_batch = self._numpy_to_tensor_connector(
                rl_module=rl_module,
                batch=bs_batch,
                episodes=episodes,
            )
            bs_preds = convert_to_numpy(
                module.unwrapped().compute_values(bs_tensor_batch[module_id])
            )
            # Stateful modules return (B, T) with T=1; flatten to (B,).
            bs_preds = bs_preds.reshape(-1)

            # Scatter bootstrap predictions back into per-episode order.
            bs_idx = 0
            values = []
            for flag in needs_bootstrap:
                if flag:
                    values.append(float(bs_preds[bs_idx]))
                    bs_idx += 1
                else:
                    values.append(0.0)
            result[module_id] = values

        return result
