"""
[1] Mastering Diverse Domains through World Models - 2023
D. Hafner, J. Pasukonis, J. Ba, T. Lillicrap
https://arxiv.org/pdf/2301.04104v1.pdf

[2] Mastering Atari with Discrete World Models - 2021
D. Hafner, T. Lillicrap, M. Norouzi, J. Ba
https://arxiv.org/pdf/2010.02193.pdf
"""
from typing import Any, Dict

import gymnasium as gym
import torch

from ray.rllib.algorithms.dreamerv3.dreamerv3_rl_module import (
    ACTIONS_ONE_HOT,
    DreamerV3RLModule,
)
from ray.rllib.core.columns import Columns
from ray.rllib.core.rl_module.rl_module import RLModule
from ray.rllib.core.rl_module.torch.torch_rl_module import TorchRLModule
from ray.rllib.utils.annotations import override


class DreamerV3TorchRLModule(TorchRLModule, DreamerV3RLModule):
    """The torch-specific RLModule class for DreamerV3.

    Serves mainly as a thin-wrapper around the `DreamerModel` (a torch.nn.Module) class.
    """

    framework = "torch"

    @override(TorchRLModule)
    def _forward_inference(self, batch: Dict[str, Any], **kwargs) -> Dict[str, Any]:
        # Call the Dreamer-Model's forward_inference method and return a dict.
        with torch.no_grad():
            actions, next_state = self.dreamer_model.forward_inference(
                observations=batch[Columns.OBS],
                previous_states=batch[Columns.STATE_IN],
                is_first=batch["is_first"],
            )
        return self._forward_inference_or_exploration_helper(batch, actions, next_state)

    @override(TorchRLModule)
    def _forward_exploration(self, batch: Dict[str, Any], **kwargs) -> Dict[str, Any]:
        # Call the Dreamer-Model's forward_exploration method and return a dict.
        with torch.no_grad():
            actions, next_state = self.dreamer_model.forward_exploration(
                observations=batch[Columns.OBS],
                previous_states=batch[Columns.STATE_IN],
                is_first=batch["is_first"],
            )
        return self._forward_inference_or_exploration_helper(batch, actions, next_state)

    @override(RLModule)
    def _forward_train(self, batch: Dict[str, Any], **kwargs):
        # Call the Dreamer-Model's forward_train method and return its outputs as-is.
        return self.dreamer_model.forward_train(
            observations=batch[Columns.OBS],
            actions=batch[Columns.ACTIONS],
            is_first=batch["is_first"],
        )

    def _forward_inference_or_exploration_helper(self, batch, actions, next_state):
        # Unfold time dimension.
        shape = batch[Columns.OBS].shape
        B, T = shape[0], shape[1]
        actions = actions.view((B, T) + actions.shape[1:])

        output = {
            Columns.ACTIONS: actions,
            ACTIONS_ONE_HOT: actions,
            Columns.STATE_OUT: next_state,
        }
        # Undo one-hot actions?
        if isinstance(self.action_space, gym.spaces.Discrete):
            output[Columns.ACTIONS] = torch.argmax(actions, dim=-1)
        return output
