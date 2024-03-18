"""
[1] Mastering Diverse Domains through World Models - 2023
D. Hafner, J. Pasukonis, J. Ba, T. Lillicrap
https://arxiv.org/pdf/2301.04104v1.pdf

[2] Mastering Atari with Discrete World Models - 2021
D. Hafner, T. Lillicrap, M. Norouzi, J. Ba
https://arxiv.org/pdf/2010.02193.pdf
"""
from typing import Any, Dict

from ray.rllib.algorithms.dreamerv3.dreamerv3_rl_module import DreamerV3RLModule
from ray.rllib.core.columns import Columns
from ray.rllib.core.rl_module.rl_module import RLModule
from ray.rllib.core.rl_module.tf.tf_rl_module import TfRLModule
from ray.rllib.utils.annotations import override
from ray.rllib.utils.framework import try_import_tf
from ray.rllib.utils.nested_dict import NestedDict

tf1, tf, _ = try_import_tf()


class DreamerV3TfRLModule(TfRLModule, DreamerV3RLModule):
    """The tf-specific RLModule class for DreamerV3.

    Serves mainly as a thin-wrapper around the `DreamerModel` (a tf.keras.Model) class.
    """

    framework: str = "tf2"

    @override(RLModule)
    def _forward_inference(self, batch: NestedDict) -> Dict[str, Any]:
        # Call the Dreamer-Model's forward_inference method and return a dict.
        actions, next_state = self.dreamer_model.forward_inference(
            observations=batch[Columns.OBS],
            previous_states=batch[Columns.STATE_IN],
            is_first=batch["is_first"],
        )
        return {Columns.ACTIONS: actions, Columns.STATE_OUT: next_state}

    @override(RLModule)
    def _forward_exploration(self, batch: NestedDict) -> Dict[str, Any]:
        # Call the Dreamer-Model's forward_exploration method and return a dict.
        actions, next_state = self.dreamer_model.forward_exploration(
            observations=batch[Columns.OBS],
            previous_states=batch[Columns.STATE_IN],
            is_first=batch["is_first"],
        )
        return {Columns.ACTIONS: actions, Columns.STATE_OUT: next_state}

    @override(RLModule)
    def _forward_train(self, batch: NestedDict):
        # Call the Dreamer-Model's forward_train method and return its outputs as-is.
        return self.dreamer_model.forward_train(
            observations=batch[Columns.OBS],
            actions=batch[Columns.ACTIONS],
            is_first=batch["is_first"],
        )
