from typing import Mapping, Any

from ray.rllib.algorithms.dreamerv3.dreamerv3_rl_module import DreamerV3RLModule
from ray.rllib.core.models.base import ACTOR, CRITIC, STATE_IN, STATE_OUT
from ray.rllib.core.models.tf.encoder import ENCODER_OUT
from ray.rllib.core.rl_module.rl_module import RLModule
from ray.rllib.core.rl_module.tf.tf_rl_module import TfRLModule
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.utils.annotations import override
from ray.rllib.utils.framework import try_import_tf
from ray.rllib.utils.nested_dict import NestedDict

tf1, tf, _ = try_import_tf()


class DreamerV3TfRLModule(DreamerV3RLModule, TfRLModule):
    framework: str = "tf2"

    def __init__(self, *args, **kwargs):
        TfRLModule.__init__(self, *args, **kwargs)
        DreamerV3RLModule.__init__(self, *args, **kwargs)

    @override(RLModule)
    def get_initial_state(self) -> NestedDict:
        return self.dreamer_model.get_initial_state()

    @override(RLModule)
    def _forward_inference(self, batch: NestedDict) -> Mapping[str, Any]:
        # TODO (sven): For now, Dreamer always uses exploratory behavior.
        return self._forward_exploration(batch)

    @override(RLModule)
    def _forward_exploration(self, batch: NestedDict) -> Mapping[str, Any]:
        actions, next_state = self.dreamer_model.forward_inference(
            previous_states=batch[STATE_IN],
            observations=batch[SampleBatch.OBS],
            is_first=batch["is_first"],
        )
        return {
            SampleBatch.ACTIONS: actions,
            STATE_OUT: next_state,
        }

    @override(RLModule)
    def _forward_train(self, batch: NestedDict):
        return self.dreamer_model.forward_train(
            observations=batch[SampleBatch.OBS],
            actions=batch[SampleBatch.ACTIONS],
            is_first=batch["is_first"],
        )
