from typing import Mapping
from ray.rllib.core.learner.learner import LearnerHyperparameters
from ray.rllib.core.learner.tf.tf_learner import TfLearner
from ray.rllib.core.rl_module.rl_module import ModuleID
from ray.rllib.utils.annotations import override
from ray.rllib.utils.nested_dict import NestedDict
from ray.rllib.utils.typing import TensorType


class DQNTfLearner(TfLearner):
    """Implements loss and update logic for DQN."""

    @override(TfLearner)
    def compute_loss_for_module(
        self,
        *,
        module_id: ModuleID,
        hps: LearnerHyperparameters,
        batch: NestedDict,
        fwd_out: Mapping[str, TensorType]
    ) -> TensorType:
        
        pass
