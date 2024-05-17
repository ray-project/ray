"""
[1] Mastering Diverse Domains through World Models - 2023
D. Hafner, J. Pasukonis, J. Ba, T. Lillicrap
https://arxiv.org/pdf/2301.04104v1.pdf

[2] Mastering Atari with Discrete World Models - 2021
D. Hafner, T. Lillicrap, M. Norouzi, J. Ba
https://arxiv.org/pdf/2010.02193.pdf
"""
from typing import Any, DefaultDict, Dict

from ray.rllib.algorithms.dreamerv3.dreamerv3 import DreamerV3Config
from ray.rllib.core.learner.learner import Learner
from ray.rllib.policy.sample_batch import MultiAgentBatch
from ray.rllib.utils.annotations import override
from ray.rllib.utils.typing import ModuleID, TensorType


class DreamerV3Learner(Learner):
    """DreamerV3 specific Learner class.

    Only implements the `additional_update_for_module()` method to define the logic
    for updating the critic EMA-copy after each training step.
    """

    @override(Learner)
    def additional_update_for_module(
        self,
        *,
        module_id: ModuleID,
        config: DreamerV3Config,
        timestep: int,
    ) -> None:
        """Updates the EMA weights of the critic network."""

        # Call the base class' method.
        super().additional_update_for_module(
            module_id=module_id, config=config, timestep=timestep
        )

        #TODO: Move the below somehow into DreamerV3.training_step, so we won't need additional_update anymore at all.
        # similar to new IMPALA.
        a=1
        # Update EMA weights of the critic.
        self.module[module_id].critic.update_ema()
