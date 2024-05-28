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
    def compile_results(
        self,
        *,
        batch: MultiAgentBatch,
        fwd_out: Dict[str, Any],
        loss_per_module: Dict[str, TensorType],
        metrics_per_module: DefaultDict[ModuleID, Dict[str, Any]],
    ) -> Dict[str, Any]:
        results = super().compile_results(
            batch=batch,
            fwd_out=fwd_out,
            loss_per_module=loss_per_module,
            metrics_per_module=metrics_per_module,
        )

        # Add the predicted obs distributions for possible (video) summarization.
        if self.config.report_images_and_videos:
            for module_id, res in results.items():
                if module_id in fwd_out:
                    res["WORLD_MODEL_fwd_out_obs_distribution_means_BxT"] = fwd_out[
                        module_id
                    ]["obs_distribution_means_BxT"]
        return results

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

        # Update EMA weights of the critic.
        self.module[module_id].critic.update_ema()
