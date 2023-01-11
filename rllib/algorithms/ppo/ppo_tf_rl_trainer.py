from typing import Any, Dict, Mapping, Union, Type

from ray.rllib.core.rl_trainer.tf.tf_rl_trainer import TfRLTrainer
from ray.rllib.core.rl_module.rl_module import ModuleID, RLModule, MultiAgentRLModule
from ray.rllib.evaluation.postprocessing import (
    Postprocessing,
)
from ray.rllib.utils.annotations import override
from ray.rllib.utils.framework import try_import_tf
from ray.rllib.policy.sample_batch import SampleBatch, MultiAgentBatch


tf1, tf, tfv = try_import_tf()
tf1.enable_eager_execution()


class PPOTfTrainer(TfRLTrainer):
    def __init__(
        self,
        module_class: Union[Type[RLModule], Type[MultiAgentRLModule]],
        module_kwargs: Mapping[str, Any],
        scaling_config: Mapping[str, Any],
        optimizer_config: Mapping[str, Any],
        entropy_objective_coefficient: float,
        clip_coefficient: float,
        distributed: bool = False,
        in_test: bool = False,
    ):
        super().__init__(
            module_class=module_class,
            module_kwargs=module_kwargs,
            scaling_config=scaling_config,
            optimizer_config=optimizer_config,
            distributed=distributed,
            in_test=in_test,
        )

        self._entropy_objective_coefficient = entropy_objective_coefficient
        self._clip_coefficient = clip_coefficient

    @override(TfRLTrainer)
    def compute_loss(
        self, fwd_out: Dict[ModuleID, Mapping[str, Any]], batch: MultiAgentBatch
    ):
        loss = {}
        total_losses = []
        for module_id in fwd_out.keys():
            fwd_out_module = fwd_out[module_id]
            module_batch = batch[module_id]
            module_loss_dict = self._loss_function(fwd_out_module, module_batch)
            total_loss_term = module_loss_dict["total_loss"]
            total_losses.append(total_loss_term)
            module_loss_dict.pop("total_loss")
            loss[module_id] = module_loss_dict
        total_loss = tf.math.add_n(total_losses)
        loss[self.TOTAL_LOSS_KEY] = total_loss
        return loss

    def _loss_function(self, fwd_out_module: Mapping[str, Any], batch: SampleBatch):
        curr_probs = fwd_out_module[SampleBatch.ACTION_LOGP]
        old_probs = batch[SampleBatch.ACTION_LOGP]
        likelihood_ratio = tf.math.exp(curr_probs - old_probs)
        clipped_likelihood = tf.clip_by_value(
            likelihood_ratio, 1 - self._clip_coefficient, 1 + self._clip_coefficient
        )

        advantages = batch[Postprocessing.ADVANTAGES]
        ppo_clipped_objective = tf.math.minimum(
            likelihood_ratio * advantages, clipped_likelihood * advantages
        )

        entropy_objective = (
            self._entropy_objective_coefficient * fwd_out_module["entropy"]
        )

        vf_loss = tf.math.square(
            fwd_out_module["vf"] - batch[Postprocessing.VALUE_TARGETS]
        )

        loss = ppo_clipped_objective + entropy_objective - vf_loss

        return {
            "total_loss": loss,
            "ppo_clipped_objective": ppo_clipped_objective,
            "entropy_objective": entropy_objective,
            "vf_loss": vf_loss,
        }
