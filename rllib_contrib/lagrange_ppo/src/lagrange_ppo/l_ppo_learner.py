# from collections import deque
from dataclasses import dataclass
from typing import Dict

from ray.rllib.algorithms.ppo.ppo_learner import PPOLearner, PPOLearnerHyperparameters
from ray.rllib.core.rl_module.rl_module import ModuleID
from ray.rllib.utils.annotations import override
from ray.rllib.utils.lambda_defaultdict import LambdaDefaultDict
from ray.rllib.utils.schedules.scheduler import Scheduler

LEARNER_RESULTS_VF_LOSS_UNCLIPPED_KEY = "vf_loss_unclipped"
LEARNER_RESULTS_VF_EXPLAINED_VAR_KEY = "vf_explained_var"
LEARNER_RESULTS_KL_KEY = "mean_kl_loss"
LEARNER_RESULTS_CURR_KL_COEFF_KEY = "curr_kl_coeff"
LEARNER_RESULTS_CURR_ENTROPY_COEFF_KEY = "curr_entropy_coeff"

LEARNER_RESULTS_CURR_LARGANGE_PENALTY_COEFF_KEY = "curr_lagrange_penalty_coeff"
LEARNER_RESULTS_MEAN_CONST_VIOL = "mean_constraint_violation"
PENALTY = "penalty_coefficient"
P_PART = "P"
I_PART = "I"
D_PART = "D"
SMOOTHED_VIOLATION = "smoothed_mean_violation"
POLICY_COST_LOSS_KEY = "policy_cost_loss"
POLICY_REWARD_LOSS_KEY = "policy_reward_loss"
MEAN_ACCUMULATED_COST = "mean_accumulated_cost"


@dataclass
class PPOLagrangeLearnerHyperparameters(PPOLearnerHyperparameters):
    """Hyperparameters for the PPOLagrangeLearner sub-classes (framework specific).

    These should never be set directly by the user. Instead, use the PPOLagrangeConfig
    class to configure your algorithm.
    See `ray.rllib.algorithms.......::PPOLagrangeConfig::training()` for more details
    on the individual properties.
    """

    learn_penalty_coeff: bool = None
    use_cost_critic: bool = None
    # cost_advant_std: bool = False
    # safety hyperparameters
    track_debuging_values: bool = False
    penalty_coeff_lr: float = 1e-2
    init_penalty_coeff: float = 0.0
    clip_cost_cvf: bool = False
    cvf_loss_coeff: float = 1.0
    cvf_clip_param: float = 1000.0
    cost_limit: float = 25.0
    # PID Lagrange coefficients
    p_coeff: float = 0.0
    d_coeff: float = 0.0
    polyak_coeff: float = 1.0
    max_penalty_coeff: float = 100.0
    # history of largrange coefficients
    penalty_coefficient: float = 0.0
    smoothed_violation: float = 0.0
    i_part: float = 0.0


class PPOLagrangeLearner(PPOLearner):
    @override(PPOLearner)
    def build(self) -> None:
        super().build()

        # Set up Lagrangian penalty coefficient variables (per module).
        # The penalty coefficient is update in
        # `self.additional_update_for_module()`.
        self.curr_lagrange_penalty_coeffs_per_module: Dict[
            ModuleID, Scheduler
        ] = LambdaDefaultDict(
            lambda module_id: {
                PENALTY: self._get_tensor_variable(
                    self.hps.get_hps_for_module(module_id).penalty_coefficient
                ),
                SMOOTHED_VIOLATION: self._get_tensor_variable(
                    self.hps.get_hps_for_module(module_id).smoothed_violation
                ),
                I_PART: self._get_tensor_variable(
                    self.hps.get_hps_for_module(module_id).i_part
                ),
            }
        )

    @override(PPOLearner)
    def remove_module(self, module_id: str):
        super().remove_module(module_id)
        self.curr_lagrange_penalty_coeffs_per_module.pop(module_id)
