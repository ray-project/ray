from typing import Any, Dict

from ray.rllib.algorithms.algorithm_config import AlgorithmConfig
from ray.rllib.algorithms.dqn.torch.dqn_rainbow_torch_learner import (
    DQNRainbowTorchLearner,
)
from ray.rllib.algorithms.sac.sac import SACConfig
from ray.rllib.algorithms.sac.sac_learner import (
    LOGPS_KEY,
    QF_LOSS_KEY,
    QF_MEAN_KEY,
    QF_MAX_KEY,
    QF_MIN_KEY,
    QF_PREDS,
    QF_TWIN_LOSS_KEY,
    QF_TWIN_PREDS,
    TD_ERROR_MEAN_KEY,
    SACLearner,
)
from ray.rllib.core.columns import Columns
from ray.rllib.core.learner.learner import (
    POLICY_LOSS_KEY,
)
from ray.rllib.utils.annotations import override
from ray.rllib.utils.framework import try_import_torch
from ray.rllib.utils.metrics import ALL_MODULES, TD_ERROR_KEY
from ray.rllib.utils.typing import ModuleID, ParamDict, TensorType


torch, nn = try_import_torch()


class SACTorchLearner(DQNRainbowTorchLearner, SACLearner):
    """Implements `torch`-specific SAC loss logic on top of `SACLearner`

    This ' Learner' class implements the loss in its
    `self.compute_loss_for_module()` method. In addition, it updates
    the target networks of the RLModule(s).
    """

    # TODO (simon): Set different learning rates for optimizers.
    @override(DQNRainbowTorchLearner)
    def configure_optimizers_for_module(
        self, module_id: ModuleID, config: AlgorithmConfig = None
    ) -> None:
        # Receive the module.
        module = self._module[module_id]

        # Define the optimizer for the critic.
        # TODO (sven): Maybe we change here naming to `qf` for unification.
        params_critic = self.get_parameters(module.qf_encoder) + self.get_parameters(
            module.qf
        )
        optim_critic = torch.optim.Adam(params_critic, eps=1e-7)

        self.register_optimizer(
            module_id=module_id,
            optimizer_name="qf",
            optimizer=optim_critic,
            params=params_critic,
            lr_or_lr_schedule=config.critic_lr,
        )
        # If necessary register also an optimizer for a twin Q network.
        if config.twin_q:
            params_twin_critic = self.get_parameters(
                module.qf_twin_encoder
            ) + self.get_parameters(module.qf_twin)
            optim_twin_critic = torch.optim.Adam(params_twin_critic, eps=1e-7)

            self.register_optimizer(
                module_id=module_id,
                optimizer_name="qf_twin",
                optimizer=optim_twin_critic,
                params=params_twin_critic,
                lr_or_lr_schedule=config.critic_lr,
            )

        # Define the optimizer for the actor.
        params_actor = self.get_parameters(module.pi_encoder) + self.get_parameters(
            module.pi
        )
        optim_actor = torch.optim.Adam(params_actor, eps=1e-7)

        self.register_optimizer(
            module_id=module_id,
            optimizer_name="policy",
            optimizer=optim_actor,
            params=params_actor,
            lr_or_lr_schedule=config.actor_lr,
        )

        # Define the optimizer for the temperature.
        temperature = self.curr_log_alpha[module_id]
        optim_temperature = torch.optim.Adam([temperature], eps=1e-7)
        self.register_optimizer(
            module_id=module_id,
            optimizer_name="alpha",
            optimizer=optim_temperature,
            params=[temperature],
            lr_or_lr_schedule=config.alpha_lr,
        )

    @override(DQNRainbowTorchLearner)
    def compute_loss_for_module(
        self,
        *,
        module_id: ModuleID,
        config: SACConfig,
        batch: Dict[str, Any],
        fwd_out: Dict[str, TensorType]
    ) -> TensorType:
        # Receive the current alpha hyperparameter.
        alpha = torch.exp(self.curr_log_alpha[module_id])

        # Get Q-values for the actually selected actions during rollout.
        # In the critic loss we use these as predictions.
        q_selected = fwd_out[QF_PREDS]
        if config.twin_q:
            q_twin_selected = fwd_out[QF_TWIN_PREDS]

        # Compute value function for next state (see eq. (3) in Haarnoja et al. (2018)).
        # Note, we use here the sampled actions in the log probabilities.
        q_target_next = (
            fwd_out["q_target_next"] - alpha.detach() * fwd_out["logp_next_resampled"]
        )
        # Now mask all Q-values with terminated next states in the targets.
        q_next_masked = (1.0 - batch[Columns.TERMINATEDS].float()) * q_target_next

        # Compute the right hand side of the Bellman equation.
        # Detach this node from the computation graph as we do not want to
        # backpropagate through the target network when optimizing the Q loss.
        q_selected_target = (
            batch[Columns.REWARDS] + (config.gamma ** batch["n_step"]) * q_next_masked
        ).detach()

        # Calculate the TD-error. Note, this is needed for the priority weights in
        # the replay buffer.
        td_error = torch.abs(q_selected - q_selected_target)
        # If a twin Q network should be used, add the TD error of the twin Q network.
        if config.twin_q:
            td_error += torch.abs(q_twin_selected - q_selected_target)
            # Rescale the TD error.
            td_error *= 0.5

        # MSBE loss for the critic(s) (i.e. Q, see eqs. (7-8) Haarnoja et al. (2018)).
        # Note, this needs a sample from the current policy given the next state.
        # Note further, we use here the Huber loss instead of the mean squared error
        # as it improves training performance.
        critic_loss = torch.mean(
            batch["weights"]
            * torch.nn.HuberLoss(reduction="none", delta=1.0)(
                q_selected, q_selected_target
            )
        )
        # If a twin Q network should be used, add the critic loss of the twin Q network.
        if config.twin_q:
            critic_twin_loss = torch.mean(
                batch["weights"]
                * torch.nn.HuberLoss(reduction="none", delta=1.0)(
                    q_twin_selected, q_selected_target
                )
            )

        # For the actor (policy) loss we need sampled actions from the current policy
        # evaluated at the current observations.
        # Note that the `q_curr` tensor below has the q-net's gradients ignored, while
        # having the policy's gradients registered. The policy net was used to rsample
        # actions used to compute `q_curr` (by passing these actions through the q-net).
        # Hence, we can't do `fwd_out[q_curr].detach()`!
        # Note further, we minimize here, while the original equation in Haarnoja et
        # al. (2018) considers maximization.
        # TODO (simon): Rename to `resampled` to `current`.
        actor_loss = torch.mean(
            alpha.detach() * fwd_out["logp_resampled"] - fwd_out["q_curr"]
        )

        # Optimize also the hyperparameter alpha by using the current policy
        # evaluated at the current state (sampled values).
        # TODO (simon): Check, why log(alpha) is used, prob. just better
        # to optimize and monotonic function. Original equation uses alpha.
        alpha_loss = -torch.mean(
            self.curr_log_alpha[module_id]
            * (fwd_out["logp_resampled"].detach() + self.target_entropy[module_id])
        )

        total_loss = actor_loss + critic_loss + alpha_loss
        # If twin Q networks should be used, add the critic loss of the twin Q network.
        if config.twin_q:
            # TODO (simon): Check, if we need to multiply the critic_loss then with 0.5.
            total_loss += critic_twin_loss

        # Log the TD-error with reduce=None, such that - in case we have n parallel
        # Learners - we will re-concatenate the produced TD-error tensors to yield
        # a 1:1 representation of the original batch.
        self.metrics.log_value(
            key=(module_id, TD_ERROR_KEY),
            value=td_error,
            reduce=None,
            clear_on_reduce=True,
        )
        # Log other important loss stats (reduce=mean (default), but with window=1
        # in order to keep them history free).
        self.metrics.log_dict(
            {
                POLICY_LOSS_KEY: actor_loss,
                QF_LOSS_KEY: critic_loss,
                "alpha_loss": alpha_loss,
                "alpha_value": alpha,
                "log_alpha_value": torch.log(alpha),
                "target_entropy": self.target_entropy[module_id],
                LOGPS_KEY: torch.mean(fwd_out["logp_resampled"]),
                QF_MEAN_KEY: torch.mean(fwd_out["q_curr"]),
                QF_MAX_KEY: torch.max(fwd_out["q_curr"]),
                QF_MIN_KEY: torch.min(fwd_out["q_curr"]),
                TD_ERROR_MEAN_KEY: torch.mean(td_error),
            },
            key=module_id,
            window=1,  # <- single items (should not be mean/ema-reduced over time).
        )
        # If twin Q networks should be used add a critic loss for the twin Q network.
        # Note, we need this in the `self.compute_gradients()` to optimize.
        if config.twin_q:
            self.metrics.log_dict(
                {
                    QF_TWIN_LOSS_KEY: critic_twin_loss,
                },
                key=module_id,
                window=1,  # <- single items (should not be mean/ema-reduced over time).
            )

        return total_loss

    @override(DQNRainbowTorchLearner)
    def compute_gradients(
        self, loss_per_module: Dict[ModuleID, TensorType], **kwargs
    ) -> ParamDict:
        grads = {}
        for module_id in set(loss_per_module.keys()) - {ALL_MODULES}:
            # Loop through optimizers registered for this module.
            for optim_name, optim in self.get_optimizers_for_module(module_id):
                # Zero the gradients. Note, we need to reset the gradients b/c
                # each component for a module operates on the same graph.
                optim.zero_grad(set_to_none=True)

                # Compute the gradients for the component and module.
                self.metrics.peek((module_id, optim_name + "_loss")).backward(
                    retain_graph=True
                )
                # Store the gradients for the component and module.
                grads.update(
                    {
                        pid: p.grad
                        for pid, p in self.filter_param_dict_for_optimizer(
                            self._params, optim
                        ).items()
                    }
                )

        return grads
