from typing import Dict

from ray.rllib.algorithms.algorithm_config import AlgorithmConfig
from ray.rllib.algorithms.dqn.dqn_learner import QF_PREDS, QF_LOSS_KEY
from ray.rllib.algorithms.iql.iql_learner import (
    IQLLearner,
    QF_TARGET_PREDS,
    VF_PREDS_NEXT,
    VF_LOSS,
)
from ray.rllib.algorithms.sac.sac_learner import QF_TWIN_PREDS, QF_TWIN_LOSS_KEY
from ray.rllib.core import ALL_MODULES
from ray.rllib.core.columns import Columns
from ray.rllib.core.learner.learner import (
    POLICY_LOSS_KEY,
)
from ray.rllib.core.learner.torch.torch_learner import TorchLearner
from ray.rllib.utils.annotations import override
from ray.rllib.utils.framework import try_import_torch
from ray.rllib.utils.typing import ModuleID, ParamDict, TensorType

torch, nn = try_import_torch()


class IQLTorchLearner(TorchLearner, IQLLearner):
    """Implements the IQL loss on top of `IQLLearner`.

    This Learner implements configure_optimizers_for_module to define
    separate optimizers for the policy, Q-, and value networks. When
    using a twin-Q network architecture, each Q-network is assigned its
    own optimizer—consistent with the SAC algorithm.

    The IQL loss is defined in compute_loss_for_module and consists of
    three components: value loss, Q-loss (TD error), and actor (policy)
    loss.

    Note that the original IQL implementation performs separate backward
    passes for each network. However, due to RLlib's reliance on TorchDDP,
    all backward passes must be executed within a single update step. This
    constraint can lead to parameter lag and cyclical loss behavior, though
    it does not hinder convergence.
    """

    @override(TorchLearner)
    def configure_optimizers_for_module(
        self, module_id: ModuleID, config: AlgorithmConfig = None
    ) -> None:

        # Note, we could have derived directly from SACTorchLearner to
        # inherit the setup of optimizers, but that learner comes with
        # additional parameters which we do not need.
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

        # Define the optimizer for the value function.
        params_value = self.get_parameters(module.vf_encoder) + self.get_parameters(
            module.vf
        )
        optim_value = torch.optim.Adam(params_value, eps=1e-7)
        self.register_optimizer(
            module_id=module_id,
            optimizer_name="value",
            optimizer=optim_value,
            params=params_value,
            lr_or_lr_schedule=config.value_lr,
        )

    @override(TorchLearner)
    def compute_loss_for_module(
        self,
        *,
        module_id: ModuleID,
        config: AlgorithmConfig,
        batch: Dict,
        fwd_out: Dict
    ):

        # Get the module and hyperparameters.
        module = self._module[module_id]
        expectile = self.expectile[module_id]
        temperature = self.temperature[module_id]

        # Get the action distribution for the actor loss.
        action_train_dist_class = module.get_train_action_dist_cls()
        action_train_dist = action_train_dist_class.from_logits(
            fwd_out[Columns.ACTION_DIST_INPUTS]
        )

        # First, compute the value loss via the target Q-network and current observations.
        value_loss = torch.mean(
            self._expectile_loss(
                fwd_out[QF_TARGET_PREDS] - fwd_out[Columns.VF_PREDS], expectile
            )
        )

        # Second, compute the actor loss using the target-Q network and values.
        exp_advantages = torch.minimum(
            torch.exp(
                temperature * (fwd_out[QF_TARGET_PREDS] - fwd_out[Columns.VF_PREDS])
            ),
            torch.Tensor([100.0]).to(self.device),
        )
        # Note, we are using here the actions from the data sample.
        action_logps = action_train_dist.logp(batch[Columns.ACTIONS])
        # Compute the actor loss.
        actor_loss = -torch.mean(exp_advantages.detach() * action_logps)

        # Third, compute the critic loss.
        target_critic = (
            batch[Columns.REWARDS]
            + config.gamma
            * (1 - batch[Columns.TERMINATEDS].float())
            * fwd_out[VF_PREDS_NEXT].detach()
        )

        critic_loss = torch.mean(
            torch.nn.MSELoss(reduction="none")(target_critic, fwd_out[QF_PREDS])
        )

        # If we have a twin-Q architecture, calculate the its loss, too.
        if config.twin_q:
            critic_twin_loss = (
                torch.mean(
                    torch.nn.MSELoss(reduction="none")(
                        target_critic, fwd_out[QF_TWIN_PREDS]
                    )
                )
                * 0.5
            )
            critic_loss *= 0.5

        # Compute the total loss.
        total_loss = value_loss + actor_loss + critic_loss

        # If we have a twin-Q architecture, add its loss.
        if config.twin_q:
            total_loss += critic_twin_loss

        # Log metrics.
        self.metrics.log_dict(
            {
                POLICY_LOSS_KEY: actor_loss,
                QF_LOSS_KEY: critic_loss,
            },
            key=module_id,
            window=1,  # <- single items (should not be mean/ema-reduced over time).
        )

        # Log the losses also in the temporary containers for gradient computation.
        self._temp_losses[(module_id, POLICY_LOSS_KEY)] = actor_loss
        self._temp_losses[(module_id, QF_LOSS_KEY)] = critic_loss
        self._temp_losses[(module_id, VF_LOSS)] = value_loss

        # If a twin-Q architecture is used add metrics and loss.
        if config.twin_q:
            self.metrics.log_value(
                key=(module_id, QF_TWIN_LOSS_KEY),
                value=critic_twin_loss,
                window=1,  # <- single items (should not be mean/ema-reduced over time).
            )
            self._temp_losses[(module_id, QF_TWIN_LOSS_KEY)] = critic_twin_loss

        return total_loss

    @override(TorchLearner)
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
                loss_tensor = self._temp_losses.pop((module_id, optim_name + "_loss"))
                loss_tensor.backward(retain_graph=True)
                # Store the gradients for the component and module.
                grads.update(
                    {
                        pid: p.grad
                        for pid, p in self.filter_param_dict_for_optimizer(
                            self._params, optim
                        ).items()
                    }
                )

        # Make sure we updated on all loss terms.
        assert not self._temp_losses
        return grads

    def _expectile_loss(self, diff: TensorType, expectile: TensorType) -> TensorType:
        """Computes the expectile loss.

        Args:
            diff: A tensor containing a difference loss.
            expectile: The expectile to use for the expectile loss.

        Returns:
            The expectile loss of `diff` using `expectile`.
        """
        weight = torch.where(diff > 0, expectile, 1 - expectile)
        return weight * torch.pow(diff, 2)
