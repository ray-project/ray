from typing import Dict

from ray.rllib.algorithms.algorithm_config import AlgorithmConfig
from ray.rllib.algorithms.dqn.dqn_learner import QF_PREDS, QF_LOSS_KEY
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

from d4rllib.algorithms.iql.iql_learner import (
    IQLLearner,
    QF_TARGET_PREDS,
    VF_PREDS_NEXT,
    VF_LOSS,
)

torch, nn = try_import_torch()


class IQLTorchLearner(TorchLearner, IQLLearner):

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

        module = self._module[module_id]
        expectile = self.expectile[module_id]
        temperature = self.temperature[module_id]

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

        # Second, compute the actor loss.
        exp_advantages = torch.minimum(
            torch.exp(
                temperature * (fwd_out[QF_TARGET_PREDS] - fwd_out[Columns.VF_PREDS])
            ),
            torch.Tensor([100.0]),
        )
        # Note, we are using here the actions from the data sample.
        action_logps = action_train_dist.logp(batch[Columns.ACTIONS])

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

        total_loss = value_loss + actor_loss + critic_loss

        if config.twin_q:
            total_loss += critic_twin_loss

        self.metrics.log_dict(
            {
                POLICY_LOSS_KEY: actor_loss,
                QF_LOSS_KEY: critic_loss,
            },
            key=module_id,
            window=1,  # <- single items (should not be mean/ema-reduced over time).
        )

        self._temp_losses[(module_id, POLICY_LOSS_KEY)] = actor_loss
        self._temp_losses[(module_id, QF_LOSS_KEY)] = critic_loss
        self._temp_losses[(module_id, VF_LOSS)] = value_loss

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

        assert not self._temp_losses
        return grads

    def _expectile_loss(self, diff, expectile: TensorType) -> TensorType:

        weight = torch.where(diff > 0, expectile, 1 - expectile)
        return weight * torch.pow(diff, 2)
