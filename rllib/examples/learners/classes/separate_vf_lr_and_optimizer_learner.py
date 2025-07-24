from ray.rllib.algorithms.algorithm_config import AlgorithmConfig
from ray.rllib.algorithms.ppo.torch.ppo_torch_learner import PPOTorchLearner
from ray.rllib.core.learner.torch.torch_learner import TorchLearner
from ray.rllib.utils.annotations import override
from ray.rllib.utils.framework import try_import_torch
from ray.rllib.utils.typing import ModuleID

torch, _ = try_import_torch()


class PPOTorchLearnerWithSeparateVfOptimizer(PPOTorchLearner):
    """A custom PPO torch learner with 2 optimizers, for policy and value function.

    Overrides the Learner's standard `configure_optimizers_for_module()` method to
    register the additional vf optimizer.

    The standard PPOLearner only uses a single optimizer (and single learning rate) to
    update the model, regardless of whether the value function network
    is separate from the policy network or whether they have shared components.

    We may leave the loss function of PPO completely untouched. It already returns a
    sum of policy loss and vf loss (and entropy loss), and thus - given that the neural
    networks used to compute each of these terms are separate and don't share any
    components - gradients are computed separately per neural network (policy vs vf)
    and applied separately through the two optimizers.
    """

    @override(TorchLearner)
    def configure_optimizers_for_module(
        self,
        module_id: ModuleID,
        config: "AlgorithmConfig" = None,
    ) -> None:
        """Registers 2 optimizers for the given ModuleID with this Learner."""

        # Make sure the RLModule has the correct properties.
        module = self.module[module_id]
        # TODO (sven): We should move this into a new `ValueFunction` API, which
        #  should has-a `get_value_function_params` method. This way, any custom
        #  RLModule that implements this API can be used here, not just the standard
        #  PPO one.
        assert (
            hasattr(module, "pi")
            and hasattr(module, "vf")
            and hasattr(module, "encoder")
            and hasattr(module.encoder, "actor_encoder")
            and hasattr(module.encoder, "critic_encoder")
        )
        assert config.model_config["vf_share_layers"] is False

        # Get all policy-related parameters from the RLModule.
        pi_params = (
            # Actor encoder and policy head.
            self.get_parameters(self.module[module_id].encoder.actor_encoder)
            + self.get_parameters(self.module[module_id].pi)
        )
        # Register the policy optimizer.
        self.register_optimizer(
            module_id=module_id,
            optimizer_name="optim_for_pi",
            optimizer=torch.optim.Adam(params=pi_params),
            params=pi_params,
            # For the policy learning rate, we use the "main" lr in the AlgorithmConfig.
            lr_or_lr_schedule=config.lr,
        )

        # Get all value function-related parameters from the RLModule.
        vf_params = (
            # Critic encoder and value head.
            self.get_parameters(self.module[module_id].encoder.critic_encoder)
            + self.get_parameters(self.module[module_id].vf)
        )
        # Register the value function optimizer.
        self.register_optimizer(
            module_id=module_id,
            optimizer_name="optim_for_vf",
            optimizer=torch.optim.Adam(params=vf_params),
            params=vf_params,
            # For the value function learning rate, we use a user-provided custom
            # setting in the `learner_config_dict` in the AlgorithmConfig. If this
            # is not provided, use the same lr as for the policy optimizer.
            lr_or_lr_schedule=config.learner_config_dict.get("lr_vf", config.lr),
        )
