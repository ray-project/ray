from abc import abstractmethod
from typing import Any, Dict, Type

from ray.rllib.algorithms.ppo import PPOConfig
from ray.rllib.core import Columns
from ray.rllib.core.models.base import ENCODER_OUT
from ray.rllib.core.models.catalog import Catalog
from ray.rllib.core.models.configs import MLPHeadConfig
from ray.rllib.core.models.specs.specs_dict import SpecDict
from ray.rllib.core.rl_module.rl_module import RLModule, SingleAgentRLModuleSpec
from ray.rllib.core.rl_module.torch.torch_rl_module import TorchRLModule
from ray.rllib.examples.envs.classes.correlated_actions_env import CorrelatedActionsEnv
from ray.rllib.models.distributions import Distribution
from ray.rllib.utils.annotations import override
from ray.rllib.utils.framework import try_import_torch
from ray.rllib.utils.torch_utils import convert_to_torch_tensor
from ray.rllib.utils.typing import TensorType

from ray.tune import register_env

register_env("correlated_actions_env", lambda _: CorrelatedActionsEnv(_))


torch, nn = try_import_torch()


class AutoregressiveActionRLM(RLModule):
    def setup(self):
        super().setup()

        # Build the encoder.
        self.encoder = self.config.get_catalog().build_encoder(framework=self.framework)

        # Build the prior and posterior heads.
        # Note, the action space is a Tuple space.
        self.action_dist_cls = self.config.get_catalog().get_action_dist_cls(
            self.framework
        )
        # Note further, we neet to know the required input dimensions for
        # the partial distributions.
        self.required_output_dims = self.action_dist_cls.required_input_dim(
            space=self.config.action_space,
            model_config=self.config.model_config_dict,
            as_list=True,
        )
        action_dims = self.config.action_space[0].shape or (1,)
        latent_dims = self.config.get_catalog().latent_dims
        prior_config = MLPHeadConfig(
            # Use the hidden dimension from the encoder output.
            input_dims=latent_dims,
            # Use configurations from the `model_config_dict`.
            hidden_layer_dims=self.config.model_config_dict["post_fcnet_hiddens"],
            hidden_layer_activation=self.config.model_config_dict[
                "post_fcnet_activation"
            ],
            output_layer_dim=self.required_output_dims[0],
            output_layer_activation="linear",
        )

        posterior_config = MLPHeadConfig(
            input_dims=(latent_dims[0] + action_dims[0],),
            hidden_layer_dims=self.config.model_config_dict["post_fcnet_hiddens"],
            hidden_layer_activation=self.config.model_config_dict[
                "post_fcnet_activation"
            ],
            output_layer_dim=self.required_output_dims[1],
            output_layer_activation="linear",
        )

        self.prior = prior_config.build(framework=self.framework)
        self.posterior = posterior_config.build(framework=self.framework)

        # Build the value function head.
        vf_config = MLPHeadConfig(
            input_dims=latent_dims,
            hidden_layer_dims=self.config.model_config_dict["post_fcnet_hiddens"],
            hidden_layer_activation=self.config.model_config_dict[
                "post_fcnet_activation"
            ],
            output_layer_dim=1,
            output_layer_activation="linear",
        )
        self.vf = vf_config.build(framework=self.framework)

    @override(RLModule)
    def get_train_action_dist_cls(self) -> Type[Distribution]:
        return self.action_dist_cls

    @override(RLModule)
    def get_exploration_action_dist_cls(self) -> Type[Distribution]:
        return self.action_dist_cls

    @override(RLModule)
    def get_inference_action_dist_cls(self) -> Type[Distribution]:
        return self.action_dist_cls

    @override(RLModule)
    def output_specs_inference(self) -> SpecDict:
        return [Columns.ACTION_DIST_INPUTS, Columns.ACTIONS, Columns.ACTION_LOGP]

    @override(RLModule)
    def output_specs_exploration(self) -> SpecDict:
        return self.output_specs_inference()

    @override(RLModule)
    def output_specs_train(self) -> SpecDict:
        return [
            Columns.ACTION_DIST_INPUTS,
            Columns.ACTIONS,
            Columns.ACTION_LOGP,
            Columns.VF_PREDS,
        ]

    @abstractmethod
    def pi(self, batch: Dict[str, TensorType]) -> Dict[str, TensorType]:
        """Computes the policy outputs given a batch of data.

        Args:
            batch: The input batch to pass through the policy head.

        Returns:
            A dict mapping Column names to batches of policy outputs.
        """

    @abstractmethod
    def _compute_values(self, batch) -> Any:
        """Computes values using the vf-specific network(s) and given a batch of data.

        Args:
            batch: The input batch to pass through this RLModule (value function
                encoder and vf-head).

        Returns:
            A dict mapping ModuleIDs to batches of value function outputs (already
            squeezed on the last dimension (which should have shape (1,) b/c of the
            single value output node). However, for complex multi-agent settings with
            shareed value networks, the output might look differently (e.g. a single
            return batch without the ModuleID-based mapping).
        """


class AutoregressiveActionTorchRLM(TorchRLModule, AutoregressiveActionRLM):
    @override(AutoregressiveActionRLM)
    def pi(self, batch: Dict[str, TensorType]) -> Dict[str, TensorType]:
        pi_outs = {}

        # Prior forward pass.
        prior_out = self.prior(batch)
        prior_logits = torch.cat(
            [
                prior_out,
                torch.zeros(size=(prior_out.shape[0], self.required_output_dims[1])),
            ],
            dim=-1,
        )
        # Get the prior action distribution to sample the prior action.
        prior_action_dist = self.action_dist_cls.from_logits(prior_logits)
        # Note, `TorchMultiDistribution from_logits` does set the `logits`, but not
        # the `probs` attribute. We need to set the `probs` attribute to be able to
        # sample from the distribution in a differentiable way.
        prior_action_dist._flat_child_distributions[0].probs = torch.softmax(
            prior_out, dim=-1
        )
        prior_action_dist._flat_child_distributions[0].logits = None
        # Note, we need to be able to backpropagate through the prior action that's
        # why we sample from the distribution using the `rsample` method.
        # TODO (simon, sven): Check, if we need return the one-hot sampled action
        # instead of the
        prior_action = torch.argmax(
            prior_action_dist._flat_child_distributions[0].rsample(),
            dim=-1,
        )
        # We need the log probability of the sampled action for the loss calculation.
        prior_action_logp = prior_action_dist._flat_child_distributions[0].logp(
            prior_action
        )

        # Posterior forward pass.
        posterior_batch = torch.cat([batch, prior_action.view(-1, 1)], dim=-1)
        posterior_out = self.posterior(posterior_batch)
        # Concatenate the prior and posterior logits to get the final logits.
        posterior_logits = torch.cat([prior_out, posterior_out], dim=-1)
        # Get the posterior action distribution to sample the posterior action.
        posterior_action_dist = self.action_dist_cls.from_logits(posterior_logits)
        posterior_action = posterior_action_dist._flat_child_distributions[1].sample()
        posterior_action_logp = posterior_action_dist._flat_child_distributions[1].logp(
            posterior_action
        )

        # Concatenate the prior and posterior actions and log probabilities.
        pi_outs[Columns.ACTIONS] = (prior_action, posterior_action)
        pi_outs[Columns.ACTION_LOGP] = torch.cat(
            [prior_action_logp, posterior_action_logp], dim=-1
        )
        pi_outs[Columns.ACTION_DIST_INPUTS] = posterior_logits
        return pi_outs

    @override(TorchRLModule)
    def _forward_inference(self, batch: Dict[str, TensorType]) -> Dict[str, TensorType]:

        # Encoder forward pass.
        encoder_out = self.encoder(batch)

        # Policy head forward pass.
        return self.pi(encoder_out[ENCODER_OUT])

    @override(TorchRLModule)
    def _forward_exploration(
        self, batch: Dict[str, TensorType], **kwargs
    ) -> Dict[str, TensorType]:
        return self._forward_inference(batch)

    @override(TorchRLModule)
    def _forward_train(self, batch: Dict[str, TensorType]) -> Dict[str, TensorType]:

        outs = {}

        # Encoder forward pass.
        encoder_out = self.encoder(batch)

        # Policy head forward pass.
        outs.update(self.pi(encoder_out[ENCODER_OUT]))

        # Value function head forward pass.
        vf_out = self.vf(encoder_out[ENCODER_OUT])
        outs[Columns.VF_PREDS] = vf_out.squeeze(-1)

        return outs

    @override(AutoregressiveActionRLM)
    def _compute_values(self, batch, device=None):
        infos = batch.pop(Columns.INFOS, None)
        batch = convert_to_torch_tensor(batch, device=device)
        if infos is not None:
            batch[Columns.INFOS] = infos

        # Encoder forward pass.
        encoder_outs = self.encoder(batch)[ENCODER_OUT]

        # Value head forward pass.
        vf_out = self.vf(encoder_outs)

        # Squeeze out last dimension (single node value head).
        return vf_out.squeeze(-1)


# from gymnasium.envs.registration import register
# import gymnasium as gym

# register(
#     id="correlated_actions_env",
#     entry_point="ray.rllib.examples.envs.classes.correlated_actions_env:CorrelatedActionsEnv",
# )
# env = gym.make("correlated_actions_env")
# spec = SingleAgentRLModuleSpec(
#     module_class=AutoregressiveActionTorchRLM,
#     observation_space=env.observation_space,
#     action_space=env.action_space,
#     model_config_dict={
#         "fcnet_hiddens": [64],
#         "post_fcnet_hiddens": [256],
#         "post_fcnet_activation": "relu",
#     },
#     catalog_class=Catalog,
# )
# module = spec.build()
# obs = env.observation_space.sample()
# outs = module._forward_inference({Columns.OBS: torch.tensor(obs).view(-1, 1)})
config = (
    PPOConfig()
    .environment(env="correlated_actions_env")
    .api_stack(
        enable_rl_module_and_learner=True,
        enable_env_runner_and_connector_v2=True,
    )
    .rl_module(
        model_config_dict={
            "post_fcnet_hiddens": [256],
        },
        rl_module_spec=SingleAgentRLModuleSpec(
            module_class=AutoregressiveActionTorchRLM,
            catalog_class=Catalog,
        ),
    )
    .env_runners(
        num_env_runners=0,
        num_envs_per_env_runner=1,
    )
)

algo = config.build()

for _ in range(2):

    results = algo.train()
