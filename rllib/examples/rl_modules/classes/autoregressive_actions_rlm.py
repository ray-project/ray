import abc
from abc import abstractmethod
from typing import Dict

from ray.rllib.core import Columns
from ray.rllib.core.models.base import ENCODER_OUT
from ray.rllib.core.models.configs import MLPHeadConfig
from ray.rllib.core.rl_module.apis.value_function_api import ValueFunctionAPI
from ray.rllib.core.rl_module.rl_module import RLModule
from ray.rllib.core.rl_module.torch.torch_rl_module import TorchRLModule
from ray.rllib.utils.annotations import (
    override,
    OverrideToImplementCustomLogic_CallToSuperRecommended,
)
from ray.rllib.utils.framework import try_import_torch
from ray.rllib.utils.typing import TensorType

torch, nn = try_import_torch()


# TODO (simon): Improvements: `inference-only` mode.
class AutoregressiveActionsRLM(RLModule, ValueFunctionAPI, abc.ABC):
    """An RLModule that implements an autoregressive action distribution.

    This RLModule implements an autoregressive action distribution, where the
    action is sampled in two steps. First, the prior action is sampled from a
    prior distribution. Then, the posterior action is sampled from a posterior
    distribution that depends on the prior action and the input data. The prior
    and posterior distributions are implemented as MLPs.

    The following components are implemented:
    - ENCODER: An encoder that processes the observations from the environment.
    - PI: A Policy head that outputs the actions, the log probabilities of the
        actions, and the input to the action distribution. This head is composed
        of two sub-heads:
        - A prior head that outputs the logits for the prior action distribution.
        - A posterior head that outputs the logits for the posterior action
            distribution.
    - VF: A value function head that outputs the value function.

    Note, this RLModule is implemented for the `PPO` algorithm only. It is not
    guaranteed to work with other algorithms.
    """

    @override(RLModule)
    @OverrideToImplementCustomLogic_CallToSuperRecommended
    def setup(self):
        super().setup()

        # Build the encoder.
        self.encoder = self.catalog.build_encoder(framework=self.framework)

        # Build the prior and posterior heads.
        # Note further, we neet to know the required input dimensions for
        # the partial distributions.
        self.required_output_dims = self.action_dist_cls.required_input_dim(
            space=self.action_space,
            as_list=True,
        )
        action_dims = self.action_space[0].shape or (1,)
        latent_dims = self.catalog.latent_dims
        prior_config = MLPHeadConfig(
            # Use the hidden dimension from the encoder output.
            input_dims=latent_dims,
            # Use configurations from the `model_config`.
            hidden_layer_dims=self.model_config["head_fcnet_hiddens"],
            hidden_layer_activation=self.model_config["head_fcnet_activation"],
            output_layer_dim=self.required_output_dims[0],
            output_layer_activation="linear",
        )
        # Define the posterior head.
        posterior_config = MLPHeadConfig(
            input_dims=(latent_dims[0] + action_dims[0],),
            hidden_layer_dims=self.model_config["head_fcnet_hiddens"],
            hidden_layer_activation=self.model_config["head_fcnet_activation"],
            output_layer_dim=self.required_output_dims[1],
            output_layer_activation="linear",
        )

        # Build the policy heads.
        self.prior = prior_config.build(framework=self.framework)
        self.posterior = posterior_config.build(framework=self.framework)

        # Build the value function head.
        vf_config = MLPHeadConfig(
            input_dims=latent_dims,
            hidden_layer_dims=self.model_config["head_fcnet_hiddens"],
            hidden_layer_activation=self.model_config["head_fcnet_activation"],
            output_layer_dim=1,
            output_layer_activation="linear",
        )
        self.vf = vf_config.build(framework=self.framework)

    @abstractmethod
    def pi(self, batch: Dict[str, TensorType]) -> Dict[str, TensorType]:
        """Computes the policy outputs given a batch of data.

        Args:
            batch: The input batch to pass through the policy head.

        Returns:
            A dict mapping Column names to batches of policy outputs.
        """


class AutoregressiveActionsTorchRLM(TorchRLModule, AutoregressiveActionsRLM):
    @override(AutoregressiveActionsRLM)
    def pi(
        self, batch: Dict[str, TensorType], inference: bool = False
    ) -> Dict[str, TensorType]:
        pi_outs = {}

        # Prior forward pass.
        prior_out = self.prior(batch)
        prior_logits = torch.cat(
            [
                prior_out,
                # We add zeros for the posterior logits, which we do not have at
                # this point of time.
                torch.zeros(size=(prior_out.shape[0], self.required_output_dims[1])),
            ],
            dim=-1,
        )
        # Get the prior action distribution to sample the prior action.
        if inference:
            # If in inference mode, we need to set the distribution to be deterministic.
            prior_action_dist = self.action_dist_cls.from_logits(
                prior_logits
            ).to_deterministic()
        else:
            # If in exploration mode, we draw a stochastic sample.
            prior_action_dist = self.action_dist_cls.from_logits(prior_logits)

        # Sample the action and reshape.
        prior_action = (
            prior_action_dist._flat_child_distributions[0]
            .sample()
            .view(*batch.shape[:-1], 1)
        )

        # Posterior forward pass.
        posterior_batch = torch.cat([batch, prior_action], dim=-1)
        posterior_out = self.posterior(posterior_batch)
        # Concatenate the prior and posterior logits to get the final logits.
        posterior_logits = torch.cat([prior_out, posterior_out], dim=-1)
        if inference:
            posterior_action_dist = self.action_dist_cls.from_logits(
                posterior_logits
            ).to_deterministic()
            # Sample the posterior action.
            posterior_action = posterior_action_dist._flat_child_distributions[
                1
            ].sample()

        else:
            # Get the posterior action distribution to sample the posterior action.
            posterior_action_dist = self.action_dist_cls.from_logits(posterior_logits)
            # Sample the posterior action.
            posterior_action = posterior_action_dist._flat_child_distributions[
                1
            ].sample()
            # We need the log-probabilities for the loss.
            pi_outs[Columns.ACTION_LOGP] = posterior_action_dist.logp(
                (prior_action, posterior_action)
            )
            # We also need the input to the action distribution to calculate the
            # KL-divergence.
            pi_outs[Columns.ACTION_DIST_INPUTS] = posterior_logits

        # Concatenate the prior and posterior actions and log probabilities.
        pi_outs[Columns.ACTIONS] = (prior_action, posterior_action)

        return pi_outs

    @override(TorchRLModule)
    def _forward_inference(self, batch: Dict[str, TensorType]) -> Dict[str, TensorType]:
        # Encoder forward pass.
        encoder_out = self.encoder(batch)
        # Policy head forward pass.
        return self.pi(encoder_out[ENCODER_OUT], inference=True)

    @override(TorchRLModule)
    def _forward_exploration(
        self, batch: Dict[str, TensorType], **kwargs
    ) -> Dict[str, TensorType]:
        # Encoder forward pass.
        encoder_out = self.encoder(batch)
        # Policy head forward pass.
        return self.pi(encoder_out[ENCODER_OUT], inference=False)

    @override(TorchRLModule)
    def _forward_train(self, batch: Dict[str, TensorType]) -> Dict[str, TensorType]:
        return self._forward_exploration(batch)

    @override(ValueFunctionAPI)
    def compute_values(self, batch: Dict[str, TensorType], embeddings=None):
        # Encoder forward pass to get `embeddings`, if necessary.
        if embeddings is None:
            embeddings = self.encoder(batch)[ENCODER_OUT]
        # Value head forward pass.
        vf_out = self.vf(embeddings)
        # Squeeze out last dimension (single node value head).
        return vf_out.squeeze(-1)
