from abc import abstractmethod
from typing import Any, Dict

from ray.rllib.core import Columns
from ray.rllib.core.models.base import ENCODER_OUT
from ray.rllib.core.models.configs import MLPHeadConfig
from ray.rllib.core.models.specs.specs_dict import SpecDict
from ray.rllib.core.rl_module.rl_module import RLModule
from ray.rllib.core.rl_module.torch.torch_rl_module import TorchRLModule
from ray.rllib.utils.annotations import (
    override,
    OverrideToImplementCustomLogic_CallToSuperRecommended,
)
from ray.rllib.utils.framework import try_import_torch
from ray.rllib.utils.torch_utils import convert_to_torch_tensor
from ray.rllib.utils.typing import TensorType

torch, nn = try_import_torch()


# TODO (simon): Improvements: `inference-only` mode.
class AutoregressiveActionsRLM(RLModule):
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
        self.encoder = self.config.get_catalog().build_encoder(framework=self.framework)

        # Build the prior and posterior heads.
        # Note further, we neet to know the required input dimensions for
        # the partial distributions.
        self.required_output_dims = self.action_dist_cls.required_input_dim(
            space=self.config.action_space,
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
        # Build the posterior head.
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
    def output_specs_inference(self) -> SpecDict:
        return [Columns.ACTIONS]

    @override(RLModule)
    def output_specs_exploration(self) -> SpecDict:
        return [Columns.ACTION_DIST_INPUTS, Columns.ACTIONS, Columns.ACTION_LOGP]

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
            # If in inference mode, we can sample in a simple way.
            prior_action = prior_action_dist._flat_child_distributions[0].sample()
        else:
            prior_action_dist = self.action_dist_cls.from_logits(prior_logits)
            # Note, `TorchMultiDistribution.from_logits` does set the `logits`, but not
            # the `probs` attribute. We need to set the `probs` attribute to be able to
            # sample from the distribution in a differentiable way.
            prior_action_dist._flat_child_distributions[0].probs = torch.softmax(
                prior_out, dim=-1
            )
            prior_action_dist._flat_child_distributions[0].logits = None
            # Otherwise, we need to be able to backpropagate through the prior action
            # that's why we sample from the distribution using the `rsample` method.
            # TODO (simon, sven): Check, if we need to return the one-hot sampled action
            # instead of the real-valued one.
            prior_action = torch.argmax(
                prior_action_dist._flat_child_distributions[0].rsample(),
                dim=-1,
            )

        # Posterior forward pass.
        posterior_batch = torch.cat([batch, prior_action.view(-1, 1)], dim=-1)
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

            # We need the log probabilities of the sampled actions for the loss
            # calculation.
            prior_action_logp = prior_action_dist._flat_child_distributions[0].logp(
                prior_action
            )
            posterior_action_logp = posterior_action_dist._flat_child_distributions[
                1
            ].logp(posterior_action)
            pi_outs[Columns.ACTION_LOGP] = prior_action_logp + posterior_action_logp
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

        outs = {}

        # Encoder forward pass.
        encoder_out = self.encoder(batch)

        # Policy head forward pass.
        outs.update(self.pi(encoder_out[ENCODER_OUT]))

        # Value function head forward pass.
        vf_out = self.vf(encoder_out[ENCODER_OUT])
        outs[Columns.VF_PREDS] = vf_out.squeeze(-1)

        return outs

    @override(AutoregressiveActionsRLM)
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
