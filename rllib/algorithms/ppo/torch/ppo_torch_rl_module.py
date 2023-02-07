from dataclasses import dataclass
from typing import Mapping, Any, Union

import gymnasium as gym

from ray.rllib.core.rl_module.rl_module import RLModule, RLModuleConfig
from ray.rllib.core.rl_module.torch import TorchRLModule
from ray.rllib.models.experimental.encoder import STATE_OUT, ACTOR, CRITIC, ENCODER_OUT
from ray.rllib.models.experimental.configs import (
    MLPModelConfig,
    ActorCriticEncoderConfig,
)
from ray.rllib.models.experimental.catalog import Catalog
from ray.rllib.models.specs.specs_dict import SpecDict
from ray.rllib.models.specs.specs_torch import TorchTensorSpec
from ray.rllib.models.torch.torch_distributions import (
    TorchCategorical,
    TorchDeterministic,
    TorchDiagGaussian,
)
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.utils.annotations import override, ExperimentalAPI
from ray.rllib.utils.framework import try_import_torch
from ray.rllib.utils.gym import convert_old_gym_space_to_gymnasium_space
from ray.rllib.utils.nested_dict import NestedDict


torch, nn = try_import_torch()


def get_ppo_loss(fwd_in, fwd_out):
    # TODO: we should replace these components later with real ppo components when
    # RLOptimizer and RLModule are integrated together.
    # this is not exactly a ppo loss, just something to show that the
    # forward train works
    adv = fwd_in[SampleBatch.REWARDS] - fwd_out[SampleBatch.VF_PREDS]
    actor_loss = -(fwd_out[SampleBatch.ACTION_LOGP] * adv).mean()
    critic_loss = (adv**2).mean()
    loss = actor_loss + critic_loss

    return loss


class PPOCatalog(Catalog):
    def __init__(self, observation_space, action_space, model_config):
        super().__init__(observation_space, action_space, model_config)
        latent_dim = self.encoder_config.output_dim

        # Replace EncoderConfig by ActorCriticEncoderConfig
        self.actor_critic_encoder_config = ActorCriticEncoderConfig(
            output_dim=latent_dim,
            base_encoder_config=self.encoder_config,
            shared=self.model_config["vf_share_layers"],
        )

        if isinstance(action_space, gym.spaces.Discrete):
            pi_output_dim = action_space.n
        else:
            pi_output_dim = action_space.shape[0] * 2

        self.pi_head_config = MLPModelConfig(
            input_dim=latent_dim, hidden_layer_dims=[32], output_dim=pi_output_dim
        )
        self.vf_head_config = MLPModelConfig(
            input_dim=latent_dim, hidden_layer_dims=[32], output_dim=1
        )

    def build_actor_critic_encoder(self, framework="torch"):
        return self.actor_critic_encoder_config.build(framework=framework)

    @override(Catalog)
    def build_encoder(self, framework="torch"):
        raise NotImplementedError("Use PPOCatalog.build_actor_critic_encoder() instead")

    def build_pi_head(self, framework="torch"):
        return self.pi_head_config.build(framework=framework)

    def build_vf_head(self, framework="torch"):
        return self.vf_head_config.build(framework=framework)


# TODO (Artur): Move to Torch-unspecific file
@ExperimentalAPI
@dataclass
class PPOModuleConfig(RLModuleConfig):  # TODO (Artur): Move to non-torch-specific file
    """Configuration for the PPORLModule.

    Attributes:
        observation_space: The observation space of the environment.
        action_space: The action space of the environment.
        catalog: The PPOCatalog object to use for building the models.
        free_log_std: For DiagGaussian action distributions, make the second half of
            the model outputs floating bias variables instead of state-dependent. This
            only has an effect is using the default fully connected net.
    """

    observation_space: gym.Space = None
    action_space: gym.Space = None
    catalog: Catalog = None
    free_log_std: bool = False

    def build(self, framework="torch"):
        if framework == "torch":
            return PPOTorchRLModule(self)
        else:
            from ray.rllib.algorithms.ppo.ppo_tf_rl_module import PPOTfRLModule

            return PPOTfRLModule(self)


class PPOTorchRLModule(TorchRLModule):
    def __init__(self, config: PPOModuleConfig) -> None:
        super().__init__()
        assert type(config) is PPOModuleConfig
        self.config = config

        self.encoder = self.config.catalog.build_actor_critic_encoder(framework="torch")
        self.pi = self.config.catalog.build_pi_head(framework="torch")
        self.vf = self.config.catalog.build_vf_head(framework="torch")

        self._is_discrete = isinstance(
            convert_old_gym_space_to_gymnasium_space(self.config.action_space),
            gym.spaces.Discrete,
        )

    @classmethod
    @override(RLModule)
    def from_model_config(
        cls,
        observation_space: gym.Space,
        action_space: gym.Space,
        *,
        model_config: Mapping[str, Any],
    ) -> Union["RLModule", Mapping[str, Any]]:
        free_log_std = model_config["free_log_std"]
        assert not free_log_std, "free_log_std not supported yet."

        assert isinstance(
            observation_space, gym.spaces.Box
        ), "This simple PPOModule only supports Box observation space."

        assert (
            len(observation_space.shape) == 1
        ), "This simple PPOModule only supports 1D observation space."

        assert isinstance(action_space, (gym.spaces.Discrete, gym.spaces.Box)), (
            "This simple PPOModule only supports Discrete and Box action space.",
        )
        catalog = PPOCatalog(
            observation_space=observation_space,
            action_space=action_space,
            model_config=model_config,
        )

        config = PPOModuleConfig(
            observation_space=observation_space,
            action_space=action_space,
            catalog=catalog,
            free_log_std=free_log_std,
        )

        return config.build(framework="torch")

    def get_initial_state(self) -> NestedDict:
        if hasattr(self.encoder, "get_initial_state"):
            return self.encoder.get_initial_state()
        else:
            return NestedDict({})

    @override(RLModule)
    def input_specs_inference(self) -> SpecDict:
        return self.input_specs_exploration()

    @override(RLModule)
    def output_specs_inference(self) -> SpecDict:
        return SpecDict({SampleBatch.ACTION_DIST: TorchDeterministic})

    @override(RLModule)
    def _forward_inference(self, batch: NestedDict) -> Mapping[str, Any]:
        output = {}

        encoder_outs = self.encoder(batch)
        output[STATE_OUT] = encoder_outs[STATE_OUT]

        # Actions
        action_logits = self.pi(encoder_outs[ENCODER_OUT][ACTOR])
        if self._is_discrete:
            action = torch.argmax(action_logits, dim=-1)
        else:
            action, _ = action_logits.chunk(2, dim=-1)
        action_dist = TorchDeterministic(action)
        output[SampleBatch.ACTION_DIST] = action_dist

        return output

    @override(RLModule)
    def input_specs_exploration(self):
        return SpecDict(self.encoder.input_spec)

    @override(RLModule)
    def output_specs_exploration(self) -> SpecDict:
        if self._is_discrete:
            action_dist_inputs = ((SampleBatch.ACTION_DIST_INPUTS, "logits"),)
        else:
            action_dist_inputs = (
                (SampleBatch.ACTION_DIST_INPUTS, "loc"),
                (SampleBatch.ACTION_DIST_INPUTS, "scale"),
            )
        return [
            SampleBatch.VF_PREDS,
            SampleBatch.ACTION_DIST,
            *action_dist_inputs,
        ]

    @override(RLModule)
    def _forward_exploration(self, batch: NestedDict) -> Mapping[str, Any]:
        """PPO forward pass during exploration.

        Besides the action distribution, this method also returns the parameters of the
        policy distribution to be used for computing KL divergence between the old
        policy and the new policy during training.
        """
        output = {}

        # Shared encoder
        encoder_outs = self.encoder(batch)
        output[STATE_OUT] = encoder_outs[STATE_OUT]

        # Value head
        vf_out = self.vf(encoder_outs[ENCODER_OUT][CRITIC])
        output[SampleBatch.VF_PREDS] = vf_out.squeeze(-1)

        # Policy head
        pi_out = self.pi(encoder_outs[ENCODER_OUT][ACTOR])
        action_logits = pi_out
        if self._is_discrete:
            action_dist = TorchCategorical(logits=action_logits)
            output[SampleBatch.ACTION_DIST_INPUTS] = {"logits": action_logits}
        else:
            loc, log_std = action_logits.chunk(2, dim=-1)
            scale = log_std.exp()
            action_dist = TorchDiagGaussian(loc, scale)
            output[SampleBatch.ACTION_DIST_INPUTS] = {"loc": loc, "scale": scale}
        output[SampleBatch.ACTION_DIST] = action_dist

        return output

    @override(RLModule)
    def input_specs_train(self) -> SpecDict:
        if self._is_discrete:
            action_spec = TorchTensorSpec("b")
        else:
            action_dim = self.config.action_space.shape[0]
            action_spec = TorchTensorSpec("b, h", h=action_dim)

        # Convert to SpecDict if needed.
        spec_dict = SpecDict(self.encoder.input_spec)
        spec_dict.update({SampleBatch.ACTIONS: action_spec})
        if SampleBatch.OBS in spec_dict:
            spec_dict[SampleBatch.NEXT_OBS] = spec_dict[SampleBatch.OBS]
        spec = SpecDict(spec_dict)
        return spec

    @override(RLModule)
    def output_specs_train(self) -> SpecDict:
        spec = SpecDict(
            {
                SampleBatch.ACTION_DIST: TorchCategorical
                if self._is_discrete
                else TorchDiagGaussian,
                SampleBatch.ACTION_LOGP: TorchTensorSpec("b", dtype=torch.float32),
                SampleBatch.VF_PREDS: TorchTensorSpec("b", dtype=torch.float32),
                "entropy": TorchTensorSpec("b", dtype=torch.float32),
            }
        )
        return spec

    @override(RLModule)
    def _forward_train(self, batch: NestedDict) -> Mapping[str, Any]:
        output = {}

        # Shared encoder
        encoder_outs = self.encoder(batch)
        output[STATE_OUT] = encoder_outs[STATE_OUT]

        # Value head
        vf_out = self.vf(encoder_outs[ENCODER_OUT][CRITIC])
        output[SampleBatch.VF_PREDS] = vf_out.squeeze(-1)

        # Policy head
        pi_out = self.pi(encoder_outs[ENCODER_OUT][ACTOR])
        action_logits = pi_out
        if self._is_discrete:
            action_dist = TorchCategorical(logits=action_logits)
        else:
            mu, scale = action_logits.chunk(2, dim=-1)
            action_dist = TorchDiagGaussian(mu, scale.exp())
        logp = action_dist.logp(batch[SampleBatch.ACTIONS])
        entropy = action_dist.entropy()
        output[SampleBatch.ACTION_DIST] = action_dist
        output[SampleBatch.ACTION_LOGP] = logp
        output["entropy"] = entropy

        return output
