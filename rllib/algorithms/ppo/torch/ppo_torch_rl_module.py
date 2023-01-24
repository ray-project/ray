from typing import Mapping, Any, Union

import gymnasium as gym

from ray.rllib.core.rl_module.rl_module import RLModule
from ray.rllib.core.rl_module.torch import TorchRLModule
from ray.rllib.models.experimental.base import STATE_OUT
from ray.rllib.models.experimental.model_configs import FCConfig, FCEncoderConfig
from ray.rllib.models.experimental.model_configs import (
    LSTMEncoderConfig,
)
from ray.rllib.models.experimental.torch.encoder import (
    ENCODER_OUT,
)
from ray.rllib.models.specs.specs_dict import SpecDict
from ray.rllib.models.specs.specs_torch import TorchTensorSpec
from ray.rllib.models.torch.torch_distributions import (
    TorchCategorical,
    TorchDeterministic,
    TorchDiagGaussian,
)
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.utils.annotations import override
from ray.rllib.utils.framework import try_import_torch
from ray.rllib.utils.gym import convert_old_gym_space_to_gymnasium_space
from ray.rllib.utils.nested_dict import NestedDict
from rllib.algorithms.ppo.ppo import __PPOModuleConfig

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


class PPOTorchRLModule(TorchRLModule):
    def __init__(self, config: __PPOModuleConfig) -> None:
        super().__init__()
        self.config = config
        self.setup()

    def setup(self) -> None:
        assert self.config.pi_config, "pi_config must be provided."
        assert self.config.vf_config, "vf_config must be provided."
        assert self.config.shared_encoder_config, (
            "shared encoder config must be " "provided."
        )

        # TODO(Artur): Unify to tf and torch setup(framework)
        self.shared_encoder = self.config.shared_encoder_config.build(framework="torch")
        self.pi = self.config.pi_config.build(framework="torch")
        self.vf = self.config.vf_config.build(framework="torch")

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

        # TODO: use the new catalog to perform this logic and construct the final config

        activation = model_config["fcnet_activation"]
        if activation == "tanh":
            activation = "Tanh"
        elif activation == "relu":
            activation = "ReLU"
        elif activation == "linear":
            activation = "linear"
        else:
            raise ValueError(f"Unsupported activation: {activation}")

        obs_dim = observation_space.shape[0]
        fcnet_hiddens = model_config["fcnet_hiddens"]
        free_log_std = model_config["free_log_std"]
        assert (
            model_config.get("vf_share_layers") is False
        ), "`vf_share_layers=False` is no longer supported."

        if model_config["use_lstm"]:
            shared_encoder_config = LSTMEncoderConfig(
                input_dim=obs_dim,
                hidden_dim=model_config["lstm_cell_size"],
                batch_first=not model_config["_time_major"],
                num_layers=1,
                output_dim=model_config["lstm_cell_size"],
            )
        else:
            shared_encoder_config = FCEncoderConfig(
                input_dim=obs_dim,
                hidden_layers=fcnet_hiddens[:-1],
                activation=activation,
                output_dim=fcnet_hiddens[-1],
            )

        pi_config = FCConfig(
            input_dim=shared_encoder_config.output_dim,
            hidden_layers=[32],
            activation="ReLU",
        )
        vf_config = FCConfig(
            input_dim=shared_encoder_config.output_dim,
            hidden_layers=[32, 1],
            activation="ReLU",
            output_dim=1,
        )

        assert isinstance(
            observation_space, gym.spaces.Box
        ), "This simple PPOModule only supports Box observation space."

        assert (
            len(observation_space.shape) == 1
        ), "This simple PPOModule only supports 1D observation space."

        assert isinstance(action_space, (gym.spaces.Discrete, gym.spaces.Box)), (
            "This simple PPOModule only supports Discrete and Box action space.",
        )

        # build policy network head
        shared_encoder_config.input_dim = observation_space.shape[0]
        pi_config.input_dim = shared_encoder_config.output_dim
        if isinstance(action_space, gym.spaces.Discrete):
            pi_config.output_dim = action_space.n
        else:
            pi_config.output_dim = action_space.shape[0] * 2

        config_ = __PPOModuleConfig(
            observation_space=observation_space,
            action_space=action_space,
            shared_encoder_config=shared_encoder_config,
            pi_config=pi_config,
            vf_config=vf_config,
            free_log_std=free_log_std,
        )

        module = PPOTorchRLModule(config_)
        return module

    def get_initial_state(self) -> NestedDict:
        if hasattr(self.shared_encoder, "get_initial_state"):
            return self.shared_encoder.get_initial_state()
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

        encoder_out = self.shared_encoder(batch)
        if STATE_OUT in encoder_out:
            output[STATE_OUT] = encoder_out[STATE_OUT]

        # Actions
        action_logits = self.pi(encoder_out[ENCODER_OUT])
        if self._is_discrete:
            action = torch.argmax(action_logits, dim=-1)
        else:
            action, _ = action_logits.chunk(2, dim=-1)
        action_dist = TorchDeterministic(action)
        output[SampleBatch.ACTION_DIST] = action_dist

        return output

    @override(RLModule)
    def input_specs_exploration(self):
        return self.shared_encoder.input_spec

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
        encoder_out = self.shared_encoder(batch)
        if STATE_OUT in encoder_out:
            output[STATE_OUT] = encoder_out[STATE_OUT]

        # Value head
        vf_out = self.vf(encoder_out[ENCODER_OUT])
        output[SampleBatch.VF_PREDS] = vf_out.squeeze(-1)

        # Policy head
        pi_out = self.pi(encoder_out[ENCODER_OUT])
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

        spec_dict = self.shared_encoder.input_spec
        spec_dict.update({SampleBatch.ACTIONS: action_spec})
        if SampleBatch.OBS in spec_dict:
            spec_dict[SampleBatch.NEXT_OBS] = spec_dict[SampleBatch.OBS]
        spec = SpecDict(spec_dict)
        return spec

    @override(RLModule)
    def output_specs_train(self) -> SpecDict:
        spec = SpecDict(
            {
                SampleBatch.ACTION_DIST: self.__get_action_dist_type(),
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
        encoder_out = self.shared_encoder(batch)
        if STATE_OUT in encoder_out:
            output[STATE_OUT] = encoder_out[STATE_OUT]

        # Value head
        vf_out = self.vf(encoder_out[ENCODER_OUT])
        output[SampleBatch.VF_PREDS] = vf_out.squeeze(-1)

        # Policy head
        pi_out = self.pi(encoder_out[ENCODER_OUT])
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

    def __get_action_dist_type(self):
        return TorchCategorical if self._is_discrete else TorchDiagGaussian
