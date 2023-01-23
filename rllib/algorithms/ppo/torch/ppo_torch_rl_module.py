from dataclasses import dataclass
import gymnasium as gym
from typing import Mapping, Any, Union

from ray.rllib.core.rl_module.torch import TorchRLModule
from ray.rllib.core.rl_module.rl_module import RLModule, RLModuleConfig
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.utils.annotations import override
from ray.rllib.utils.nested_dict import NestedDict
from ray.rllib.utils.framework import try_import_torch
from ray.rllib.models.specs.specs_dict import SpecDict
from ray.rllib.models.specs.specs_torch import TorchTensorSpec
from ray.rllib.models.torch.torch_distributions import (
    TorchCategorical,
    TorchDeterministic,
    TorchDiagGaussian,
)
from ray.rllib.core.rl_module.encoder import (
    FCNet,
    FCConfig,
    LSTMConfig,
    IdentityConfig,
    LSTMEncoder,
    ENCODER_OUT,
)
from ray.rllib.utils.gym import convert_old_gym_space_to_gymnasium_space


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


@dataclass
class PPOModuleConfig(RLModuleConfig):
    """Configuration for the PPO module.

    Attributes:
        pi_config: The configuration for the policy network.
        vf_config: The configuration for the value network.
        shared_encoder_config: The configuration for the encoder network.
        free_log_std: For DiagGaussian action distributions, make the second half of
            the model outputs floating bias variables instead of state-dependent. This
            only has an effect is using the default fully connected net.
        shared_encoder: Whether to share the encoder between the pi and value
    """

    pi_config: FCConfig = None
    vf_config: FCConfig = None
    pi_encoder_config: FCConfig = None
    vf_encoder_config: FCConfig = None
    shared_encoder_config: FCConfig = None
    free_log_std: bool = False
    shared_encoder: bool = True


class PPOTorchRLModule(TorchRLModule):
    def __init__(self, config: PPOModuleConfig) -> None:
        super().__init__()
        self.config = config
        self.setup()

    def setup(self) -> None:

        assert self.config.pi_config, "pi_config must be provided."
        assert self.config.vf_config, "vf_config must be provided."

        self.shared_encoder = self.config.shared_encoder_config.build()
        self.pi_encoder = self.config.pi_encoder_config.build()
        self.vf_encoder = self.config.vf_encoder_config.build()

        self.pi = FCNet(
            input_dim=self.config.pi_encoder_config.output_dim,
            output_dim=self.config.pi_config.output_dim,
            hidden_layers=self.config.pi_config.hidden_layers,
            activation=self.config.pi_config.activation,
        )

        self.vf = FCNet(
            input_dim=self.config.vf_encoder_config.output_dim,
            output_dim=1,
            hidden_layers=self.config.vf_config.hidden_layers,
            activation=self.config.vf_config.activation,
        )

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
        vf_share_layers = model_config["vf_share_layers"]
        free_log_std = model_config["free_log_std"]
        use_lstm = model_config["use_lstm"]

        if vf_share_layers:
            shared_encoder_config = FCConfig(
                input_dim=obs_dim,
                hidden_layers=fcnet_hiddens,
                activation=activation,
                output_dim=model_config["fcnet_hiddens"][-1],
            )
        else:
            shared_encoder_config = IdentityConfig(output_dim=obs_dim)

        if use_lstm:
            pi_encoder_config = LSTMConfig(
                input_dim=shared_encoder_config.output_dim,
                hidden_dim=model_config["lstm_cell_size"],
                batch_first=not model_config["_time_major"],
                output_dim=model_config["lstm_cell_size"],
                num_layers=1,
            )
        else:
            pi_encoder_config = FCConfig(
                input_dim=shared_encoder_config.output_dim,
                hidden_layers=fcnet_hiddens,
                activation=activation,
                output_dim=model_config["fcnet_hiddens"][-1],
            )

        vf_encoder_config = FCConfig(
            input_dim=shared_encoder_config.output_dim,
            hidden_layers=fcnet_hiddens,
            activation=activation,
            output_dim=model_config["fcnet_hiddens"][-1],
        )
        pi_config = FCConfig()
        vf_config = FCConfig()

        assert isinstance(
            observation_space, gym.spaces.Box
        ), "This simple PPOModule only supports Box observation space."

        assert (
            len(observation_space.shape) == 1
        ), "This simple PPOModule only supports 1D observation space."

        assert isinstance(action_space, (gym.spaces.Discrete, gym.spaces.Box)), (
            "This simple PPOModule only supports Discrete and Box action space.",
        )

        # build pi network
        shared_encoder_config.input_dim = observation_space.shape[0]
        pi_encoder_config.input_dim = shared_encoder_config.output_dim
        pi_config.input_dim = pi_encoder_config.output_dim
        if isinstance(action_space, gym.spaces.Discrete):
            pi_config.output_dim = action_space.n
        else:
            pi_config.output_dim = action_space.shape[0] * 2

        # build vf network
        vf_encoder_config.input_dim = shared_encoder_config.output_dim
        vf_config.input_dim = vf_encoder_config.output_dim
        vf_config.output_dim = 1

        config_ = PPOModuleConfig(
            observation_space=observation_space,
            action_space=action_space,
            max_seq_len=model_config["max_seq_len"],
            shared_encoder_config=shared_encoder_config,
            pi_config=pi_config,
            vf_config=vf_config,
            pi_encoder_config=pi_encoder_config,
            vf_encoder_config=vf_encoder_config,
            free_log_std=free_log_std,
            shared_encoder=vf_share_layers,
        )

        module = PPOTorchRLModule(config_)
        return module

    def get_initial_state(self) -> NestedDict:
        if isinstance(self.shared_encoder, LSTMEncoder):
            return self.shared_encoder.get_initial_state()
        elif isinstance(self.pi_encoder, LSTMEncoder):
            return self.pi_encoder.get_initial_state()
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
        shared_enc_out = self.shared_encoder(batch)
        pi_enc_out = self.pi_encoder(shared_enc_out)

        action_logits = self.pi(pi_enc_out[ENCODER_OUT])

        if self._is_discrete:
            action = torch.argmax(action_logits, dim=-1)
        else:
            action, _ = action_logits.chunk(2, dim=-1)

        action_dist = TorchDeterministic(action)
        output = {SampleBatch.ACTION_DIST: action_dist}
        output["state_out"] = pi_enc_out.get("state_out", {})
        return output

    @override(RLModule)
    def input_specs_exploration(self):
        return self.shared_encoder.input_spec()

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
        encoder_out = self.shared_encoder(batch)
        encoder_out_pi = self.pi_encoder(encoder_out)
        encoder_out_vf = self.vf_encoder(encoder_out)
        action_logits = self.pi(encoder_out_pi[ENCODER_OUT])

        output = {}
        if self._is_discrete:
            action_dist = TorchCategorical(logits=action_logits)
            output[SampleBatch.ACTION_DIST_INPUTS] = {"logits": action_logits}
        else:
            loc, log_std = action_logits.chunk(2, dim=-1)
            scale = log_std.exp()
            action_dist = TorchDiagGaussian(loc, scale)
            output[SampleBatch.ACTION_DIST_INPUTS] = {"loc": loc, "scale": scale}
        output[SampleBatch.ACTION_DIST] = action_dist

        # compute the value function
        output[SampleBatch.VF_PREDS] = self.vf(encoder_out_vf[ENCODER_OUT]).squeeze(-1)
        output["state_out"] = encoder_out_pi.get("state_out", {})
        return output

    @override(RLModule)
    def input_specs_train(self) -> SpecDict:
        if self._is_discrete:
            action_spec = TorchTensorSpec("b")
        else:
            action_dim = self.config.action_space.shape[0]
            action_spec = TorchTensorSpec("b, h", h=action_dim)

        spec_dict = self.shared_encoder.input_spec()
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
        encoder_out = self.shared_encoder(batch)
        encoder_out_pi = self.pi_encoder(encoder_out)
        encoder_out_vf = self.vf_encoder(encoder_out)

        action_logits = self.pi(encoder_out_pi[ENCODER_OUT])
        vf = self.vf(encoder_out_vf[ENCODER_OUT])

        if self._is_discrete:
            action_dist = TorchCategorical(logits=action_logits)
        else:
            mu, scale = action_logits.chunk(2, dim=-1)
            action_dist = TorchDiagGaussian(mu, scale.exp())

        logp = action_dist.logp(batch[SampleBatch.ACTIONS])
        entropy = action_dist.entropy()

        output = {
            SampleBatch.ACTION_DIST: action_dist,
            SampleBatch.ACTION_LOGP: logp,
            SampleBatch.VF_PREDS: vf.squeeze(-1),
            "entropy": entropy,
        }

        output["state_out"] = encoder_out_pi.get("state_out", {})
        return output

    def __get_action_dist_type(self):
        return TorchCategorical if self._is_discrete else TorchDiagGaussian
