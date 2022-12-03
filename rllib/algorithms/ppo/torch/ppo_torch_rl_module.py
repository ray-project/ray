from dataclasses import dataclass, field
import gym
from typing import List, Mapping, Any

from ray.rllib.core.rl_module.torch import TorchRLModule
from ray.rllib.core.rl_module.rl_module import RLModule
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.utils.annotations import override
from ray.rllib.utils.nested_dict import NestedDict
from ray.rllib.utils.framework import try_import_torch
from ray.rllib.models.specs.specs_dict import ModelSpec, check_specs
from ray.rllib.models.specs.specs_torch import TorchTensorSpec
from ray.rllib.models.torch.torch_distributions import (
    TorchCategorical,
    TorchDeterministic,
    TorchDiagGaussian,
)


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


# TODO: Most of the neural network, and model specs in this file will eventually be
# retreived from the model catalog. That includes FCNet, Encoder, etc.
def get_shared_encoder_config(env):
    return PPOModuleConfig(
        observation_space=env.observation_space,
        action_space=env.action_space,
        encoder_config=FCConfig(
            hidden_layers=[32],
            activation="ReLU",
        ),
        pi_config=FCConfig(
            hidden_layers=[32],
            activation="ReLU",
        ),
        vf_config=FCConfig(
            hidden_layers=[32],
            activation="ReLU",
        ),
    )


def get_separate_encoder_config(env):
    return PPOModuleConfig(
        observation_space=env.observation_space,
        action_space=env.action_space,
        pi_config=FCConfig(
            hidden_layers=[32],
            activation="ReLU",
        ),
        vf_config=FCConfig(
            hidden_layers=[32],
            activation="ReLU",
        ),
    )


@dataclass
class FCConfig:
    """Configuration for a fully connected network.

    Attributes:
        input_dim: The input dimension of the network. It cannot be None.
        output_dim: The output dimension of the network. if None, the last layer would
            be the last hidden layer.
        hidden_layers: The sizes of the hidden layers.
        activation: The activation function to use after each layer (except for the
            output).
        output_activation: The activation function to use for the output layer.
    """

    input_dim: int = None
    output_dim: int = None
    hidden_layers: List[int] = field(default_factory=lambda: [256, 256])
    activation: str = "ReLU"
    output_activation: str = None


@dataclass
class PPOModuleConfig:
    """Configuration for the PPO module.

    Attributes:
        observation_space: The observation space of the environment.
        action_space: The action space of the environment.
        pi_config: The configuration for the policy network.
        vf_config: The configuration for the value network.
        encoder_config: The configuration for the encoder network.
        free_log_std: For DiagGaussian action distributions, make the second half of
            the model outputs floating bias variables instead of state-dependent. This
            only has an effect is using the default fully connected net.
    """

    observation_space: gym.Space = None
    action_space: gym.Space = None
    pi_config: FCConfig = None
    vf_config: FCConfig = None
    encoder_config: FCConfig = None
    free_log_std: bool = False


class FCNet(nn.Module):
    """A simple fully connected network.

    Attributes:
        input_dim: The input dimension of the network. It cannot be None.
        output_dim: The output dimension of the network. if None, the last layer would
            be the last hidden layer.
        hidden_layers: The sizes of the hidden layers.
        activation: The activation function to use after each layer.
    """

    def __init__(self, config: FCConfig):
        super().__init__()
        self.input_dim = config.input_dim
        self.hidden_layers = config.hidden_layers

        activation_class = getattr(nn, config.activation, lambda: None)()
        self.layers = []
        self.layers.append(nn.Linear(self.input_dim, self.hidden_layers[0]))
        for i in range(len(self.hidden_layers) - 1):
            if config.activation != "linear":
                self.layers.append(activation_class)
            self.layers.append(
                nn.Linear(self.hidden_layers[i], self.hidden_layers[i + 1])
            )

        if config.output_dim is not None:
            if config.activation != "linear":
                self.layers.append(activation_class)
            self.layers.append(nn.Linear(self.hidden_layers[-1], config.output_dim))

        if config.output_dim is None:
            self.output_dim = config.hidden_layers[-1]
        else:
            self.output_dim = config.output_dim

        self.layers = nn.Sequential(*self.layers)

        self._input_specs = self.input_specs()

    def input_specs(self):
        return TorchTensorSpec("b, h", h=self.input_dim)

    @check_specs(input_spec="_input_specs")
    def forward(self, x):
        return self.layers(x)


class PPOTorchRLModule(TorchRLModule):
    def __init__(self, config: PPOModuleConfig) -> None:
        super().__init__(config)

    def setup(self) -> None:
        assert isinstance(
            self.config.observation_space, gym.spaces.Box
        ), "This simple PPOModule only supports Box observation space."

        assert (
            len(self.config.observation_space.shape) == 1
        ), "This simple PPOModule only supports 1D observation space."

        assert isinstance(
            self.config.action_space, (gym.spaces.Discrete, gym.spaces.Box)
        ), ("This simple PPOModule only supports Discrete and Box action space.",)

        assert self.config.pi_config, "pi_config must be provided."
        assert self.config.vf_config, "vf_config must be provided."

        self.encoder = None
        if self.config.encoder_config:
            encoder_config = self.config.encoder_config
            encoder_config.input_dim = self.config.observation_space.shape[0]
            self.encoder = FCNet(self.config.encoder_config)

        # build pi network
        pi_config = self.config.pi_config
        if not self.encoder:
            pi_config.input_dim = self.config.observation_space.shape[0]
        else:
            pi_config.input_dim = self.encoder.output_dim

        if isinstance(self.config.action_space, gym.spaces.Discrete):
            pi_config.output_dim = self.config.action_space.n
        else:
            pi_config.output_dim = self.config.action_space.shape[0] * 2
        self.pi = FCNet(pi_config)

        # build vf network
        vf_config = self.config.vf_config
        if not self.encoder:
            vf_config.input_dim = self.config.observation_space.shape[0]
        else:
            vf_config.input_dim = self.encoder.output_dim

        vf_config.output_dim = 1
        self.vf = FCNet(vf_config)

        self._is_discrete = isinstance(self.config.action_space, gym.spaces.Discrete)

    @override(RLModule)
    def input_specs_inference(self) -> ModelSpec:
        return self.input_specs_exploration()

    @override(RLModule)
    def output_specs_inference(self) -> ModelSpec:
        return ModelSpec({SampleBatch.ACTION_DIST: TorchDeterministic})

    @override(RLModule)
    def _forward_inference(self, batch: NestedDict) -> Mapping[str, Any]:
        encoded_state = batch[SampleBatch.OBS]
        if self.encoder:
            encoded_state = self.encoder(encoded_state)
        action_logits = self.pi(encoded_state)

        if self._is_discrete:
            action = torch.argmax(action_logits, dim=-1)
        else:
            action, _ = action_logits.chunk(2, dim=-1)

        action_dist = TorchDeterministic(action)
        return {SampleBatch.ACTION_DIST: action_dist}

    @override(RLModule)
    def input_specs_exploration(self):
        return ModelSpec(
            {
                SampleBatch.OBS: (
                    self.encoder.input_specs()
                    if self.encoder
                    else self.pi.input_specs()
                )
            }
        )

    @override(RLModule)
    def output_specs_exploration(self) -> ModelSpec:
        specs = {SampleBatch.ACTION_DIST: self.__get_action_dist_type()}
        if self._is_discrete:
            specs[SampleBatch.ACTION_DIST_INPUTS] = {
                "logits": TorchTensorSpec("b, h", h=self.config.action_space.n)
            }
        else:
            specs[SampleBatch.ACTION_DIST_INPUTS] = {
                "loc": TorchTensorSpec("b, h", h=self.config.action_space.shape[0]),
                "scale": TorchTensorSpec("b, h", h=self.config.action_space.shape[0]),
            }

        return ModelSpec(specs)

    @override(RLModule)
    def _forward_exploration(self, batch: NestedDict) -> Mapping[str, Any]:
        """PPO forward pass during exploration.

        Besides the action distribution, this method also returns the parameters of the
        policy distribution to be used for computing KL divergence between the old
        policy and the new policy during training.
        """
        encoded_state = batch[SampleBatch.OBS]
        if self.encoder:
            encoded_state = self.encoder(encoded_state)
        action_logits = self.pi(encoded_state)

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
        output[SampleBatch.VF_PREDS] = self.vf(encoded_state).squeeze(-1)
        return output

    @override(RLModule)
    def input_specs_train(self) -> ModelSpec:
        if self._is_discrete:
            action_spec = TorchTensorSpec("b")
        else:
            action_dim = self.config.action_space.shape[0]
            action_spec = TorchTensorSpec("b, h", h=action_dim)

        obs_specs = (
            self.encoder.input_specs() if self.encoder else self.pi.input_specs()
        )
        spec = ModelSpec(
            {
                SampleBatch.OBS: obs_specs,
                SampleBatch.NEXT_OBS: obs_specs,
                SampleBatch.ACTIONS: action_spec,
            }
        )

        return spec

    @override(RLModule)
    def output_specs_train(self) -> ModelSpec:
        spec = ModelSpec(
            {
                SampleBatch.ACTION_DIST: self.__get_action_dist_type(),
                SampleBatch.ACTION_LOGP: TorchTensorSpec("b", dtype=torch.float32),
                SampleBatch.VF_PREDS: TorchTensorSpec("b", dtype=torch.float32),
                "entropy": TorchTensorSpec("b", dtype=torch.float32),
                "vf_preds_next_obs": TorchTensorSpec("b", dtype=torch.float32),
            }
        )
        return spec

    @override(RLModule)
    def _forward_train(self, batch: NestedDict) -> Mapping[str, Any]:
        encoded_state = batch[SampleBatch.OBS]
        if self.encoder:
            encoded_state = self.encoder(encoded_state)

        action_logits = self.pi(encoded_state)
        vf = self.vf(encoded_state)

        if self._is_discrete:
            action_dist = TorchCategorical(logits=action_logits)
        else:
            mu, scale = action_logits.chunk(2, dim=-1)
            action_dist = TorchDiagGaussian(mu, scale.exp())

        logp = action_dist.logp(batch[SampleBatch.ACTIONS])
        entropy = action_dist.entropy()

        # get vf of the next obs
        encoded_next_state = batch[SampleBatch.NEXT_OBS]
        if self.encoder:
            encoded_next_state = self.encoder(encoded_next_state)
        vf_next_obs = self.vf(encoded_next_state)

        output = {
            SampleBatch.ACTION_DIST: action_dist,
            SampleBatch.ACTION_LOGP: logp,
            SampleBatch.VF_PREDS: vf.squeeze(-1),
            "entropy": entropy,
            "vf_preds_next_obs": vf_next_obs.squeeze(-1),
        }
        return output

    def __get_action_dist_type(self):
        return TorchCategorical if self._is_discrete else TorchDiagGaussian
