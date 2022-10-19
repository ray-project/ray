from dataclasses import dataclass

from ray.rllib.core.rl_module.torch_rl_module import TorchRLModule
from ray.rllib.core.rl_module.rl_module import RLModule
from ray.rllib.utils.annotations import override
from ray.rllib.utils.nested_dict import NestedDict
from ray.rllib.models.specs.specs_dict import ModelSpecDict
from ray.rllib.models.specs.specs_torch import TorchSpecs

import torch
import torch.nn as nn
import gym

from typing import List, Mapping, Any

@dataclass
class FCConfig:
    """Configuration for a fully connected network.
    
    Attributes:
        input_dim: The input dimension of the network. It cannot be None.
        output_dim: The output dimension of the network. if None, the last layer would  
            be the last hidden layer.
        hidden_layers: The sizes of the hidden layers.
        activation: The activation function to use after each layer.
    """
    input_dim: int = None
    output_dim: int = None
    hidden_layers: List[int] = [256, 256]
    activation: str = "relu"

@dataclass
class PPOModuleConfig:
    """Configuration for the PPO module.
    
    Attributes:
        observation_space: The observation space of the environment.
        action_space: The action space of the environment.
        pi_config: The configuration for the policy network.
        vf_config: The configuration for the value network.
        encoder_config: The configuration for the encoder network.
    """
    observation_space: gym.Space = None
    action_space: gym.Space = None
    pi_config: FCConfig = None
    vf_config: FCConfig = None
    encoder_config: FCConfig = None

class SimplePPOModule(TorchRLModule):

    def __init__(self, config: PPOModuleConfig) -> None:
        super().__init__(config)

        assert (
            isinstance(self.config.observation_space, gym.Space.Box),
            "This simple PPOModule only supports Box observation space."
        )

        assert (
            self.config.observation_space.shape[0] == 1,
            "This simple PPOModule only supports 1D observation space."
        )

        assert (
            isinstance(self.config.action_space, (gym.Space.Discrete, gym.Space.Box)), 
            "This simple PPOModule only supports Discrete and Box action space."
        )

        assert self.config.pi_config, "pi_config must be provided."
        assert self.config.vf_config, "vf_config must be provided."

        self.encoder = None
        if self.config.encoder_config:
            encoder_config = self.config.encoder_config
            encoder_config.input_dim = self.config.observation_space.shape[0]
            self.encoder = self._build_fc_net(self.config.encoder_config)

        # build pi network
        pi_config = self.config.pi_config
        if not self.encoder:
            pi_config.input_dim = self.config.observation_space.shape[0]
        else:
            pi_config.input_dim = self.encoder.output_dim
        
        if isinstance(self.config.action_space, gym.Space.Discrete):
            pi_config.output_dim = self.config.action_space.n
        else:
            pi_config.output_dim = self.config.action_space.shape[0] * 2
        self.pi = self._build_fc_net(pi_config)
        
        # build vf network
        vf_config = self.config.vf_config
        if not self.encoder:
            vf_config.input_dim = self.config.observation_space.shape[0]
        else:
            vf_config.input_dim = self.encoder.output_dim

        vf_config.output_dim = 1
        self.vf = self._build_fc_net(vf_config)


    def forward_inference(self, batch):
        """During inference, we only return action = mu"""
        ret = self.forward_exploration(batch)
        ret["action"] = ret["action_dist"].mean
        return ret

    def forward_exploration(self, batch):
        mu, scale = self.pi(batch["obs"]).chunk(2, dim=-1)
        action_dist = torch.distributions.Normal(mu, scale.exp())
        return {"action_dist": action_dist}

    @override(RLModule)
    def input_specs_train(self) -> ModelSpecDict:
        if isinstance(self.config.action_space, gym.Space.Box):
            action_dim = self.config.action_space.shape[0]
        else:
            action_dim = self.config.action_space.n

        return ModelSpecDict({
            "obs": self.encoder.input_specs if self.encoder else self.pi.input_specs,
            "actions": TorchSpecs("b, da", da=action_dim)
        })

    @override(RLModule)
    def output_specs_train(self) -> ModelSpecDict:
        return ModelSpecDict({
            "action_dist": TorchActionDistributionSpecs,
            "logp": logp,
            "entropy": entropy,
            "vf": vf,
        })

    
    def _forward_train(self, batch: NestedDict) -> Mapping[str, Any]:
        if self.encoder:
            encoded_state = self.encoder(batch["obs"])
            mu, scale = self.pi(encoded_state).chunk(2, dim=-1)
            vf = self.vf(encoded_state)
        else:
            mu, scale = self.pi(batch["obs"]).chunk(2, dim=-1)
            vf = self.vf(batch["obs"])
        
        action_dist = torch.distributions.Normal(mu, scale.exp())
        logp = action_dist.log_prob(batch["actions"])
        entropy = action_dist.entropy()
        return {
            "action_dist": action_dist,
            "logp": logp,
            "entropy": entropy,
            "vf": vf,
        }


if __name__ == "__main__":

    model = SimplePPOModule({})
    print(model)

    bsize = 1
    batch = {
        "obs": torch.randn(bsize, 4),
        "actions": torch.randn(bsize, 2),
    }
    breakpoint()

    print(model(batch))

    print(model.forward_inference(batch))

    print(model.forward_exploration(batch))

    print(model.forward_train(batch))
