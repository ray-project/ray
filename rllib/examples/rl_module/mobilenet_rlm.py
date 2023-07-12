"""
This example shows how to take full control over what models and action distribution
are being built inside an RL Module. With the pattern, we can bypass a Catalog and
explicitly define our own models within a given RL Module.
"""
# __sphinx_doc_begin__
import gymnasium as gym
import numpy as np

from ray.rllib.algorithms.ppo.ppo import PPOConfig
from ray.rllib.algorithms.ppo.torch.ppo_torch_rl_module import PPOTorchRLModule
from ray.rllib.core.models.base import Encoder, ENCODER_OUT
from ray.rllib.core.models.configs import MLPHeadConfig
from ray.rllib.core.models.torch.base import TorchModel
from ray.rllib.core.rl_module.rl_module import SingleAgentRLModuleSpec
from ray.rllib.examples.env.random_env import RandomEnv
from ray.rllib.models.torch.torch_distributions import TorchCategorical
from ray.rllib.utils.framework import try_import_torch

MOBILENET_INPUT_SHAPE = (3, 224, 224)

torch, nn = try_import_torch()


class MobileNetV2Encoder(TorchModel, Encoder):
    def __init__(self, config):
        super().__init__(config)
        self.net = torch.hub.load(
            "pytorch/vision:v0.6.0", "mobilenet_v2", pretrained=True
        )

    def _forward(self, input_dict, **kwargs):
        return {ENCODER_OUT: (self.net(input_dict["obs"]))}


class MobileNetTorchPPORLModule(PPOTorchRLModule):
    """A PPORLModules with mobilenet v2 as an encoder.

    The idea behind this model is to demonstrate how we can bypass catalog to
    take full control over what models and action distribution are being built.
    In this example, we do this to modify an existing RLModule with a custom encoder.
    """
    def setup(self):
        self.encoder = MobileNetV2Encoder(config=None)

        pi_config = MLPHeadConfig(
            input_dims=[1000],
            output_layer_dim=2,
        )

        vf_config = MLPHeadConfig(
            input_dims=[1000],
            output_layer_dim=1
        )

        self.pi = pi_config.build(framework="torch")
        self.vf = vf_config.build(framework="torch")

        self.action_dist_cls = TorchCategorical


config = (
    PPOConfig()
    .rl_module(rl_module_spec=SingleAgentRLModuleSpec(
        module_class=MobileNetTorchPPORLModule))
        .environment(RandomEnv, env_config={
        "action_space": gym.spaces.Discrete(2),
        # Test a simple Image observation space.
        "observation_space": gym.spaces.Box(
            0.0,
            1.0,
            shape=MOBILENET_INPUT_SHAPE,
            dtype=np.float32,
        ),
    }, )
)

config.build().train()
# __sphinx_doc_end__
