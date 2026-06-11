"""
This example shows how to take full control over what models and action distribution
are being built inside an RL Module. With this pattern, we can bypass a Catalog and
explicitly define our own models within a given RL Module.

Here, we plug a pre-trained MobileNet V3 (small) image encoder from `torchvision` into
a PPO RLModule and use it to encode image observations before the policy- and
value-heads. You can modify this example to accommodate your own encoder network or
other pre-trained networks.
"""
# __sphinx_doc_begin__
import gymnasium as gym
import numpy as np

from ray.rllib.algorithms.ppo.ppo import PPOConfig
from ray.rllib.algorithms.ppo.torch.default_ppo_torch_rl_module import (
    DefaultPPOTorchRLModule,
)
from ray.rllib.core.models.base import ENCODER_OUT, Encoder
from ray.rllib.core.models.configs import (
    ActorCriticEncoderConfig,
    MLPHeadConfig,
    ModelConfig,
)
from ray.rllib.core.models.torch.base import TorchModel
from ray.rllib.core.rl_module.rl_module import RLModuleSpec
from ray.rllib.examples.envs.classes.random_env import RandomEnv
from ray.rllib.utils.framework import try_import_torch

torch, nn = try_import_torch()

# torchvision's pre-trained image classifiers expect (3, 224, 224) inputs.
MOBILENET_INPUT_SHAPE = (3, 224, 224)


class MobileNetV3EncoderConfig(ModelConfig):
    # MobileNet V3 (small) has a flat output of length 1000 (its ImageNet logits).
    output_dims = (1000,)
    freeze = True

    def build(self, framework):
        assert framework == "torch", "Unsupported framework `{}`!".format(framework)
        return MobileNetV3Encoder(self)


class MobileNetV3Encoder(TorchModel, Encoder):
    """A MobileNet V3 (small) encoder for RLlib."""

    def __init__(self, config):
        super().__init__(config)
        # Load MobileNet V3 (small) with its default pre-trained ImageNet weights from
        # the installed torchvision. We use torchvision directly (rather than
        # `torch.hub.load`) so the model code always matches the installed torch.
        from torchvision.models import MobileNet_V3_Small_Weights, mobilenet_v3_small

        self.net = mobilenet_v3_small(weights=MobileNet_V3_Small_Weights.DEFAULT)
        if config.freeze:
            # We don't want to train this encoder, so freeze its parameters!
            for p in self.net.parameters():
                p.requires_grad = False

    def _forward(self, input_dict, **kwargs):
        return {ENCODER_OUT: (self.net(input_dict["obs"]))}


class MobileNetTorchPPORLModule(DefaultPPOTorchRLModule):
    """A DefaultPPORLModule with MobileNet V3 (small) as an encoder.

    The idea behind this model is to demonstrate how we can bypass catalog to
    take full control over what models and action distribution are being built.
    In this example, we do this to modify an existing RLModule with a custom encoder.
    """

    def setup(self):
        mobilenet_config = MobileNetV3EncoderConfig()
        # Since we want to use PPO, which is an actor-critic algorithm, we need to
        # use an ActorCriticEncoderConfig to wrap the base encoder config.
        actor_critic_encoder_config = ActorCriticEncoderConfig(
            base_encoder_config=mobilenet_config
        )

        self.encoder = actor_critic_encoder_config.build(framework="torch")
        mobilenet_output_dims = mobilenet_config.output_dims

        pi_config = MLPHeadConfig(
            input_dims=mobilenet_output_dims,
            output_layer_dim=2,
        )

        vf_config = MLPHeadConfig(input_dims=mobilenet_output_dims, output_layer_dim=1)

        self.pi = pi_config.build(framework="torch")
        self.vf = vf_config.build(framework="torch")


config = (
    PPOConfig()
    .environment(
        RandomEnv,
        env_config={
            "action_space": gym.spaces.Discrete(2),
            # Test a simple Image observation space.
            "observation_space": gym.spaces.Box(
                0.0,
                1.0,
                shape=MOBILENET_INPUT_SHAPE,
                dtype=np.float32,
            ),
        },
    )
    .env_runners(num_env_runners=0)
    # The following training settings make it so that a training iteration is very
    # quick. This is just for the sake of this example. PPO will not learn properly
    # with these settings!
    .training(
        train_batch_size_per_learner=32,
        minibatch_size=16,
        num_epochs=1,
    )
    .rl_module(rl_module_spec=RLModuleSpec(module_class=MobileNetTorchPPORLModule))
)

config.build().train()
# __sphinx_doc_end__
