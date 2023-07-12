"""
This example shows two modifications:
- How to write a custom Encoder (using MobileNet v2)
- How to enhance Catalogs with this custom Encoder

With the pattern shown in this example, we can enhance Catalogs such that they extend
to new observation- or action spaces while retaining their original functionality.
"""
# __sphinx_doc_begin__
import functools
import gymnasium as gym
import numpy as np
import torch

from ray.rllib.algorithms.ppo.ppo import PPOConfig
from ray.rllib.algorithms.ppo.ppo_catalog import PPOCatalog
from ray.rllib.core.models.base import Encoder, ENCODER_OUT
from ray.rllib.core.models.configs import ModelConfig
from ray.rllib.core.models.torch.base import TorchModel
from ray.rllib.core.rl_module.rl_module import SingleAgentRLModuleSpec
from ray.rllib.examples.env.random_env import RandomEnv

MOBILENET_INPUT_SHAPE = (3, 224, 224)


class MobileNetV2EncoderConfig(ModelConfig):
    # MobileNet v2 has a flat output of (1000,).
    output_dims = (1000, )

    def build(self, framework):
        assert framework == "torch", "Unsupported framework `{}`!".format(framework)
        return MobileNetV2Encoder(self)


class MobileNetV2Encoder(TorchModel, Encoder):
    def __init__(self, config):
        super().__init__(config)
        self.net = torch.hub.load(
            "pytorch/vision:v0.6.0", "mobilenet_v2", pretrained=True
        )

    def _forward(self, input_dict, **kwargs):
        return {ENCODER_OUT: (self.net(input_dict["obs"]))}


# Define a PPO Catalog that we can use to inject our MobileNetV2 Encoder into RLlib's
# decision tree of what model to choose
class MobileNetEnhancedPPOCatalog(PPOCatalog):
    def __post_init__(self):
        if isinstance(self.observation_space, gym.spaces.Box) and self.observation_space.shape == MOBILENET_INPUT_SHAPE:
            # Inject our custom encoder here, only if the observation space fits it
            self.encoder_config = MobileNetV2EncoderConfig()
        else:
            # If the observation space does not fit, choose the default behaviour of
            # Catalogs
            self.encoder_config = self._get_encoder_config(
                observation_space=self.observation_space,
                action_space=self.action_space,
                model_config_dict=self.model_config_dict,
                view_requirements=self.view_requirements,
            )

        # We still need to set _action_dist_class_fn and latent_dims for the rest of
        # the PPOCatalog to work. The context of these is not part of this example.
        self._action_dist_class_fn = functools.partial(
            self._get_dist_cls_from_action_space, action_space=self.action_space
        )
        self.latent_dims = self.encoder_config.output_dims


# Create a generic config with our enhanced Catalog
ppo_config = (
    PPOConfig()
    .rl_module(rl_module_spec=SingleAgentRLModuleSpec(catalog_class=MobileNetEnhancedPPOCatalog))
    .rollouts(num_rollout_workers=0)
)

# Train with our MobileNetEncoder on a fitting RandomEnv
ppo_config.environment(RandomEnv, env_config={
            "action_space": gym.spaces.Discrete(2),
            # Test a simple Image observation space.
            "observation_space": gym.spaces.Box(
                0.0,
                1.0,
                shape=MOBILENET_INPUT_SHAPE,
                dtype=np.float32,
            ),
        },)
results = ppo_config.build().train()
print(results)

# Train without our MobileNetEncoder on another Env
ppo_config.environment("CartPole-v1")
results = ppo_config.build().train()
print(results)

# __sphinx_doc_end__
