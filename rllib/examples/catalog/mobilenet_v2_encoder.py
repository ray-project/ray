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

from ray.rllib.algorithms.ppo.ppo import PPOConfig
from ray.rllib.algorithms.ppo.ppo_catalog import PPOCatalog
from ray.rllib.examples.models.mobilenet_v2_encoder import (
    MobileNetV2EncoderConfig,
    MOBILENET_INPUT_SHAPE,
)
from ray.rllib.core.rl_module.rl_module import SingleAgentRLModuleSpec
from ray.rllib.examples.env.random_env import RandomEnv


# Define a PPO Catalog that we can use to inject our MobileNetV2 Encoder into RLlib's
# decision tree of what model to choose
class MobileNetEnhancedPPOCatalog(PPOCatalog):
    def __post_init__(self):
        if (
            isinstance(self.observation_space, gym.spaces.Box)
            and self.observation_space.shape == MOBILENET_INPUT_SHAPE
        ):
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
    .rl_module(
        rl_module_spec=SingleAgentRLModuleSpec(
            catalog_class=MobileNetEnhancedPPOCatalog
        )
    )
    .rollouts(num_rollout_workers=0)
    # The following training settings make it so that a training iteration is very
    # quick. This is just for the sake of this example. PPO will not learn properly
    # with these settings!
    .training(train_batch_size=32, sgd_minibatch_size=16, num_sgd_iter=1)
)

# Train without our MobileNetEncoder on CartPole-v1
ppo_config.environment("CartPole-v1")
results = ppo_config.build().train()
print(results)

# Train with our MobileNetEncoder on a RandomEnv with the same Catalog
ppo_config.environment(
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
results = ppo_config.build().train()
print(results)
# __sphinx_doc_end__
