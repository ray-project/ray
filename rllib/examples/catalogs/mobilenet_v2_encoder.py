# @OldAPIStack

"""
This example shows two modifications:
- How to write a custom Encoder (using MobileNet v2)
- How to enhance Catalogs with this custom Encoder

With the pattern shown in this example, we can enhance Catalogs such that they extend
to new observation- or action spaces while retaining their original functionality.
"""
# __sphinx_doc_begin__
import gymnasium as gym
import numpy as np

from ray.rllib.algorithms.ppo.ppo import PPOConfig
from ray.rllib.algorithms.ppo.ppo_catalog import PPOCatalog
from ray.rllib.core.rl_module.rl_module import RLModuleSpec
from ray.rllib.examples._old_api_stack.models.mobilenet_v2_encoder import (
    MobileNetV2EncoderConfig,
    MOBILENET_INPUT_SHAPE,
)
from ray.rllib.examples.envs.classes.random_env import RandomEnv


# Define a PPO Catalog that we can use to inject our MobileNetV2 Encoder into RLlib's
# decision tree of what model to choose
class MobileNetEnhancedPPOCatalog(PPOCatalog):
    @classmethod
    def _get_encoder_config(
        cls,
        observation_space: gym.Space,
        **kwargs,
    ):
        if (
            isinstance(observation_space, gym.spaces.Box)
            and observation_space.shape == MOBILENET_INPUT_SHAPE
        ):
            # Inject our custom encoder here, only if the observation space fits it
            return MobileNetV2EncoderConfig()
        else:
            return super()._get_encoder_config(observation_space, **kwargs)


# Create a generic config with our enhanced Catalog
ppo_config = (
    PPOConfig()
    .api_stack(
        enable_rl_module_and_learner=True,
        enable_env_runner_and_connector_v2=True,
    )
    .rl_module(rl_module_spec=RLModuleSpec(catalog_class=MobileNetEnhancedPPOCatalog))
    .env_runners(num_env_runners=0)
    # The following training settings make it so that a training iteration is very
    # quick. This is just for the sake of this example. PPO will not learn properly
    # with these settings!
    .training(train_batch_size=32, minibatch_size=16, num_epochs=1)
)

# CartPole's observation space is not compatible with our MobileNetV2 Encoder, so
# this will use the default behaviour of Catalogs
ppo_config.environment("CartPole-v1")
results = ppo_config.build().train()
print(results)

# For this training, we use a RandomEnv with observations of shape
# MOBILENET_INPUT_SHAPE. This will use our custom Encoder.
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
