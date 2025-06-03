"""
This example demonstrates the use of a custom masked self-attention encoder (based off of https://arxiv.org/abs/1909.07528) to handle variable-length Repeated observation spaces.

It demonstrates:

 - How to write a custom catalog and pytorch Encoder
 - How to operate upon a Repeated observation space when training a model

"""

# __sphinx_doc_begin__
import gymnasium as gym
import numpy as np

from ray.rllib.algorithms.ppo.ppo import PPOConfig
from ray.rllib.algorithms.ppo.ppo_catalog import PPOCatalog
from ray.rllib.core.rl_module.rl_module import RLModuleSpec
from ray.tune.registry import register_env

from ray.rllib.env.wrappers.repeated_wrapper import ObsVectorizationWrapper
from ray.rllib.examples.catalogs.models.attention_encoder import AttentionEncoderConfig
from ray.rllib.examples.envs.classes.repeated_obs_env import RepeatedObsEnv

# TODO: Remove
from ray.rllib.utils.metrics import (
    DIFF_NUM_GRAD_UPDATES_VS_SAMPLER_POLICY,
    ENV_RUNNER_RESULTS,
    EPISODE_RETURN_MEAN,
    EVALUATION_RESULTS,
    NUM_ENV_STEPS_TRAINED,
    NUM_ENV_STEPS_SAMPLED_LIFETIME,
)

# Define a PPO Catalog that we can use to inject our attention-based Encoder into RLlib's
# decision tree of what model to choose
class AttentionPPOCatalog(PPOCatalog):
    '''
      A special PPO catalog producing an encoder that handles dictionaries of (potentially Repeated) action spaces in the same manner as https://arxiv.org/abs/1909.07528.
    '''
    @classmethod
    def _get_encoder_config(cls,observation_space: gym.Space,**kwargs,):
      return AttentionEncoderConfig(observation_space, **kwargs)

register_env("env", lambda cfg: ObsVectorizationWrapper(RepeatedObsEnv(cfg)))

# Create a generic config with our enhanced Catalog
ppo_config = (
    PPOConfig()
    .environment(
        env="env",
        env_config={
            'max_voter_pairs': 5,
            'num_values': 2,
            'random_seed': 0
        },
    )
    .framework("torch")
    .env_runners(
        num_env_runners=0,
        num_envs_per_env_runner=1,
    )
    .training(
        train_batch_size=256,
    )
    .rl_module(rl_module_spec=RLModuleSpec(
          catalog_class=AttentionPPOCatalog,
          model_config={
              "attention_emb_dim": 128,
              "head_fcnet_hiddens": (256,256),
              'vf_share_layers': False,
          }
        ),
    )
)

algo = ppo_config.build_algo()

# TODO: Switch this to the experiment setup used by everything else.
num_iters = 100

for i in range(num_iters):
  results = algo.train()
  if ENV_RUNNER_RESULTS in results:
      mean_return = results[ENV_RUNNER_RESULTS].get(
          EPISODE_RETURN_MEAN, np.nan
      )
      print(f"iter={i} R={mean_return}")
# __sphinx_doc_end__
