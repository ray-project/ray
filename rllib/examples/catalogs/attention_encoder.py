"""
This example demonstrates the use of a custom masked self-attention encoder (based off of https://arxiv.org/abs/1909.07528) to handle variable-length Repeated observation spaces.

It demonstrates:

 - How to write a custom catalog and pytorch Encoder
 - How to operate upon a Repeated observation space when training a model
 
How to run this script
----------------------
`python [script file name].py

For debugging, use the following additional command line options
`--no-tune --num-env-runners=0`
which should allow you to set breakpoints anywhere in the RLlib code and
have the execution stop there for inspection and debugging.

For logging to your WandB account, use:
`--wandb-key=[your WandB API key] --wandb-project=[some project name]
--wandb-run-name=[optional: WandB run name (within the defined project)]`
"""
import gymnasium as gym
import numpy as np

from ray.rllib.algorithms.ppo.ppo import PPOConfig
from ray.rllib.algorithms.ppo.ppo_catalog import PPOCatalog
from ray.rllib.core.rl_module.rl_module import RLModuleSpec
from ray.tune.registry import register_env
from ray.rllib.utils.test_utils import (
    add_rllib_example_script_args,
    run_rllib_example_script_experiment,
)

from ray.rllib.env.wrappers.repeated_wrapper import ObsVectorizationWrapper
from ray.rllib.examples.catalogs.models.attention_encoder import AttentionEncoderConfig
from ray.rllib.examples.envs.classes.repeated_obs_env import RepeatedObsEnv

from ray.rllib.utils.metrics import (
    ENV_RUNNER_RESULTS,
    EPISODE_RETURN_MEAN,
    TRAINING_ITERATION_TIMER,
)

# Handle command line args
parser = add_rllib_example_script_args(
    default_reward=0.8, default_iters=50
)
parser.set_defaults(
    enable_new_api_stack=True,
)
parser.add_argument("--max-voter-pairs",type=int,default=5)
parser.add_argument("--num-values",type=int,default=2)
parser.add_argument("--attn-dim",type=int,default=128)

# Define a PPO Catalog that tells Rllib to use our custom encoder
class AttentionPPOCatalog(PPOCatalog):
    '''
      A special PPO catalog producing an encoder that handles dictionaries of (potentially Repeated) action spaces in the same manner as https://arxiv.org/abs/1909.07528.
    '''
    @classmethod
    def _get_encoder_config(cls,observation_space: gym.Space,**kwargs,):
      return AttentionEncoderConfig(observation_space, **kwargs)

if __name__ == "__main__":
    args = parser.parse_args()
    # The wrapper allows Repeated observations to be batched successfully by Rllib.
    register_env("env", lambda cfg: ObsVectorizationWrapper(RepeatedObsEnv(cfg)))

    # Define our config
    config = (
        PPOConfig()
        .environment(
            env="env",
            env_config={
                'max_voter_pairs': args.max_voter_pairs,
                'num_values': args.num_values,
                'random_seed': 0
            },
        )
        .framework("torch")
        .training(
            train_batch_size=256,
        )
        .rl_module(rl_module_spec=RLModuleSpec(
              catalog_class=AttentionPPOCatalog,
              model_config={
                  "attention_emb_dim": args.attn_dim,
                  "head_fcnet_hiddens": (256,256),
                  'vf_share_layers': False,
              }
            ),
        )
    )

    # Set the stopping arguments.
    EPISODE_RETURN_MEAN_KEY = f"{ENV_RUNNER_RESULTS}/{EPISODE_RETURN_MEAN}"
    stop = {
        TRAINING_ITERATION_TIMER: args.stop_iters,
        EPISODE_RETURN_MEAN_KEY: args.stop_reward,
    }

    # Run the experiment.
    run_rllib_example_script_experiment(config,
        args,
        stop=stop,
        success_metric={
            f"{ENV_RUNNER_RESULTS}/{EPISODE_RETURN_MEAN}": args.stop_reward,
        },
    )