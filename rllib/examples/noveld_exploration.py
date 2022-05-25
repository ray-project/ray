"""Example for using NovelD exploration for sparse reward settings.

Note: NovelD works to this point only synchronously, i.e. the 
`config["num_workers"]=0` is a requirement. 

The example also uses the NovelDMetricsCallbacks that activates 
some metrics in TensorBoard and gives the user a possibility to 
monitor the process of exploring.

The environment used for the example is the MiniGrid environment 
that can be found here: 

    https://github.com/maximecb/gym-minigrid
    
To install this environment simply run `pip install gym_minigrid`.

Note, NovelD works also with the torch framework and it can be 
used with most of RLLib's algorithms (agents). The distillation
network of the NovelD exploration can be configured in the same 
way as the Policy Network (and should use the same input layer). 
"""

import gym
import gym_minigrid
import ray

from ray.rllib.agents.ppo.ppo import PPOConfig
from ray import tune
from ray.tune.registry import register_env
from ray.rllib.utils.exploration.noveld import NovelDMetricsCallbacks


def env_creator(config=None):
    name = config.get("name", "MiniGrid-MultiRoom-N4-S5-v0")
    env = gym.make(name)
    env = gym_minigrid.wrappers.ImgObsWrapper(env)

    return env


register_env("mini-grid", env_creator)

config = (
    PPOConfig()
    .framework(framework="tf")
    .environment(env="mini-grid")
    # For other environment in the MiniGrid world use:
    #   env_config = {
    #       "name": "MiniGrid-Empty-8x8-v0",
    #   }
    .rollouts(
        num_envs_per_worker=4,
        # This is important: NovelD (as of now) does not work
        # asynchronously.
        num_rollout_workers=0,
        # In practice using standardized observations in the distillation
        # results in a more stable training.
        observation_filter=lambda _: tune.grid_search(["NoFilter", "MeanStdFilter"]),
    )
    .training(
        gamma=0.999,
        lr=1e-5,
        num_sgd_iter=8,
        entropy_coeff=0.0005,
        model={
            "post_fcnet_hiddens": [256, 256],
            "post_fcnet_activation": "relu",
            "conv_filters": [[32, [5, 5], 1], [128, [3, 3], 2], [512, [4, 4], 1]],
        },
    )
    .callbacks(
        # Use the callbacks for the NovelD metrics in TensorBoard.
        callbacks_class=NovelDMetricsCallbacks,
    )
    .exploration(
        exploration_config={
            "type": "NovelD",
            "embed_dim": 128,
            "lr": 0.0001,
            "intrinsic_reward_coeff": 0.005,
            "sub_exploration": {
                "type": "StochasticSampling",
            },
        },
    )
)

ray.init(local_mode=True, ignore_reinit_error=True)
tune.run(
    "PPO",
    config=config.to_dict(),
    stop={
        # "training_iteration": 10000,
        "num_env_steps_sampled": 10000,
    },
    verbose=1,
    num_samples=1,
    checkpoint_freq=200,
    checkpoint_at_end=True,
)
ray.shutdown()
