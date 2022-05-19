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
from ray.rllib.agents.callbacks import NovelDMetricsCallbacks
from ray.rllib.agents.ppo import ppo
from ray import tune
from ray.tune.registry import register_env


def env_creator(config=None):
    name = config.get("name", "MiniGrid-MultiRoom-N4-S5-v0")
    env = gym.make(name)
    env = gym_minigrid.wrappers.ImgObsWrapper(env)

    return env


register_env("mini-grid", env_creator)

CONV_FILTERS = [[32, [5, 5], 1], [128, [3, 3], 2], [512, [4, 4], 1]]
config = ppo.DEFAULT_CONFIG.copy()
config["framework"] = "tf"
config["env"] = "mini-grid"
# For other environment in the MiniGrid world use:
# config["env_config"] = {
#     "name": "MiniGrid-Empty-8x8-v0",
# }
config["num_envs_per_worker"] = 4
config["model"]["post_fcnet_hiddens"] = [256, 256]
config["model"]["post_fcnet_activation"] = "relu"
config["gamma"] = 0.999
config["num_sgd_iter"] = 8
config["entropy_coeff"] = 0.0005
# This is important: NovelD (as of now) does not work
# asynchronously.
config["num_workers"] = 0
# In practice using standardized observations in the distillation
# results in a more stable training.
config["observation_filter"] = tune.grid_search(["NoFilter", "MeanStdFilter"])
config["lr"] = 1e-5
config["model"]["conv_filters"] = CONV_FILTERS
config["record_env"] = "videos"
config["exploration_config"] = {
    "type": "NovelD",
    "embed_dim": 128,
    "lr": 0.0001,
    "intrinsic_reward_coeff": 0.005,
    "sub_exploration": {
        "type": "StochasticSampling",
    },
}
# Use the callbacks for the NovelD metrics in TensorBoard.
config["callbacks"] = NovelDMetricsCallbacks

ray.init(local_mode=True, ignore_reinit_error=True)
tune.run(
    "PPO",
    config=config,
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
