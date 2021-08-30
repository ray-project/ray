from copy import deepcopy
from numpy import float32
import os
from supersuit import normalize_obs_v0, dtype_v0, color_reduction_v0

import ray
from ray.rllib.agents.registry import get_trainer_class
from ray.rllib.env import PettingZooEnv
from pettingzoo.butterfly import pistonball_v4

from ray.tune.registry import register_env

if __name__ == "__main__":
    """For this script, you need:
    1. Algorithm name and according module, e.g.: "PPo" + agents.ppo as agent
    2. Name of the aec game you want to train on, e.g.: "pistonball".
    3. num_cpus
    4. num_rollouts

    Does require SuperSuit
    """
    alg_name = "PPO"

    # Function that outputs the environment you wish to register.
    def env_creator(config):
        env = pistonball_v4.env(local_ratio=config.get("local_ratio", 0.2))
        env = dtype_v0(env, dtype=float32)
        env = color_reduction_v0(env, mode="R")
        env = normalize_obs_v0(env)
        return env

    num_cpus = 1
    num_rollouts = 2

    # Gets default training configuration and specifies the POMgame to load.
    config = deepcopy(get_trainer_class(alg_name)._default_config)

    # Set environment config. This will be passed to
    # the env_creator function via the register env lambda below.
    config["env_config"] = {"local_ratio": 0.5}

    # Register env
    register_env("pistonball",
                 lambda config: PettingZooEnv(env_creator(config)))
    env = PettingZooEnv(env_creator(config))
    observation_space = env.observation_space
    action_space = env.action_space
    del env

    # Configuration for multiagent setup with policy sharing:
    config["multiagent"] = {
        # Setup a single, shared policy for all agents.
        "policies": {
            "av": (None, observation_space, action_space, {})
        },
        # Map all agents to that policy.
        "policy_mapping_fn": lambda agent_id, episode, **kwargs: "av",
    }

    # Use GPUs iff `RLLIB_NUM_GPUS` env var set to > 0.
    config["num_gpus"] = int(os.environ.get("RLLIB_NUM_GPUS", "0"))
    config["log_level"] = "DEBUG"
    config["num_workers"] = 1
    # Fragment length, collected at once from each worker and for each agent!
    config["rollout_fragment_length"] = 30
    # Training batch size -> Fragments are concatenated up to this point.
    config["train_batch_size"] = 200
    # After n steps, force reset simulation
    config["horizon"] = 200
    # Default: False
    config["no_done_at_end"] = False
    # Info: If False, each agents trajectory is expected to have
    # maximum one done=True in the last step of the trajectory.
    # If no_done_at_end = True, environment is not resetted
    # when dones[__all__]= True.

    # Initialize ray and trainer object
    ray.init(num_cpus=num_cpus + 1)
    trainer = get_trainer_class(alg_name)(env="pistonball", config=config)

    # Train once
    trainer.train()
