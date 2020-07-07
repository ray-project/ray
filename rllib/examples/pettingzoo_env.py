from copy import deepcopy
import ray
try:
    from ray.rllib.agents.agent import get_agent_class
except ImportError:
    from ray.rllib.agents.registry import get_agent_class
from ray.tune.registry import register_env
from ray.rllib.env import PettingZooEnv
from pettingzoo.gamma import prison_v0
from supersuit.aec_wrappers import normalize_obs, dtype, color_reduction

from numpy import float32

if __name__ == "__main__":
    """For this script, you need:
    1. Algorithm name and according module, e.g.: "PPo" + agents.ppo as agent
    2. Name of the aec game you want to train on, e.g.: "prison".
    3. num_cpus
    4. num_rollouts

    Does require SuperSuit
    """
    alg_name = "PPO"

    # function that outputs the environment you wish to register.
    def env_creator(config):
        env = prison_v0.env(num_floors=config.get("num_floors", 4))
        env = dtype(env, dtype=float32)
        env = color_reduction(env, dtype=float32)
        env = normalize_obs(env, mode="R")
        return env

    num_cpus = 1
    num_rollouts = 2

    # 1. Gets default training configuration and specifies the POMgame to load.
    config = deepcopy(get_agent_class(alg_name)._default_config)

    # 2. Set environment config. This will be passed to
    # the env_creator function via the register env lambda below
    config["env_config"] = {"num_floors": 5}

    # 3. Register env
    register_env("prison", lambda config: PettingZooEnv(env_creator(config)))

    # 4. Extract space dimensions
    test_env = PettingZooEnv(env_creator({}))
    obs_space = test_env.observation_space
    act_space = test_env.action_space

    # 5. Configuration for multiagent setup with policy sharing:
    config["multiagent"] = {
        "policies": {
            # the first tuple value is None -> uses default policy
            "av": (None, obs_space, act_space, {}),
        },
        "policy_mapping_fn": lambda agent_id: "av"
    }

    config["log_level"] = "DEBUG"
    config["num_workers"] = 1
    # Fragment length, collected at once from each worker and for each agent!
    config["sample_batch_size"] = 30
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

    # 6. Initialize ray and trainer object
    ray.init(num_cpus=num_cpus + 1)
    trainer = get_agent_class(alg_name)(env="prison", config=config)

    # 7. Train once
    trainer.train()

    test_env.reset()
