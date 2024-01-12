import re
from typing import Dict

import safety_gymnasium
from envs.pendulum.pendulum import PendulumEnv, SafePendulumEnv

from .saute_env import SauteEnv
from ray.tune.registry import register_env


def get_safety_gym_names(env_name: str):
    class_names = re.findall("[A-Z][^A-Z]*", env_name.split("-")[0])
    safety_type = class_names[0]
    robot_name = class_names[1]
    task_name = "".join(class_names[2:])
    return safety_type, robot_name, task_name


def safetygym_env_creator(env_name: str, config: Dict):
    safety_type, robot_name, task_name = get_safety_gym_names(env_name)
    if safety_type == "Saute":
        env_name = "Safety" + env_name[5:]
    env = safety_gymnasium.make(env_name)
    # gymnasium wrapper
    env = safety_gymnasium.wrappers.SafetyGymnasium2Gymnasium(env)
    # saute wrapper
    if safety_type == "Saute":
        env = SauteEnv(
            env,
            safety_budget=config["cost_lim"],
            saute_discount_factor=config["cost_gamma"],
            max_ep_len=config["max_ep_len"],
            use_reward_shaping=False,
            use_state_augmentation=True,
        )
    return env


def custom_pendulum_creator(config: Dict):
    return PendulumEnv()


def safe_pendulum_creator(config: Dict):
    return SafePendulumEnv()


def saute_pendulum_creator(env_config: Dict):
    env = SafePendulumEnv()
    return SauteEnv(
        env,
        safety_budget=env_config["cost_lim"],
        saute_discount_factor=env_config["cost_gamma"],
        max_ep_len=env_config["max_ep_len"],
        use_reward_shaping=False,
        use_state_augmentation=True,
    )


register_env("CustomPendulum-v0", custom_pendulum_creator)

register_env("SafePendulum-v0", safe_pendulum_creator)

register_env("SautePendulum-v0", saute_pendulum_creator)

register_env(
    "SafetyPointGoal1-v0",
    lambda config: safetygym_env_creator("SafetyPointGoal1-v0", config),
)

register_env(
    "SautePointGoal1-v0",
    lambda config: safetygym_env_creator("SautePointGoal1-v0", config),
)

register_env(
    "SafetyPointSimpleGoal1-v0",
    lambda config: safetygym_env_creator("SafetyPointSimpleGoal1-v0", config),
)

register_env(
    "SautePointSimpleGoal1-v0",
    lambda config: safetygym_env_creator("SautePointSimpleGoal1-v0", config),
)
