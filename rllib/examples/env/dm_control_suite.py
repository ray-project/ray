from ray.rllib.env.dm_control_wrapper import DMCEnv
import numpy as np
"""
8 Environments from Deepmind Control Suite
"""


def acrobot_swingup():
    return DMCEnv(
        "acrobot",
        "swingup",
        from_pixels=True,
        height=64,
        width=64,
        task_kwargs={"random": np.random.randint(low=0, high=1e9)})


def hopper_hop():
    return DMCEnv(
        "hopper",
        "hop",
        from_pixels=True,
        height=64,
        width=64,
        task_kwargs={"random": np.random.randint(low=0, high=1e9)})


def hopper_stand():
    return DMCEnv(
        "hopper",
        "stand",
        from_pixels=True,
        height=64,
        width=64,
        task_kwargs={"random": np.random.randint(low=0, high=1e9)})


def cheetah_run():
    return DMCEnv(
        "cheetah",
        "run",
        from_pixels=True,
        height=64,
        width=64,
        task_kwargs={"random": np.random.randint(low=0, high=1e9)})


def walker_run():
    return DMCEnv(
        "walker",
        "run",
        from_pixels=True,
        height=64,
        width=64,
        task_kwargs={"random": np.random.randint(low=0, high=1e9)})


def pendulum_swingup():
    return DMCEnv(
        "pendulum",
        "swingup",
        from_pixels=True,
        height=64,
        width=64,
        task_kwargs={"random": np.random.randint(low=0, high=1e9)})


def cartpole_swingup():
    return DMCEnv(
        "cartpole",
        "swingup",
        from_pixels=True,
        height=64,
        width=64,
        task_kwargs={"random": np.random.randint(low=0, high=1e9)})


def humanoid_walk():
    return DMCEnv(
        "humanoid",
        "walk",
        from_pixels=True,
        height=64,
        width=64,
        task_kwargs={"random": np.random.randint(low=0, high=1e9)})
