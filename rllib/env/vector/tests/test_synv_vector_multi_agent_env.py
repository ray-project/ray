import gymnasium as gym
from functools import partial
from ray.rllib.examples.envs.classes.multi_agent import MultiAgentCartPole
from ray.rllib.env.vector.registration import make_vec

def env_creator(config):
    return MultiAgentCartPole(config)

env_config = {"num_agents": 2}
entry_point = partial(
    env_creator,
    env_config,
)

gym.register(
    "rllib-multi-agent-env-v0",
    entry_point=entry_point,
    disable_env_checker=True,
)

env = make_vec("rllib-multi-agent-env-v0", num_envs=2, vectorization_mode="sync", vector_kwargs={"copy": True})

obs, infos = env.reset() 

