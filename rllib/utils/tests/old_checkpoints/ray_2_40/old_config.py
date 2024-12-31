# The original Ray RLlib-accepted PPO config at the time the (possibly older)
# checkpoint (possibly from an older python version) was created.

# Do NOT change this file. RLlib's `Checkpointable` class should simply ignore the
# `config` in here either way as it requires the user to bring along an up-to-date
# config.

from ray.rllib.algorithms.ppo import PPOConfig
from ray.rllib.core.rl_module.default_model_config import DefaultModelConfig
from ray.rllib.examples.envs.classes.multi_agent import MultiAgentCartPole
from ray.tune import register_env


register_env("multi_agent_cartpole", lambda cfg: MultiAgentCartPole(config=cfg))

config = (
    PPOConfig()
    .environment("multi_agent_cartpole", env_config={"num_agents": 2})
    # Keep things very small.
    .rl_module(model_config=DefaultModelConfig(fcnet_hiddens=[16]))
    .multi_agent(
        policy_mapping_fn=lambda aid, *arg, **kw: f"p{aid}",
        policies={"p0", "p1"},
        algorithm_config_overrides_per_module={
            "p0": PPOConfig.overrides(lr=0.00005),
            "p1": PPOConfig.overrides(lr=0.0001),
        },
    )
)
