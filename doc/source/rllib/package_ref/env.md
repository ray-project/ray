---
myst:
  html_meta:
    description: "API reference for RLlib environments, covering environment vectorization, external environments, and the single-agent and multi-agent EnvRunner APIs."
---

(env-reference-docs)=

# Environments

RLlib mainly supports the [Farama gymnasium API](https://gymnasium.farama.org/) for
single-agent environments, and RLlib's own {py:class}`~ray.rllib.env.multi_agent_env.MultiAgentEnv`
API for multi-agent setups.

## Env Vectorization

For single-agent setups, RLlib automatically vectorizes your provided
[gymnasium.Env](https://gymnasium.farama.org/_modules/gymnasium/core/#Env) using
gymnasium's own [vectorization feature](https://gymnasium.farama.org/api/vector/).

Use the `config.env_runners(num_envs_per_env_runner=..)` setting to vectorize your env
beyond 1 env copy.

## External Envs

:::{note}
External Env support is under development on the new API stack. The recommended
way to implement your own external env connection logic, for example through TCP or
shared memory, is to write your own {py:class}`~ray.rllib.env.env_runner.EnvRunner`
subclass.
:::

See this an end-to-end example of an [external CartPole (client) env](https://github.com/ray-project/ray/blob/master/rllib/examples/envs/env_connecting_to_rllib_w_tcp_client.py)
connecting to RLlib through a custom, TCP-capable
{py:class}`~ray.rllib.env.env_runner.EnvRunner` server.

## Environment API Reference

```{toctree}
:maxdepth: 1

env/env_runner
env/single_agent_env_runner
env/single_agent_episode
env/multi_agent_env
env/multi_agent_env_runner
env/multi_agent_episode
env/external
env/utils
```
