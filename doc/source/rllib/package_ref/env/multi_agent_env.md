---
myst:
  html_meta:
    description: "API reference for RLlib's MultiAgentEnv class and the make_multi_agent helper."
---

(multi-agent-env-reference-docs)=

# MultiAgentEnv API

## rllib.env.multi_agent_env.MultiAgentEnv

```{eval-rst}
.. autoclass:: ray.rllib.env.multi_agent_env.MultiAgentEnv

    .. automethod:: __init__
    .. automethod:: reset
    .. automethod:: step
    .. automethod:: get_observation_space
    .. automethod:: get_action_space
    .. automethod:: with_agent_groups
    .. automethod:: render
```

## Convert gymnasium.Env into MultiAgentEnv

```{eval-rst}
.. automodule:: ray.rllib.env.multi_agent_env
    :members: make_multi_agent
```
