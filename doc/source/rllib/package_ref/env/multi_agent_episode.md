---
myst:
  html_meta:
    description: "API reference for RLlib's MultiAgentEpisode class, covering construction, information access, environment data, and episode chunking."
---

(multi-agent-episode-reference-docs)=

# MultiAgentEpisode API

## rllib.env.multi_agent_episode.MultiAgentEpisode

```{eval-rst}
.. currentmodule:: ray.rllib.env.multi_agent_episode
```

### Constructor

```{eval-rst}
.. autosummary::
    :nosignatures:
    :toctree: env/

    ~MultiAgentEpisode
    ~MultiAgentEpisode.validate
```

### Getting basic information

```{eval-rst}
.. autosummary::
    :nosignatures:
    :toctree: env/

    ~MultiAgentEpisode.get_return
    ~MultiAgentEpisode.get_duration_s
    ~MultiAgentEpisode.is_done
    ~MultiAgentEpisode.is_numpy
    ~MultiAgentEpisode.env_steps
    ~MultiAgentEpisode.agent_steps
```

### Multi-agent information

```{eval-rst}
.. autosummary::
    :nosignatures:
    :toctree: env/

    ~MultiAgentEpisode.module_for
    ~MultiAgentEpisode.get_agents_to_act
    ~MultiAgentEpisode.get_agents_that_stepped
```

### Getting environment data

```{eval-rst}
.. autosummary::
    :nosignatures:
    :toctree: env/

    ~MultiAgentEpisode.get_observations
    ~MultiAgentEpisode.get_infos
    ~MultiAgentEpisode.get_actions
    ~MultiAgentEpisode.get_rewards
    ~MultiAgentEpisode.get_extra_model_outputs
    ~MultiAgentEpisode.get_terminateds
    ~MultiAgentEpisode.get_truncateds
```

### Adding data

```{eval-rst}
.. autosummary::
    :nosignatures:
    :toctree: env/

    ~MultiAgentEpisode.add_env_reset
    ~MultiAgentEpisode.add_env_step
```

### Creating and handling episode chunks

```{eval-rst}
.. autosummary::
    :nosignatures:
    :toctree: env/

    ~MultiAgentEpisode.cut
    ~MultiAgentEpisode.slice
    ~MultiAgentEpisode.concat_episode
    ~MultiAgentEpisode.to_numpy
```
