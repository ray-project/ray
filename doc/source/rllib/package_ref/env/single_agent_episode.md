---
myst:
  html_meta:
    description: "API reference for RLlib's SingleAgentEpisode class, covering construction, information access, environment data, and episode chunking."
---

(single-agent-episode-reference-docs)=

# SingleAgentEpisode API

## rllib.env.single_agent_episode.SingleAgentEpisode

```{eval-rst}
.. currentmodule:: ray.rllib.env.single_agent_episode
```

### Constructor

```{eval-rst}
.. autosummary::
    :nosignatures:
    :toctree: env/

    ~SingleAgentEpisode
    ~SingleAgentEpisode.validate
```

### Getting basic information

```{eval-rst}
.. autosummary::
    :nosignatures:
    :toctree: env/

    ~SingleAgentEpisode.get_return
    ~SingleAgentEpisode.get_duration_s
    ~SingleAgentEpisode.is_done
    ~SingleAgentEpisode.is_numpy
    ~SingleAgentEpisode.env_steps
```

### Getting environment data

```{eval-rst}
.. autosummary::
    :nosignatures:
    :toctree: env/

    ~SingleAgentEpisode.get_observations
    ~SingleAgentEpisode.get_infos
    ~SingleAgentEpisode.get_actions
    ~SingleAgentEpisode.get_rewards
    ~SingleAgentEpisode.get_extra_model_outputs
```

### Adding data

```{eval-rst}
.. autosummary::
    :nosignatures:
    :toctree: env/

    ~SingleAgentEpisode.add_env_reset
    ~SingleAgentEpisode.add_env_step
```

### Creating and handling episode chunks

```{eval-rst}
.. autosummary::
    :nosignatures:
    :toctree: env/

    ~SingleAgentEpisode.cut
    ~SingleAgentEpisode.slice
    ~SingleAgentEpisode.concat_episode
    ~SingleAgentEpisode.to_numpy
```
