---
myst:
  html_meta:
    description: "API reference for RLlib's replay buffer classes, covering single-agent and multi-agent buffers and their sampling and state methods."
---

(replay-buffer-api-reference-docs)=

# Replay Buffer API

The following classes don't take into account the separation of experiences from different policies, multi-agent replay buffers will be explained further below.

## Replay Buffer Base Classes

```{eval-rst}
.. currentmodule:: ray.rllib.utils.replay_buffers
```

```{eval-rst}
.. autosummary::
    :nosignatures:
    :toctree: doc/

    ~replay_buffer.StorageUnit
    ~replay_buffer.ReplayBuffer
    ~prioritized_replay_buffer.PrioritizedReplayBuffer
    ~reservoir_replay_buffer.ReservoirReplayBuffer
```

## Public Methods

```{eval-rst}
.. currentmodule:: ray.rllib.utils.replay_buffers.replay_buffer
```

```{eval-rst}
.. autosummary::
    :nosignatures:
    :toctree: doc/

    ~ReplayBuffer.sample
    ~ReplayBuffer.add
    ~ReplayBuffer.get_state
    ~ReplayBuffer.set_state
```

## Multi Agent Buffers

The following classes use the above, "single-agent", buffers as underlying buffers to facilitate splitting up experiences between the different agents' policies.
In multi-agent RL, more than one agent exists in the environment and not all of these agents may utilize the same policy (mapping M agents to N policies, where M <= N).
This leads to the need for MultiAgentReplayBuffers that store the experiences of different policies separately.

```{eval-rst}
.. currentmodule:: ray.rllib.utils.replay_buffers
```

```{eval-rst}
.. autosummary::
    :nosignatures:
    :toctree: doc/

    ~multi_agent_replay_buffer.MultiAgentReplayBuffer
    ~multi_agent_prioritized_replay_buffer.MultiAgentPrioritizedReplayBuffer
```

## Utility Methods

```{eval-rst}
.. autosummary::
    :nosignatures:
    :toctree: doc/

    ~utils.update_priorities_in_replay_buffer
    ~utils.sample_min_n_steps_from_buffer
```
