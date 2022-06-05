.. _replay-buffer-api-reference-docs:

ReplayBuffer API
================

The following classes don't take into account the separation of experiences from different policies, multi-agent replay buffers will be explained further below.

ray.rllib.utils.replay_buffers.replay_buffer
---------------------------------------------

.. autoclass:: ray.rllib.utils.replay_buffers.replay_buffer.StorageUnit
    :members:

.. autoclass:: ray.rllib.utils.replay_buffers.replay_buffer.ReplayBuffer
    :members:
    :show-inheritance:

ray.rllib.utils.replay_buffers.prioritized_replay_buffer
--------------------------------------------------------

.. autoclass:: ray.rllib.utils.replay_buffers.prioritized_replay_buffer.PrioritizedReplayBuffer
    :members:
    :show-inheritance:

ray.rllib.utils.replay_buffers.reservoir_replay_buffer
------------------------------------------------------

.. autoclass:: ray.rllib.utils.replay_buffers.reservoir_replay_buffer.ReservoirReplayBuffer
    :members:
    :show-inheritance:


MultiAgentReplayBuffer classes
==============================

The following classes use the above, "single-agent", buffers as underlying buffers to facilitate splitting up experiences between the different agents' policies.
In multi-agent RL, more than one agent exists in the environment and not all of these agents may utilize the same policy (mapping M agents to N policies, where M <= N).
This leads to the need for MultiAgentReplayBuffers that store the experiences of different policies separately.

ray.rllib.utils.replay_buffers.multi_agent_replay_buffer
--------------------------------------------------------

.. autoclass:: ray.rllib.utils.replay_buffers.multi_agent_replay_buffer.MultiAgentReplayBuffer
    :members:
    :show-inheritance:

ray.rllib.utils.replay_buffers.multi_agent_prioritized_replay_buffer
--------------------------------------------------------------------

.. autoclass:: ray.rllib.utils.replay_buffers.multi_agent_prioritized_replay_buffer.MultiAgentPrioritizedReplayBuffer
    :members:
    :show-inheritance:

Utility Methods
===============

.. automethod:: ray.rllib.utils.replay_buffers.utils.update_priorities_in_replay_buffer

.. automethod:: ray.rllib.utils.replay_buffers.utils.sample_min_n_steps_from_buffer
