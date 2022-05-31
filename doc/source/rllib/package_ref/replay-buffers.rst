.. _replay-buffer-reference-docs:

ReplayBuffer API
================

These classes don't take into account the separation of experiences from different policies, which we use in multi-agent settings.

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

These classes use the above, "single-agent", buffers as underlying buffers to facilitate splitting up experiences between policies.

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
