.. _replay-buffer-api-reference-docs:

Replay Buffer API
==================

RLlib comes with a set of extendable replay buffers which are used mostly in Q-Learning algorithms.
The base :py:class:`~ray.rllib.utils.replay_buffers.replay_buffer.ReplayBuffer` class (link) only supports storing experiences in different storage units (link).
Advanced types add functionality while retaining compatibility through inheritance.
You can find buffer types and arguments to modify their behaviour as part of RLlib's default parameters. They are part of
the replay buffer config (link).

Basic Usage
------------

Here is a full example of running the R2D2 algorithm, which runs without prioritized replay by default, with prioritzed replay:

.. dropdown:: **Changing a replay buffer configuration**
    :animate: fade-in-slide-down

    .. literalinclude:: ../../../../rllib/examples/replay_buffer_api.py
        :language: python

    ..   start-after: __sphinx_doc_basic_replay_buffer_usage__begin__
    ..   end-before: __sphinx_doc_basic_replay_buffer_usage_end__

Specifying a buffer type works the same way as specifying an exploration type.
Here are three ways of specifying a type:

.. literalinclude:: ../../../../rllib/examples/documentation/replay_buffer_demo.py
   :language: python

..   start-after: __sphinx_doc_replay_buffer_type_specification__begin__
..   end-before: __sphinx_doc_replay_buffer_type_specification__end__

Apart from specifying a type, other parameters in that config are:

* Parameters that define how algorithms interact with replay buffers
   * e.g. "worker_side_prioritization" to decide where to compute priorities

* Constructor arguments to instantiate the replay buffer
   * e.g. "capacity" to limit the buffers size

* Call arguments for underlying replay buffer methods
   * e.g. "prioritized_replay_beta" is used by the :py:class:`~ray.rllib.utils.replay_buffers.multi_agent_prioritized_replay_buffer.PrioritizedMultiAgentReplayBuffer` to call the sample method of every underlying :py:class:`~ray.rllib.utils.replay_buffers.prioritized_replay_buffer.PrioritizedReplayBuffer`


Most of the time, only 1. and 2. are of interest.
3. is an advanced feature that supports use cases where a :py:class:`~ray.rllib.utils.replay_buffers.multi_agent_replay_buffer.MultiAgentReplayBuffer` instantiates underlying buffers that need constructor or default call arguments.

Here is an example of how to implement your own toy example of a ReplayBuffer class and make SimpleQ use it:

.. literalinclude:: ../../../../rllib/examples/documentation/replay_buffer_demo.py
   :language: python

..   start-after: __sphinx_doc_replay_buffer_own_buffer__begin__
..   end-before: __sphinx_doc_replay_buffer_own_buffer__begin__

Advanced Usage
---------------

In RLlib, all replay buffers implement the :py:class:`~ray.rllib.utils.replay_buffers.replay_buffer.ReplayBuffer` interface.
Therefore, they support, whenever possible, different :py:class:`~ray.rllib.utils.replay_buffers.replay_buffer.StorageUnit` s.
The storage_unit constructor argument of a replay buffer defines how sequences are stored, and therefore the unit in which they are sampled.
When later calling the sample() method, num_units will relate to said storage_unit.

Here is a full example of how to modify the storage_unit and interact with a custom buffer:

.. literalinclude:: ../../../../rllib/examples/documentation/replay_buffer_demo.py
   :language: python

..   start-after: __sphinx_doc_replay_buffer_advanced_usage_storage_unit__begin__
..   end-before: __sphinx_doc_replay_buffer_advanced_usage_storage_unit__end__

As noted above, Rllib's :py:class:`~ray.rllib.utils.replay_buffers.multi_agent_replay_buffer.MultiAgentReplayBuffer` s
support modification of underlying replay buffers. Under the hood, the :py:class:`~ray.rllib.utils.replay_buffers.multi_agent_replay_buffer.MultiAgentReplayBuffer`
stores experiences per policy in separate underlying replay buffers. You can modify their behaviour by specifying an underlying replay buffer config that works
the same as the parent's config.

Here is an example of how to specify an underlying replay buffer:

.. literalinclude:: ../../../../rllib/examples/documentation/replay_buffer_demo.py
   :language: python

..   start-after: __sphinx_doc_replay_buffer_advanced_usage_underlying_buffers__begin__
..   end-before: __sphinx_doc_replay_buffer_advanced_usage_underlying_buffers__end__



Replay Buffers API Reference
-----------------------------

.. toctree::
   :maxdepth: 1

   replay_buffers/replay_buffer.rst

