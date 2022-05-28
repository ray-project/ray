.. _replay-buffer-api-reference-docs:

Replay Buffer API
==================

RLlib comes with a set of extendable replay buffers which are used mostly in Q-Learning algorithms.
We provide a base :py:class:`~ray.rllib.utils.replay_buffers.replay_buffer.ReplayBuffer` class from which you can build your own buffer.
Some use cases may require :py:class:`~ray.rllib.utils.replay_buffers.multi_agent_replay_buffer.MultiAgentReplayBuffer`\s.
You can find buffer types and arguments to modify their behaviour as part of RLlib's default parameters. They are part of
the ``replay_buffer_config``.

Basic Usage
===========

You rarely have to instantiate your own replay buffer when running an experiment, but rather configure it as follows.
Here is an example of configuring the R2D2 algorithm, which runs without `PER <https://arxiv.org/abs/1511.05952>`__ by default, with PER:

.. dropdown:: **Changing a replay buffer configuration**
    :animate: fade-in-slide-down

    .. literalinclude:: ../../../../rllib/examples/replay_buffer_api.py
        :language: python
        :start-after: __sphinx_doc_basic_replay_buffer_usage__begin__
        :end-before: __sphinx_doc_basic_replay_buffer_usage_end__

Specifying a buffer type works the same way as specifying an exploration type.
Here are three ways of specifying a type:

.. dropdown:: **Changing a replay buffer configuration**
    :animate: fade-in-slide-down

    .. literalinclude:: ../../../../rllib/examples/documentation/replay_buffer_demo.py
        :language: python
        :start-after: __sphinx_doc_replay_buffer_type_specification__begin__
        :end-before: __sphinx_doc_replay_buffer_type_specification__end__

Apart from specifying a type, other parameters in that config are:

* Parameters that define how algorithms interact with replay buffers
   * e.g. "worker_side_prioritization" to decide where to compute priorities

* Constructor arguments to instantiate the replay buffer
   * e.g. "capacity" to limit the buffer's size

* Call arguments for underlying replay buffer methods
   * e.g. "prioritized_replay_beta" is used by the :py:class:`~ray.rllib.utils.replay_buffers.multi_agent_prioritized_replay_buffer.MultiAgentPrioritizedReplayBuffer` to call the sample method of every underlying :py:class:`~ray.rllib.utils.replay_buffers.prioritized_replay_buffer.PrioritizedReplayBuffer`


Most of the time, only 1. and 2. are of interest.
3. is an advanced feature that supports use cases where a :py:class:`~ray.rllib.utils.replay_buffers.multi_agent_replay_buffer.MultiAgentReplayBuffer` instantiates underlying buffers that need constructor or default call arguments.


ReplayBuffer Base Class
-----------------------

The base :py:class:`~ray.rllib.utils.replay_buffers.replay_buffer.ReplayBuffer` class only supports storing and replaying
experiences in different :py:class:`~ray.rllib.utils.replay_buffers.replay_buffer.StorageUnit`\s.
Advanced buffer types add functionality while trying retaining compatibility through inheritance.
The following excerpt from the class documentation only shows how to construct, sample from and add to a replay buffer.
If you plan on implementing your own buffer, these are also the main places to look at.

.. dropdown:: **Constructor, add and sample methods.**
    :animate: fade-in-slide-down

    .. autoclass:: ray.rllib.utils.replay_buffers.replay_buffer.ReplayBuffer
        :noindex:

        .. automethod:: __init__
            :noindex:
        .. automethod:: add
            :noindex:
        .. automethod:: sample
            :noindex:


Building your own ReplayBuffer
------------------------------
Here is an example of how to implement your own toy example of a ReplayBuffer class and make SimpleQ use it:

.. literalinclude:: ../../../../rllib/examples/documentation/replay_buffer_demo.py
    :language: python
    :start-after: __sphinx_doc_replay_buffer_own_buffer__begin__
    :end-before: __sphinx_doc_replay_buffer_own_buffer__end__

For a full implementation, you should consider other methods like get_state() and set_state() methods.
A more extensive example is our implementation of `reservoir sampling <https://www.cs.umd.edu/~samir/498/vitter.pdf>`__:

.. dropdown:: **Changing a replay buffer configuration**
    :animate: fade-in-slide-down

    .. literalinclude:: ../../../../rllib/utils/replay_buffers/reservoir_replay_buffer.py
        :language: python
        :start-after: __sphinx_doc_reservoir_buffer__begin__
        :end-before: __sphinx_doc_reservoir_buffer__end__


Advanced Usage
==============

In RLlib, all replay buffers implement the :py:class:`~ray.rllib.utils.replay_buffers.replay_buffer.ReplayBuffer` interface.
Therefore, they support, whenever possible, different :py:class:`~ray.rllib.utils.replay_buffers.replay_buffer.StorageUnit`\s.
The storage_unit constructor argument of a replay buffer defines how experiences are stored, and therefore the unit in which they are sampled.
When later calling the sample() method, num_items will relate to said storage_unit.

Here is a full example of how to modify the storage_unit and interact with a custom buffer:

.. literalinclude:: ../../../../rllib/examples/documentation/replay_buffer_demo.py
    :language: python
    :start-after: __sphinx_doc_replay_buffer_advanced_usage_storage_unit__begin__
    :end-before: __sphinx_doc_replay_buffer_advanced_usage_storage_unit__end__

As noted above, Rllib's :py:class:`~ray.rllib.utils.replay_buffers.multi_agent_replay_buffer.MultiAgentReplayBuffer`\s
support modification of underlying replay buffers. Under the hood, the :py:class:`~ray.rllib.utils.replay_buffers.multi_agent_replay_buffer.MultiAgentReplayBuffer`
stores experiences per policy in separate underlying replay buffers. You can modify their behaviour by specifying an underlying ``replay_buffer_config`` that works
the same way as the parent's config.

Here is an example of how to specify an underlying replay buffer along with a default call argument:

.. literalinclude:: ../../../../rllib/examples/documentation/replay_buffer_demo.py
    :language: python
    :start-after: __sphinx_doc_replay_buffer_advanced_usage_underlying_buffers__begin__
    :end-before: __sphinx_doc_replay_buffer_advanced_usage_underlying_buffers__end__


Replay Buffers API Reference
============================

.. toctree::
    :maxdepth: 1

    replay_buffers/replay_buffer.rst

