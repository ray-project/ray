.. _replay-buffer-api-reference-docs:

Replay Buffers
##############

Quick intro into replay buffers in RL
=====================================

When we talk about replay buffers in reinforcement learning, we generally mean a buffer that stores and replays experiences collected from interactions of our agent(s) with the environment.
In python, a simple buffer can be implemented by a list to which elements are added and sampled from.
Such buffers are used mostly in off-policy learning algorithms. This makes sense intuitively because these algorithms can learn from
experiences that are stored in the buffer but where produced by a previous version of the policy (or even a completely different policy).

Sampling Strategy
-----------------

When sampling from a replay buffer, we choose which experiences to train our agent with. A straightforward strategy to choose these
samples that has proven effective for many algorithms is uniform sampling. That is, choosing a random item from the buffer each time we sample.
A strategy that has proven better in many cases is `Prioritized Experiences Replay (PER) <https://arxiv.org/abs/1511.05952>`__.
In PER, single items in the buffer are assigned a priority which denotes their significance, or in simple terms, how much we expect
to learn from these items. As a consequence, experiences with a higher priority are made more likely to be sampled.

Eviction Strategy
-----------------

A buffer will generally be limited in its capacity to hold experiences. In the course of running an algorith, a buffer will eventually reach
its capacity and in order to make room for new experiences, we need to delete (evict) older ones. This is generally done on a first-in-first-out basis.
For your algorithms this means that buffers with a high capacity give the opportunity to learn from older samples, while smaller buffers
make the learning process more on-policy. An exception from this strategy is made in buffers that implement `reservoir sampling <https://www.cs.umd.edu/~samir/498/vitter.pdf>`__.


Replay Buffers in RLlib
=======================

RLlib comes with a set of extendable replay buffers built in.
We provide a base :py:class:`~ray.rllib.utils.replay_buffers.replay_buffer.ReplayBuffer` class from which you can build your own buffer.
In most algorithms, we require :py:class:`~ray.rllib.utils.replay_buffers.multi_agent_replay_buffer.MultiAgentReplayBuffer`\s.
This is because we want them to generalize to the the multi-agent case. Therefore, these buffer's ``add()`` and ``sample()`` methods require a ``policy_id`` to handle experiences per policy.
Have a look at the :py:class:`~ray.rllib.utils.replay_buffers.multi_agent_replay_buffer.MultiAgentReplayBuffer` to get a sense of how it extends our base class.
You can find buffer types and arguments to modify their behaviour as part of RLlib's default parameters. They are part of
the ``replay_buffer_config``.

Basic Usage
-----------

You will rarely have to define your own replay buffer sub-class, when running an experiment, but rather configure existing buffers.
The following is `from RLlib's examples section <https://github.com/ray-project/ray/blob/master/rllib/examples/replay_buffer_api.py>`__:  and runs the R2D2 algorithm with `PER <https://arxiv.org/abs/1511.05952>`__ (which by default it doesn't).
The highlighted lines focus on the PER configuration.

.. dropdown:: **Executable example script**
    :animate: fade-in-slide-down

    .. literalinclude:: ../../../../rllib/examples/replay_buffer_api.py
        :emphasize-lines: 62, 63, 64, 65, 66, 67, 68, 69, 70, 71, 72, 73, 74, 75, 76, 77
        :language: python
        :start-after: __sphinx_doc_replay_buffer_api_example_script_begin__
        :end-before: __sphinx_doc_replay_buffer_api_example_script_end__

.. tip:: Because of its prevalence, most Q-learning algorithms support PER. The priority update step that is needed is embedded into their training iteration functions.
.. warning:: If your custom buffer requires extra interaction, you will have to change the training iteration function, too!


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
   * e.g. ``worker_side_prioritization`` to decide where to compute priorities

* Constructor arguments to instantiate the replay buffer
   * e.g. ``capacity`` to limit the buffer's size

* Call arguments for underlying replay buffer methods
   * e.g. ``prioritized_replay_beta`` is used by the :py:class:`~ray.rllib.utils.replay_buffers.multi_agent_prioritized_replay_buffer.MultiAgentPrioritizedReplayBuffer` to call the ``sample()`` method of every underlying :py:class:`~ray.rllib.utils.replay_buffers.prioritized_replay_buffer.PrioritizedReplayBuffer`


.. tip:: Most of the time, only 1. and 2. are of interest. 3. is an advanced feature that supports use cases where a :py:class:`~ray.rllib.utils.replay_buffers.multi_agent_replay_buffer.MultiAgentReplayBuffer` instantiates underlying buffers that need constructor or default call arguments.


ReplayBuffer Base Class
-----------------------

The base :py:class:`~ray.rllib.utils.replay_buffers.replay_buffer.ReplayBuffer` class only supports storing and replaying
experiences in different :py:class:`~ray.rllib.utils.replay_buffers.replay_buffer.StorageUnit`\s.
Advanced buffer types add functionality while trying retaining compatibility through inheritance.
The following excerpt from the class documentation only shows how to construct, sample from and add to a replay buffer.
If you plan on implementing your own buffer, these are also the main places to look at.

.. dropdown:: **Constructor, ``add()`` and ``sample()`` methods.**
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

For a full implementation, you should consider other methods like ``get_state()`` and ``set_state()``.
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
When later calling the ``sample()`` method, num_items will relate to said storage_unit.

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


.. toctree::
    :hidden:
    :maxdepth: 2

    replay_buffers/replay_buffer.rst

