.. _env-reference-docs:

Replay Buffers
============

RLlib comes with a set of extendable replay buffers which are used mostly in Q-Learning algorithms.
The base :py:class:`~ray.rllib.utils.replay_buffers.replay_buffer.ReplayBuffer` class (link) only supports storing experiences in different storage units (link).
Advanced types add functionality while retaining compatibility through inheritance.
You can find buffer types and arguments to modify their behaviour as part of RLlib's default parameters. They are part of
the replay buffer config (link).

Basic Usage
----

Here is a basic example of running the R2D2 algorithm, which runs without prioritized replay by default, with prioritzed replay:

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

1) Parameters that define how algorithms interact with replay buffers
 * e.g. "worker_side_prioritization" to decide where to compute priorities
2) Constructor arguments to instantiate the replay buffer
 * e.g. "capacity" to limit the buffers size
3) Call arguments for underlying replay buffer methods
 * e.g. "prioritized_replay_beta" is used by the :py:class:`~ray.rllib.utils.replay_buffers.multi_agent_prioritized_replay_buffer.PrioritizedMultiAgentReplayBuffer` to call the sample method of every underlying :py:class:`~ray.rllib.utils.replay_buffers.prioritized_replay_buffer.PrioritizedReplayBuffer`

Most of the time, only 1. and 2. are of interest.
3. is an advanced feature that supports use cases where a :py:class:`~ray.rllib.utils.replay_buffers.multi_agent_replay_buffer.MultiAgentReplayBuffer` instantiates underlying buffers that need constructor or default call arguments.

Here is an example of how to implement your own toy example of a ReplayBuffer class and make SimpleQ use it:

.. literalinclude:: ../../../../rllib/examples/documentation/replay_buffer_demo.py
   :language: python

..   start-after: __sphinx_doc_replay_buffer_own_buffer__begin__
..   end-before: __sphinx_doc_replay_buffer_own_buffer__begin__

Advanced Usage
----

In RLlib, all replay buffers implement the :py:class:`~ray.rllib.utils.replay_buffers.replay_buffer.ReplayBuffer` interface.
Therefore, they support, whenever possible, different :py:class:`~ray.rllib.utils.replay_buffers.replay_buffer.StorageUnit`s.
The storage_unit constructor argument of a replay buffer defines how sequences are stored, and therefore the unit in which they are sampled.
When later calling the sample() method, num_units will relate to said storage_unit.

Here is an example

The :py:class:`~ray.rllib.env.base_env.BaseEnv` API allows RLlib to support:

1) Vectorization of sub-environments (i.e. individual `gym.Env <https://github.com/openai/gym>`_ instances, stacked to form a vector of envs) in order to batch the action computing model forward passes.
2) External simulators requiring async execution (e.g. envs that run on separate machines and independently request actions from a policy server).
3) Stepping through the individual sub-environments in parallel via pre-converting them into separate `@ray.remote` actors.
4) Multi-agent RL via dicts mapping agent IDs to observations/rewards/etc..

For example, if you provide a custom `gym.Env <https://github.com/openai/gym>`_ class to RLlib, auto-conversion to :py:class:`~ray.rllib.env.base_env.BaseEnv` goes as follows:

- User provides a `gym.Env <https://github.com/openai/gym>`_ -> :py:class:`~ray.rllib.env.vector_env._VectorizedGymEnv` (is-a :py:class:`~ray.rllib.env.vector_env.VectorEnv`) -> :py:class:`~ray.rllib.env.base_env.BaseEnv`

Here is a simple example:

.. literalinclude:: ../../../../rllib/examples/documentation/custom_gym_env.py
   :language: python

..   start-after: __sphinx_doc_model_construct_1_begin__
..   end-before: __sphinx_doc_model_construct_1_end__

However, you may also conveniently sub-class any of the other supported RLlib-specific
environment types. The automated paths from those env types (or callables returning instances of those types) to
an RLlib :py:class:`~ray.rllib.env.base_env.BaseEnv` is as follows:

- User provides a custom :py:class:`~ray.rllib.env.multi_agent_env.MultiAgentEnv` (is-a `gym.Env <https://github.com/openai/gym>`_) -> :py:class:`~ray.rllib.env.vector_env.VectorEnv` -> :py:class:`~ray.rllib.env.base_env.BaseEnv`
- User uses a policy client (via an external simulator) -> :py:class:`~ray.rllib.env.external_env.ExternalEnv` | :py:class:`~ray.rllib.env.external_multi_agent_env.ExternalMultiAgentEnv` -> :py:class:`~ray.rllib.env.base_env.BaseEnv`
- User provides a custom :py:class:`~ray.rllib.env.vector_env.VectorEnv` -> :py:class:`~ray.rllib.env.base_env.BaseEnv`
- User provides a custom :py:class:`~ray.rllib.env.base_env.BaseEnv` -> do nothing


Replay Buffers API Reference
-------------------------

.. toctree::
   :maxdepth: 1

   replaybuffers/replay_buffer.rst
   replaybuffers/multi_agent_env.rst
   replaybuffers/vector_env.rst
   replaybuffers/external_env.rst

