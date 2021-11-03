.. _env-docs:

Environment APIs
================

.. toctree::
   :maxdepth: 2

   env/base_env.rst
   env/multi_agent_env.rst
   env/vector_env.rst
   env/external_env.rst

Any environment type provided by you to RLlib (e.g. a user-defined ``gym.Env`` class),
is converted internally into the ``BaseEnv`` API, whose main methods are ``poll()`` and ``send_actions()``:

.. https://docs.google.com/drawings/d/1NtbVk-Mo89liTRx-sHu_7fqi3Kn7Hjdf3i6jIMbxGlY/edit
.. image:: ../../images/rllib/env_classes_overview.svg



This API allows ``BaseEnv`` to support:

1) Vectorization of sub-environments (i.e. individual gym.Env instances, stacked to form a vector of envs) in order to batch action-computing model forward passes.
2) External simulators requiring async execution (e.g. Envs that run on separate machines and independently request actions from a policy server).
3) Stepping through the vectorized sub-environments in parallel by converting all sub-environments into @ray.remote actors.
4) Multi-agent RL via dicts mapping agent IDs to observations/rewards/etc..

For example, if you provide a custom ``gym.Env`` class to RLlib, auto-conversion to ``BaseEnv`` goes as follows:

- User provides a ``gym.Env`` -> ``_VectorizedGymEnv`` (is-a ``VectorEnv``) -> ``BaseEnv``

Here is a simple example:

.. literalinclude:: ../../../../rllib/examples/documentation/custom_gym_env.py
   :language: python

..   start-after: __sphinx_doc_model_construct_1_begin__
..   end-before: __sphinx_doc_model_construct_1_end__

However, you may also conveniently sub-class any of the other supported RLlib-specific
environment types. The automated paths from those env types (or callables returning instances of those types) to
an RLlib ``BaseEnv`` is as follows:

- User provides a custom ``MultiAgentEnv`` (is-a ``gym.Env``) -> ``VectorEnv`` -> ``BaseEnv``
- User uses a policy client (via an external simulator) -> ``ExternalEnv|ExternalMultiAgentEnv`` -> ``BaseEnv``
- User provides a custom ``VectorEnv`` -> ``BaseEnv``
- User provides a custom ``BaseEnv`` -> do nothing
