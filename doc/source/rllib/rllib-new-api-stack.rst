.. include:: /_includes/rllib/we_are_hiring.rst


.. _rllib-new-api-stack-guide:


RLlib's New API Stack
=====================

Overview
--------

Starting in Ray 2.10, you can opt-in to the alpha version of a "new API stack", a fundamental overhaul from the ground up with respect to architecture,
design principles, code base, and user facing APIs. The following select algorithms and setups are available.

.. list-table::
   :header-rows: 1
   :widths: 40 40 40

   * - Feature/Algo (on new API stack)
     - **PPO**
     - **SAC**
   * - Single Agent
     - Yes
     - Yes
   * - Multi Agent
     - Yes
     - No
   * - Fully-connected (MLP)
     - Yes
     - Yes
   * - Image inputs (CNN)
     - Yes
     - No
   * - RNN support (LSTM)
     - Yes
     - No
   * - Complex inputs (flatten)
     - Yes
     - Yes


Over the next couple of months, the Ray Team will continue to test, benchmark, bug-fix, and
further polish these new APIs as well as rollout more and more algorithms that you can run in
either stack.
The goal is to reach a state where the new stack can completely replace the old one.

Keep in mind that due to its alpha nature, when using the new stack, you might run into issues and encounter instabilities.
Also, rest assured that you are able to continue using your custom classes and setups
on the old API stack for the foreseeable future (beyond Ray 3.0).


What is the New API Stack?
--------------------------

The new API stack is the result of re-writing from scratch RLlib's core APIs and reducing
its user-facing classes from more than a dozen critical ones
down to only a handful of classes. During the design of these new interfaces from the ground up,
the Ray Team strictly applied the following principles:

* Suppose a simple mental-model underlying the new APIs
* Classes must be usable outside of RLlib
* Separate concerns as much as possible. Try to answer: "**WHAT** should be done **WHEN** and by **WHOM**?"
* Offer fine-grained modularity, full interoperability, and frictionless pluggability of classes

Applying the above principles, the Ray Team reduced the important **must-know** classes
for the average RLlib user from seven on the old stack, to only four on the new stack.
The **core** new API stack classes are:

* :py:class:`~ray.rllib.core.rl_module.rl_module.RLModule` (replaces :py:class:`~ray.rllib.models.modelv2.ModelV2` and :py:class:`~ray.rllib.policy.policy_map.PolicyMap` APIs)
* :py:class:`~ray.rllib.core.learner.learner.Learner` (replaces :py:class:`~ray.rllib.evaluation.rollout_worker.RolloutWorker` and some of :py:class:`~ray.rllib.policy.policy.Policy`)
* :py:class:`~ray.rllib.env.single_agent_episode.SingleAgentEpisode` and :py:class:`~ray.rllib.env.multi_agent_episode.MultiAgentEpisode` (replaces :py:class:`~ray.rllib.policy.view_requirement.ViewRequirement`, :py:class:`~ray.rllib.evaluation.collectors.SampleCollector`, :py:class:`~ray.rllib.evaluation.episode.Episode`, and :py:class:`~ray.rllib.evaluation.episode_v2.EpisodeV2`)
* :py:class:`~ray.rllib.connector.connector_v2.ConnectorV2` (replaces :py:class:`~ray.rllib.connector.connector.Connector` and some of :py:class:`~ray.rllib.evaluation.rollout_worker.RolloutWorker` and :py:class:`~ray.rllib.policy.policy.Policy`)

The :py:class:`~ray.rllib.algorithm.algorithm_config.AlgorithmConfig` and :py:class:`~ray.rllib.algorithm.algorithm.Algorithm` APIs remain as-is. These are already established APIs on the old stack.


Who should use the new API stack?
---------------------------------

Eventually, all users of RLlib should switch over to running experiments and developing their custom classes
against the new API stack.

Right now, it's only available for a few algorithms and setups (see table above), however, if you do use
PPO (single- or multi-agent) or SAC (single-agent), you should try it.

The following section, lists some compelling reasons to migrate to the new stack.

Note these indicators against using it at this early stage:

1) You're using a custom :py:class:`~ray.rllib.models.modelv2.ModelV2` class and aren't interested right now in moving it into the new :py:class:`~ray.rllib.core.rl_module.rl_module.RLModule` API.
1) You're using a custom :py:class:`~ray.rllib.policy.policy.Policy` class (e.g., with a custom loss function and aren't interested right now in moving it into the new :py:class:`~ray.rllib.core.learner.learner.Learner` API.
1) You're using custom :py:class:`~ray.rllib.connector.connector.Connector` classes and aren't interested right now in moving them into the new :py:class:`~ray.rllib.connector.connector_v2.ConnectorV2` API.

If any of the above applies to you, don't migrate for now, and continue running with the old API stack. Migrate to the new
stack whenever you're ready to re-write some small part of your code.


Comparison to the Old API Stack
-------------------------------

This table compares features and design choices between the new and old API stack:

.. list-table::
   :header-rows: 1
   :widths: 40 40 40

   * -
     - **New API Stack**
     - **Old API Stack**
   * - Reduced code complexity (for beginners and advanced users)
     - 5 user-facing classes (`AlgorithmConfig`, `RLModule`, `Learner`, `ConnectorV2`, `Episode`)
     - 8 user-facing classes (`AlgorithmConfig`, `ModelV2`, `Policy`, `build_policy`, `Connector`, `RolloutWorker`, `BaseEnv`, `ViewRequirement`)
   * - Classes are usable outside of RLlib
     - Yes
     - Partly
   * - Separation-of-concerns design (e.g., during sampling, only action must be computed)
     - Yes
     - No
   * - Distributed/scalable sample collection
     - Yes
     - Yes
   * - Full 360° read/write access to (multi-)agent trajectories
     - Yes
     - No
   * - Multi-GPU and multi-node/multi-GPU
     - Yes
     - Yes & No
   * - Support for shared (multi-agent) model components (e.g., communication channels, shared value functions, etc.)
     - Yes
     - No
   * - Env vectorization with `gym.vector.Env`
     - Yes
     - No (RLlib's own solution)


How to Use the New API Stack?
-----------------------------

The new API stack is disabled by default for all algorithms.
To activate it for PPO (single- and multi-agent) or SAC (single-agent only),
change the following in your `AlgorithmConfig` object:

.. tab-set::

    .. tab-item:: Single Agent **PPO**

        .. literalinclude:: doc_code/new_api_stack.py
            :language: python
            :start-after: __enabling-new-api-stack-sa-ppo-begin__
            :end-before: __enabling-new-api-stack-sa-ppo-end__


    .. tab-item:: Multi Agent **PPO**

        .. literalinclude:: doc_code/new_api_stack.py
            :language: python
            :start-after: __enabling-new-api-stack-ma-ppo-begin__
            :end-before: __enabling-new-api-stack-ma-ppo-end__


    .. tab-item:: Single Agent **SAC**

        .. literalinclude:: doc_code/new_api_stack.py
            :language: python
            :start-after: __enabling-new-api-stack-sa-sac-begin__
            :end-before: __enabling-new-api-stack-sa-sac-end__
