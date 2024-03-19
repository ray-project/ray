.. include:: /_includes/rllib/we_are_hiring.rst


.. _rllib-new-api-stack-guide:


RLlib's New API Stack
=====================

Overview
--------

Since early 2022, RLlib has been undergoing a fundamental overhaul from the ground up with respect to its architecture,
design principles, code base, and user facing APIs. With Ray 2.10, we finally announce having reached alpha stability
on this "new API stack" and it is now available (via opt-in) for the following select algorithms and setups.

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


Over the next couple of months, we will continue to test, benchmark, bug-fix, and
further polish these new APIs as well as rollout more and more algorithms that can be run in
either stack.
The goal is to reach a state where the new stack can completely replace the old one.

Keep in mind that due to its alpha nature, when using the new stack, you might run into issues and encounter instabilities.
Also, rest assured that you will be able to continue using your custom classes and setups
on the old API stack for the foreseeable future (beyond Ray 3.0).


What is the New API Stack?
--------------------------

The new API stack is the result of re-writing from scratch RLlib's core APIs and reducing
its user-facing classes from more than a dozen critical ones
down to only a handful of classes. During the design of these new interfaces from the ground up,
we strictly applied the following principles:

* Suppose a simple mental-model underlying the new APIs
* Classes must be usable outside of RLlib
* Separate concerns as much as possible (in other words: Try to answer: "**WHAT** should be done **WHEN** and by **WHOM**?")
* Offer finegrained modularity, full interoperability, and friction-less pluggability of classes

Applying these principles above, we were able to reduce the important **must-know** classes
for the average RLlib user from seven (on the old stack) to only four (on the new stack).
Those **core** new API stack classes are:

* :py:class:`~ray.rllib.core.rl_module.rl_module.RLModule` (replaces :py:class:`~ray.rllib.models.modelv2.ModelV2` and :py:class:`~ray.rllib.policy.policy_map.PolicyMap` APIs)
* :py:class:`~ray.rllib.core.learner.learner.Learner` (replaces :py:class:`~ray.rllib.evaluation.rollout_worker.RolloutWorker` and some of :py:class:`~ray.rllib.policy.policy.Policy`)
* :py:class:`~ray.rllib.env.single_agent_episode.SingleAgentEpisode` and :py:class:`~ray.rllib.env.multi_agent_episode.MultiAgentEpisode` (replaces :py:class:`~ray.rllib.policy.view_requirement.ViewRequirement`, :py:class:`~ray.rllib.evaluation.collectors.SampleCollector`, :py:class:`~ray.rllib.evaluation.episode.Episode`, and :py:class:`~ray.rllib.evaluation.episode_v2.EpisodeV2`)
* :py:class:`~ray.rllib.connector.connector_v2.ConnectorV2` (replaces :py:class:`~ray.rllib.connector.connector.Connector` and some of :py:class:`~ray.rllib.evaluation.rollout_worker.RolloutWorker` and :py:class:`~ray.rllib.policy.policy.Policy`)

The :py:class:`~ray.rllib.algorithm.algorithm_config.AlgorithmConfig` and :py:class:`~ray.rllib.algorithm.algorithm.Algorithm` APIs remain as-is (these are already established APIs on the old stack).


Who should use the new API stack?
---------------------------------

Eventually, all users of RLlib should switch over to running experiments and developing their custom classes
against the new API stack.

Right now, it is only available for a few algorithms and setups (see table above), however, if you do use
PPO (single- or multi-agent) or SAC (single-agent), you should give it a shot.

In the following section, we'll list some very good reasons to cut over.

There are also a few indicators against using it at this early stage:

1) You are using a custom :py:class:`~ray.rllib.models.modelv2.ModelV2` class and are not interested right now in moving it into the new :py:class:`~ray.rllib.core.rl_module.rl_module.RLModule` API.
1) You are using a custom :py:class:`~ray.rllib.policy.policy.Policy` class (e.g. with a custom loss function and are not interested right now in moving it into the new :py:class:`~ray.rllib.core.learner.learner.Learner` API.
1) You are using custom :py:class:`~ray.rllib.connector.connector.Connector` classes and are not interested right now in moving them into the new :py:class:`~ray.rllib.connector.connector_v2.ConnectorV2` API.

If any of the above applies to you, simply stay put for now, continue running with the old API stack, and cut over to the new
stack whenever you feel ready to re-write some (small) part of your code.


Comparison to the Old API Stack
-------------------------------

Here is a quick comparison table listing features and design choices from the new- vs the old API stack:

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
   * - Separation-of-concerns design (e.g. during sampling, only action must be computed)
     - Yes
     - No
   * - Distributed/scalable sample collection
     - Yes
     - Yes
   * - Full 360° read/write access to (multi-)agent trajectories
     - Yes
     - No
   * - Multi-GPU & multi-node/multi-GPU
     - Yes
     - Yes & No
   * - Support for shared (multi-agent) model components (e.g. communication channels, shared value functions, etc..)
     - Yes
     - No
   * - Env vectorization via `gym.vector.Env`
     - Yes
     - No (RLlib's own solution)


How to Use the New API Stack?
-----------------------------

The new API stack is disabled by default for all algorithms.
If you want to activate it for PPO (single- and multi-agent) or SAC (single-agent only),
you should make the following simple changes in your `AlgorithmConfig` object:

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
