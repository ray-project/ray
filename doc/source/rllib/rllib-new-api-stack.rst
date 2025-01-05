.. include:: /_includes/rllib/we_are_hiring.rst


.. _rllib-new-api-stack-guide:


RLlib's new API stack
=====================

.. hint::

    This section describes the new API stack and why you should migrate to it
    if you have old API stack custom code. See the :ref:`migration guide <rllib-new-api-stack-migration-guide>` for details.


Overview
--------

Starting in Ray 2.10, you can opt-in to the alpha version of the "new API stack", a fundamental overhaul from the ground
up with respect to architecture, design principles, code base, and user facing APIs.
The following select algorithms and setups are available.

.. list-table::
   :header-rows: 1
   :widths: 25 25 25 25 25 25

   * - Feature/Algo (on new API stack)
     - **APPO**
     - **DQN**
     - **IMPALA**
     - **PPO**
     - **SAC**
   * - Single- and Multi-Agent
     - Yes
     - Yes
     - Yes
     - Yes
     - Yes
   * - Fully-connected (MLP)
     - Yes
     - Yes
     - Yes
     - Yes
     - Yes
   * - Image inputs (CNN)
     - Yes
     - Yes
     - Yes
     - Yes
     - Yes
   * - RNN support (LSTM)
     - Yes
     - No
     - Yes
     - Yes
     - No
   * - Complex inputs (flatten)
     - Yes
     - Yes
     - Yes
     - Yes
     - Yes


Over the next few months, the RLlib Team continues to document, test, benchmark, bug-fix, and
further polish these new APIs as well as rollout more algorithms
that you can run in the new stack, with a focus on offline RL.

You can continue using custom classes and setups
on the old API stack for the foreseeable future, beyond Ray 3.0. However, you should
migrate to the new stack with the :ref:`migration guide <rllib-new-api-stack-migration-guide>`


New API stack
--------------------------

The new API stack is the result of re-writing RLlib's core APIs from scratch and reducing
its user-facing classes from more than a dozen critical ones down to only a handful
of classes, without any loss of functionaliy. During the design of these new interfaces,
the Ray Team strictly applied the following principles:

* Classes must be usable outside of RLlib
* Separate concerns as much as possible. Try to answer: "**WHAT** should be done **WHEN** and by **WHOM**?"
* Offer fine-grained modularity, full interoperability, and frictionless pluggability of classes

Applying the above principles, the Ray Team reduced the important **must-know** classes
for the average RLlib user from seven on the old stack, to only four on the new stack.
The **core** new API stack classes are:

* :py:class:`~ray.rllib.core.rl_module.rl_module.RLModule` (replaces ``ModelV2`` and ``PolicyMap`` APIs)
* :py:class:`~ray.rllib.core.learner.learner.Learner` (replaces ``RolloutWorker`` and some of ``Policy``)
* :py:class:`~ray.rllib.env.single_agent_episode.SingleAgentEpisode` and :py:class:`~ray.rllib.env.multi_agent_episode.MultiAgentEpisode` (replaces ``ViewRequirement``, ``SampleCollector``, ``Episode``, and ``EpisodeV2``)
* :py:class:`~ray.rllib.connector.connector_v2.ConnectorV2` (replaces ``Connector`` and some of ``RolloutWorker`` and ``Policy``)

The :py:class:`~ray.rllib.algorithm.algorithm_config.AlgorithmConfig` and :py:class:`~ray.rllib.algorithm.algorithm.Algorithm` APIs remain as-is. These are already established APIs on the old stack.


Who should use the new API stack?
---------------------------------

Migrate your code from the old to new API stack as soon as possible.
The classes and APIs are sufficiently stable. The Ray team expects very minor changes.

See the :ref:`New API stack migration guide <rllib-new-api-stack-migration-guide>` for a comprehensive migration guide with step-by-step instructions on translating your code from the
old to new API stack.

A comparison of the old to new API stack provides additional motivation for migrating to the new stack.


Comparison to the old API stack
-------------------------------

This table compares features and design choices between the new and old API stack:

.. list-table::
   :header-rows: 1
   :widths: 40 40 40

   * -
     - **New API Stack**
     - **Old API Stack**
   * - Multi-GPU and multi-node/multi-GPU
     - Yes
     - Yes and No
   * - Support for shared (multi-agent) model components (e.g., communication channels, shared value functions, etc.)
     - Yes
     - No
   * - Reduced code complexity (for beginners and advanced users)
     - 5 user-facing classes (`AlgorithmConfig`, `RLModule`, `Learner`, `ConnectorV2`, `Episode`)
     - 8 user-facing classes (`AlgorithmConfig`, `ModelV2`, `Policy`, `build_policy`, `Connector`, `RolloutWorker`, `BaseEnv`, `ViewRequirement`)
   * - Classes are usable outside of RLlib
     - Yes
     - Partly
   * - Strict separation-of-concerns design
     - Yes
     - No
   * - Distributed/scalable sample collection
     - Yes
     - Yes
   * - Full 360° read/write access to (multi-)agent trajectories
     - Yes
     - No
   * - Env vectorization with `gym.vector.Env`
     - Yes
     - No (RLlib's own solution)


How to Use the New API Stack?
-----------------------------

See :ref:`New API stack migration guide <rllib-new-api-stack-migration-guide>` for a complete and comprehensive migration guide
with detailed steps and changes to apply to your
custom RLlib classes to migrate from the old to the new stack.
