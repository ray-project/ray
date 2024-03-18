.. include:: /_includes/rllib/we_are_hiring.rst


.. _rllib-new-api-stack-guide:


RLlib's New API Stack
=====================

Overview
--------

Since early 2022, RLlib has been undergoing a fundamental overhaul from the ground up with respect to its architecture,
design principles, code base, and user facing APIs. With Ray 2.10, we finally announce having reached alpha stability
on this "new API stack" and it is now available for the following select algorithms and setups.

.. list-table::
   :header-rows: 1
   :widths: 40 40 40

   * - Feature/Algo
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

In a nutshell, the new API stack is the result of re-writing from scratch
RLlib's core APIs and reducing its user-facing classes from more than a dozen critical one
to only a handful. When designing these new interfaces from the ground up, we strictly
applied the following design principles:

* Always suppose a simple mental-model underlying a new API
* Classes must be usable outside of RLlib
* Always separate concerns as much as possible (in other word, always try to clearly answer: "WHAT should be done WHEN and by WHOM?")
* Offer more finegrained modularity, better interoperability, and friction-less pluggability of classes

Applying these principles above, we were able to reduce the important "must-know" classes
for the average RLlib user from 7 (old stack) to only 4 (new stack):

* RLModule (replaces ModelV2 and PolicyMap APIs)
* Learner (replaces Policy and some of RolloutWorker)
* SingleAgentEpisode and MultiAgentEpisode (replaces TrajectoryViews, SampleCollector, Episode, and EpisodeV2)
* ConnectorV2 (replaces Connector and some of RolloutWorker and Policy)

The `AlgorithmConfig` and `Algorithm` APIs remain as-is (these are already established APIs on the old stack).


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
     - 4 user-facing classes (AlgorithmConfig, RLModule, Learner, ConnectorV2, Episode)
     - 7 user-facing classes (AlgorithmConfig, ModelV2, Policy, build_policy, Connector, BaseEnv, ViewRequirement)
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

In order

