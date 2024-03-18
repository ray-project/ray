.. include:: /_includes/rllib/we_are_hiring.rst


.. _rllib-new-api-stack-guide:


RLlib's New API Stack
=====================

Since early 2022, RLlib has been undergoing a fundamental overhaul from the ground up with respect to its architecture,
design principles, code base, and user facing APIs. With Ray 2.10, we finally announce having reached alpha stability
for those new APIs and this "new API stack" is now available for the following algorithms and setups.


.. list-table::
   :header-rows: 1
   :widths: 40 40 40

   * - Algorithm
     - Single Agent
     - Multi Agent
     - Fully-connected (MLP)
     - Image inputs (CNN)
     - RNN support (LSTM)
     - Complex inputs (flatten)
   * - **PPO**
     - Yes
     - Yes
     - Yes
     - Yes
     - Yes
     - Yes
   * - **SAC**
     - Yes
     - No
     - Yes
     - No
     - No
     - Yes


Keep in mind that we continue to test, benchmark, bug-fix, and further polish these new APIs
and that when using the new API stack, you might run into issues and encounter instabilities.


What is the New API Stack?
--------------------------

In a nutshell, the new API stack is the result of reducing RLlib's APIs from more than
a dozen critical classes to only a handful. When designing the new interfaces, we strictly
applied the following design principles:

* Suppose a simpler mental-model underlying an API
* Usability of classes outside of RLlib
* Separation of concerns (better answering: "What should be done when and by whom?")
* Offer better modularity, interoperability, and pluggability of classes

Applying these principles above, we were able to reduce the important classes an average
user of RLlib should be familiar with from 7 (old stack) to only 4 (new stack):

TODO: add class ref links
* RLModule (replaces ModelV2 and PolicyMap APIs)
* Learner (replaces Policy and some of RolloutWorker)
* SingleAgent- and MultiAgentEpisode (replaces TrajectoryViews, SampleCollector, Episode, and EpisodeV2)
* ConnectorV2 (replaces Connector and some of RolloutWorker & Policy)

The `AlgorithmConfig` API remains as-is (already an established API in the old stack).


Comparison to the Old API Stack
-------------------------------

Here is a quick comparison table listing features and design choices from the new- vs the old API stack:

.. list-table::
   :header-rows: 1
   :widths: 40 40 40

   * - Features / Design Choices
     - Reduced code complexity (for beginners and advanced users)
     - Classes are usable outside of RLlib
     - Separation-of-concerns design (e.g. during sampling, only action must be computed)
     - Distributed/scalable sample collection
     - Full 360° read/write access to (multi-)agent trajectories
     - Multi-GPU & multi-node/multi-GPU
     - Support for shared (multi-agent) model components (e.g. communication channels, shared value functions, etc..)
     - Env vectorization via `gym.vector.Env`
   * - **New API Stack**
     - 4 user-facing classes (AlgorithmConfig, RLModule, Learner, ConnectorV2, Episode)
     - Yes
     - Yes
     - Yes
     - Yes
     - Yes
     - Yes
     - Yes
   * - **Old API Stack**
     - 7 user-facing classes (AlgorithmConfig, ModelV2, Policy, build_policy, Connector, BaseEnv, ViewRequirement)
     - Partly
     - No
     - Yes
     - No
     - Yes & No
     - No
     - No (RLlib's own solution)


How to Use the New API Stack?
-----------------------------

TODO: Simple HOWTO with links to example scripts.

