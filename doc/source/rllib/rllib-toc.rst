.. include:: we_are_hiring.rst

RLlib Table of Contents
=======================

RLlib Core Concepts
-------------------

*  `Policies <rllib/core-concepts.html#policies>`__

*  `Sample Batches <rllib/core-concepts.html#sample-batches>`__

*  `Training <rllib/core-concepts.html#training>`__

Training APIs
-------------
*  `Command-line <rllib-training.html>`__

   -  `Evaluating Trained Policies <rllib-training.html#evaluating-trained-policies>`__

*  `Configuration <rllib-training.html#configuration>`__

   -  `Specifying Parameters <rllib-training.html#specifying-parameters>`__

   -  `Specifying Resources <rllib-training.html#specifying-resources>`__

   -  `Common Parameters <rllib-training.html#common-parameters>`__

   -  `Scaling Guide <rllib-training.html#scaling-guide>`__

   -  `Tuned Examples <rllib-training.html#tuned-examples>`__

*  `Basic Python API <rllib-training.html#basic-python-api>`__

   -  `Computing Actions <rllib-training.html#computing-actions>`__

   -  `Accessing Policy State <rllib-training.html#accessing-policy-state>`__

   -  `Accessing Model State <rllib-training.html#accessing-model-state>`__

*  `Advanced Python APIs <rllib-training.html#advanced-python-apis>`__

   -  `Custom Training Workflows <rllib-training.html#custom-training-workflows>`__

   -  `Global Coordination <rllib-training.html#global-coordination>`__

   -  `Callbacks and Custom Metrics <rllib-training.html#callbacks-and-custom-metrics>`__

   -  `Customizing Exploration Behavior <rllib-training.html#customizing-exploration-behavior>`__

   -  `Customized Evaluation During Training <rllib-training.html#customized-evaluation-during-training>`__

   -  `Rewriting Trajectories <rllib-training.html#rewriting-trajectories>`__

   -  `Curriculum Learning <rllib-training.html#curriculum-learning>`__

*  `Debugging <rllib-training.html#debugging>`__

   -  `Gym Monitor <rllib-training.html#gym-monitor>`__

   -  `Eager Mode <rllib-training.html#eager-mode>`__

   -  `Episode Traces <rllib-training.html#episode-traces>`__

   -  `Log Verbosity <rllib-training.html#log-verbosity>`__

   -  `Stack Traces <rllib-training.html#stack-traces>`__

*  `External Application API <rllib-training.html#external-application-api>`__

Environments
------------
*  `RLlib Environments Overview <rllib-env.html>`__
*  `OpenAI Gym <rllib-env.html#openai-gym>`__
*  `Vectorized <rllib-env.html#vectorized>`__
*  `Multi-Agent and Hierarchical <rllib-env.html#multi-agent-and-hierarchical>`__
*  `External Agents and Applications <rllib-env.html#external-agents-and-applications>`__

   -  `External Application Clients <rllib-env.html#external-application-clients>`__

*  `Advanced Integrations <rllib-env.html#advanced-integrations>`__

Models, Preprocessors, and Action Distributions
-----------------------------------------------
*  `RLlib Models, Preprocessors, and Action Distributions Overview <rllib-models.html>`__
*  `TensorFlow Models <rllib-models.html#tensorflow-models>`__
*  `PyTorch Models <rllib-models.html#pytorch-models>`__
*  `Custom Preprocessors <rllib-models.html#custom-preprocessors>`__
*  `Custom Action Distributions <rllib-models.html#custom-action-distributions>`__
*  `Supervised Model Losses <rllib-models.html#supervised-model-losses>`__
*  `Self-Supervised Model Losses <rllib-models.html#self-supervised-model-losses>`__
*  `Variable-length / Complex Observation Spaces <rllib-models.html#variable-length-complex-observation-spaces>`__
*  `Variable-length / Parametric Action Spaces <rllib-models.html#variable-length-parametric-action-spaces>`__
*  `Autoregressive Action Distributions <rllib-models.html#autoregressive-action-distributions>`__

Algorithms
----------

*  High-throughput architectures

   -  |pytorch| |tensorflow| :ref:`Distributed Prioritized Experience Replay (Ape-X) <apex>`

   -  |pytorch| |tensorflow| :ref:`Importance Weighted Actor-Learner Architecture (IMPALA) <impala>`

   -  |pytorch| |tensorflow| :ref:`Asynchronous Proximal Policy Optimization (APPO) <appo>`

   -  |pytorch| :ref:`Decentralized Distributed Proximal Policy Optimization (DD-PPO) <ddppo>`

*  Gradient-based

   -  |pytorch| |tensorflow| :ref:`Advantage Actor-Critic (A2C, A3C) <a3c>`

   -  |pytorch| |tensorflow| :ref:`Deep Deterministic Policy Gradients (DDPG, TD3) <ddpg>`

   -  |pytorch| |tensorflow| :ref:`Deep Q Networks (DQN, Rainbow, Parametric DQN) <dqn>`

   -  |pytorch| |tensorflow| :ref:`Policy Gradients <pg>`

   -  |pytorch| |tensorflow| :ref:`Proximal Policy Optimization (PPO) <ppo>`

   -  |pytorch| |tensorflow| :ref:`Soft Actor Critic (SAC) <sac>`

   -  |pytorch| :ref:`Slate Q-Learning (SlateQ) <slateq>`

*  Derivative-free

   -  |pytorch| |tensorflow| :ref:`Augmented Random Search (ARS) <ars>`

   -  |pytorch| |tensorflow| :ref:`Evolution Strategies <es>`

*  Model-based / Meta-learning / Offline

   -  |pytorch| :ref:`Single-Player AlphaZero (contrib/AlphaZero) <alphazero>`

   -  |pytorch| |tensorflow| :ref:`Model-Agnostic Meta-Learning (MAML) <maml>`

   -  |pytorch| :ref:`Model-Based Meta-Policy-Optimization (MBMPO) <mbmpo>`

   -  |pytorch| :ref:`Dreamer (DREAMER) <dreamer>`

   -  |pytorch| :ref:`Conservative Q-Learning (CQL) <cql>`

*  Multi-agent

   -  |pytorch| :ref:`QMIX Monotonic Value Factorisation (QMIX, VDN, IQN) <qmix>`
   -  |tensorflow| :ref:`Multi-Agent Deep Deterministic Policy Gradient (contrib/MADDPG) <maddpg>`

*  Offline

   -  |pytorch| |tensorflow| :ref:`Advantage Re-Weighted Imitation Learning (MARWIL) <marwil>`

*  Contextual bandits

   -  |pytorch| :ref:`Linear Upper Confidence Bound (contrib/LinUCB) <linucb>`
   -  |pytorch| :ref:`Linear Thompson Sampling (contrib/LinTS) <lints>`

*  Exploration-based plug-ins (can be combined with any algo)

   -  |pytorch| :ref:`Curiosity (ICM: Intrinsic Curiosity Module) <curiosity>`

Sample Collection
-----------------
*  `The SampleCollector Class is Used to Store and Retrieve Temporary Data <rllib-sample-collection.html#the-samplecollector-class-is-used-to-store-and-retrieve-temporary-data>`__
*  `Trajectory View API <rllib-sample-collection.html#trajectory-view-api>`__


Offline Datasets
----------------
*  `Working with Offline Datasets <rllib-offline.html>`__
*  `Input Pipeline for Supervised Losses <rllib-offline.html#input-pipeline-for-supervised-losses>`__
*  `Input API <rllib-offline.html#input-api>`__
*  `Output API <rllib-offline.html#output-api>`__

Concepts and Custom Algorithms
------------------------------
*  `Policies <rllib-concepts.html>`__

   -  `Policies in Multi-Agent <rllib-concepts.html#policies-in-multi-agent>`__

   -  `Building Policies in TensorFlow <rllib-concepts.html#building-policies-in-tensorflow>`__

   -  `Building Policies in TensorFlow Eager <rllib-concepts.html#building-policies-in-tensorflow-eager>`__

   -  `Building Policies in PyTorch <rllib-concepts.html#building-policies-in-pytorch>`__

   -  `Extending Existing Policies <rllib-concepts.html#extending-existing-policies>`__

*  `Policy Evaluation <rllib-concepts.html#policy-evaluation>`__
*  `Execution Plans <rllib-concepts.html#execution-plans>`__
*  `Trainers <rllib-concepts.html#trainers>`__

Examples
--------

*  `Tuned Examples <rllib-examples.html#tuned-examples>`__
*  `Training Workflows <rllib-examples.html#training-workflows>`__
*  `Custom Envs and Models <rllib-examples.html#custom-envs-and-models>`__
*  `Serving and Offline <rllib-examples.html#serving-and-offline>`__
*  `Multi-Agent and Hierarchical <rllib-examples.html#multi-agent-and-hierarchical>`__
*  `Community Examples <rllib-examples.html#community-examples>`__

Development
-----------

*  `Development Install <rllib-dev.html#development-install>`__
*  `API Stability <rllib-dev.html#api-stability>`__
*  `Features <rllib-dev.html#feature-development>`__
*  `Benchmarks <rllib-dev.html#benchmarks>`__
*  `Contributing Algorithms <rllib-dev.html#contributing-algorithms>`__

Package Reference
-----------------
*  `ray.rllib.agents <rllib-package-ref.html#module-ray.rllib.agents>`__
*  `ray.rllib.env <rllib-package-ref.html#module-ray.rllib.env>`__
*  `ray.rllib.evaluation <rllib-package-ref.html#module-ray.rllib.evaluation>`__
*  `ray.rllib.execution <rllib-package-ref.html#module-ray.rllib.execution>`__
*  `ray.rllib.models <rllib-package-ref.html#module-ray.rllib.models>`__
*  `ray.rllib.utils <rllib-package-ref.html#module-ray.rllib.utils>`__

Troubleshooting
---------------

If you encounter errors like
`blas_thread_init: pthread_create: Resource temporarily unavailable` when using many workers,
try setting ``OMP_NUM_THREADS=1``. Similarly, check configured system limits with
`ulimit -a` for other resource limit errors.

For debugging unexpected hangs or performance problems, you can run ``ray stack`` to dump
the stack traces of all Ray workers on the current node, ``ray timeline`` to dump
a timeline visualization of tasks to a file, and ``ray memory`` to list all object
references in the cluster.

TensorFlow 2.0
~~~~~~~~~~~~~~

RLlib supports both tf2.x as well as ``tf.compat.v1`` modes.
Always use the ``ray.rllib.utils.framework.try_import_tf()`` utility function to import tensorflow.
It returns three values:
*  ``tf1``: The ``tf.compat.v1`` module or the installed tf1.x package (if the version is < 2.0).
*  ``tf``: The installed tensorflow module as-is.
*  ``tfv``: A convenience version int, whose values are either 1 or 2.

`See here <https://github.com/ray-project/ray/blob/master/rllib/examples/eager_execution.py>`__ for a detailed example script.

.. |tensorflow| image:: images/tensorflow.png
    :class: inline-figure
    :width: 16

.. |pytorch| image:: images/pytorch.png
    :class: inline-figure
    :width: 16
