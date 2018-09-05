RLlib: Scalable Reinforcement Learning
======================================

RLlib is an open-source library for reinforcement learning that offers both a collection of reference algorithms and scalable primitives for composing new ones.

.. image:: rllib-stack.svg

Learn more about RLlib's design by reading the `ICML paper <https://arxiv.org/abs/1712.09381>`__.

Installation
------------

RLlib has extra dependencies on top of ``ray``. First, you'll need to install either `PyTorch <http://pytorch.org/>`__ or `TensorFlow <https://www.tensorflow.org>`__. Then, install the Ray RLlib module:

.. code-block:: bash

  pip install tensorflow  # or tensorflow-gpu
  pip install ray[rllib]

You might also want to clone the Ray repo for convenient access to RLlib helper scripts:

.. code-block:: bash

  git clone https://github.com/ray-project/ray
  cd ray/python/ray/rllib

Training APIs
-------------
* `Command-line <rllib-training.html>`__
* `Python API <rllib-training.html#python-api>`__
* `REST API <rllib-training.html#rest-api>`__

Environments
------------
* `RLlib Environments Overview <rllib-env.html>`__
* `OpenAI Gym <rllib-env.html#openai-gym>`__
* `Vectorized <rllib-env.html#vectorized>`__
* `Multi-Agent <rllib-env.html#multi-agent>`__
* `Agent-Driven <rllib-env.html#agent-driven>`__
* `Offline Data Ingest <rllib-env.html#offline-data>`__ 
* `Batch Asynchronous <rllib-env.html#batch-asynchronous>`__

Algorithms
----------

*  High-throughput architectures

   -  `Distributed Prioritized Experience Replay (Ape-X) <rllib-algorithms.html#distributed-prioritized-experience-replay-ape-x>`__

   -  `Importance Weighted Actor-Learner Architecture (IMPALA) <rllib-algorithms.html#importance-weighted-actor-learner-architecture-impala>`__

*  Gradient-based

   -  `Advantage Actor-Critic (A2C, A3C) <rllib-algorithms.html#advantage-actor-critic-a2c-a3c>`__

   -  `Deep Deterministic Policy Gradients (DDPG) <rllib-algorithms.html#deep-deterministic-policy-gradients-ddpg>`__

   -  `Deep Q Networks (DQN, Rainbow) <rllib-algorithms.html#deep-q-networks-dqn-rainbow>`__

   -  `Policy Gradients <rllib-algorithms.html#policy-gradients>`__

   -  `Proximal Policy Optimization (PPO) <rllib-algorithms.html#proximal-policy-optimization-ppo>`__

*  Derivative-free

   -  `Augmented Random Search (ARS) <rllib-algorithms.html#augmented-random-search-ars>`__

   -  `Evolution Strategies <rllib-algorithms.html#evolution-strategies>`__

Models and Preprocessors
------------------------
* `RLlib Models and Preprocessors Overview <rllib-models.html>`__
* `Built-in Models and Preprocessors <rllib-models.html#built-in-models-and-preprocessors>`__
* `Custom Models <rllib-models.html#custom-models>`__
* `Custom Preprocessors <rllib-models.html#custom-preprocessors>`__
* `Customizing Policy Graphs <rllib-models.html#customizing-policy-graphs>`__
* `Model-Based Rollouts <rllib-models.html#model-based-rollouts>`__

RLlib Concepts
--------------
* `Policy Graphs <rllib-concepts.html>`__
* `Policy Evaluation <rllib-concepts.html#policy-evaluation>`__
* `Policy Optimization <rllib-concepts.html#policy-optimization>`__

Package Reference
-----------------
* `ray.rllib.agents <rllib-package-ref.html#module-ray.rllib.agents>`__
* `ray.rllib.env <rllib-package-ref.html#module-ray.rllib.env>`__
* `ray.rllib.evaluation <rllib-package-ref.html#module-ray.rllib.evaluation>`__
* `ray.rllib.models <rllib-package-ref.html#module-ray.rllib.models>`__
* `ray.rllib.optimizers <rllib-package-ref.html#module-ray.rllib.optimizers>`__
* `ray.rllib.utils <rllib-package-ref.html#module-ray.rllib.utils>`__

Troubleshooting
---------------

If you encounter errors like
`blas_thread_init: pthread_create: Resource temporarily unavailable` when using many workers,
try setting ``OMP_NUM_THREADS=1``. Similarly, check configured system limits with
`ulimit -a` for other resource limit errors.
