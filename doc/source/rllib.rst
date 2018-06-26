RLlib: Scalable Reinforcement Learning
======================================

RLlib is an open-source library for reinforcement learning that offers both a collection of reference algorithms and scalable primitives for composing new ones.

Installation
------------

RLlib has extra dependencies on top of **ray**. First, you'll need to install either `PyTorch <http://pytorch.org/>`__ or `TensorFlow <https://www.tensorflow.org/TensorFlow>`__. Then, install the Ray RLlib module:

.. code-block:: bash

  pip install tensorflow  # or tensorflow-gpu
  pip install 'ray[rllib]'

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
* `OpenAI Gym <rllib-env.html#openai-gym>`__
* `Vectorized (Batch) <rllib-env.html#vectorized>`__
* `Multi-Agent <rllib-env.html#multi-agent>`__
* `Serving (Agent-oriented) <rllib-env.html#serving>`__
* `Offline Data Ingest <rllib-env.html#offline-data>`__ 
* `Batch Asynchronous <rllib-env.html#batch-asynchronous>`__

Algorithms
----------
* Ape-X Distributed Prioritized Experience Replay (APEX_DQN, APEX_DDPG)
* Asynchronous Advantage Actor-Critic (A3C)
* Deep Deterministic Policy Gradients (DDPG)
* Deep Q Networks (DQN)
* Evolution Strategies (ES)
* Policy Gradients (PG)
* Proximal Policy Optimization (PPO)

Models and Preprocessors
-------------------------------
* Built-in Models and Preprocessors
* Custom Models
* Custom Preprocessors

RL Building Blocks
------------------
* Policy Models, Losses, Postprocessing
* Policy Evaluation
* Policy Optimization

Package Reference
-----------------
* ray.rllib.env
* `ray.rllib.models <rllib-package-ref.html#module-ray.rllib.models>`__
* ray.rllib.evaluation
* `ray.rllib.optimizers <rllib-package-ref.html#module-ray.rllib.optimizers>`__
* `ray.rllib.utils <rllib-package-ref.html#module-ray.rllib.utils>`__
