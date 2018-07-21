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
* `Command-line <rllib-training.rst>`__
* `Python API <rllib-training.rst#python-api>`__
* `REST API <rllib-training.rst#rest-api>`__

Environments
------------
* `RLlib Environments Overview <rllib-env.rst>`__
* `OpenAI Gym <rllib-env.rst#openai-gym>`__
* `Vectorized <rllib-env.rst#vectorized>`__
* `Multi-Agent <rllib-env.rst#multi-agent>`__
* `Serving (Agent-oriented) <rllib-env.rst#serving>`__
* `Offline Data Ingest <rllib-env.rst#offline-data>`__ 
* `Batch Asynchronous <rllib-env.rst#batch-asynchronous>`__

Algorithms
----------
* `Ape-X Distributed Prioritized Experience Replay <rllib-algorithms.rst#ape-x-distributed-prioritized-experience-replay>`__
* `Asynchronous Advantage Actor-Critic <rllib-algorithms.rst#asynchronous-advantage-actor-critic>`__
* `Deep Deterministic Policy Gradients <rllib-algorithms.rst#deep-deterministic-policy-gradients>`__
* `Deep Q Networks <rllib-algorithms.rst#deep-q-networks>`__
* `Evolution Strategies <rllib-algorithms.rst#evolution-strategies>`__
* `Policy Gradients <rllib-algorithms.rst#policy-gradients>`__
* `Proximal Policy Optimization <rllib-algorithms.rst#proximal-policy-optimization>`__

Models and Preprocessors
------------------------
* `RLlib Models and Preprocessors Overview <rllib-models.rst>`__
* `Built-in Models and Preprocessors <rllib-models.rst#built-in-models-and-preprocessors>`__
* `Custom Models <rllib-models.rst#custom-models>`__
* `Custom Preprocessors <rllib-models.rst#custom-preprocessors>`__

RLlib Concepts
--------------
* `Policy Graphs <rllib-concepts.rst>`__
* `Policy Evaluation <rllib-concepts.rst#policy-evaluation>`__
* `Policy Optimization <rllib-concepts.rst#policy-optimization>`__

Package Reference
-----------------
* `ray.rllib.agents <rllib-package-ref.rst#module-ray.rllib.agents>`__
* `ray.rllib.env <rllib-package-ref.rst#module-ray.rllib.env>`__
* `ray.rllib.evaluation <rllib-package-ref.rst#module-ray.rllib.evaluation>`__
* `ray.rllib.models <rllib-package-ref.rst#module-ray.rllib.models>`__
* `ray.rllib.optimizers <rllib-package-ref.rst#module-ray.rllib.optimizers>`__
* `ray.rllib.utils <rllib-package-ref.rst#module-ray.rllib.utils>`__
