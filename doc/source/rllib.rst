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
* `Serving (Agent-oriented) <rllib-env.html#serving>`__
* `Offline Data Ingest <rllib-env.html#offline-data>`__ 
* `Batch Asynchronous <rllib-env.html#batch-asynchronous>`__

Algorithms
----------
* `Ape-X Distributed Prioritized Experience Replay <rllib-algorithms.html#ape-x-distributed-prioritized-experience-replay>`__
* `Asynchronous Advantage Actor-Critic <rllib-algorithms.html#asynchronous-advantage-actor-critic>`__
* `Deep Deterministic Policy Gradients <rllib-algorithms.html#deep-deterministic-policy-gradients>`__
* `Deep Q Networks <rllib-algorithms.html#deep-q-networks>`__
* `Evolution Strategies <rllib-algorithms.html#evolution-strategies>`__
* `Policy Gradients <rllib-algorithms.html#policy-gradients>`__
* `Proximal Policy Optimization <rllib-algorithms.html#proximal-policy-optimization>`__

Models and Preprocessors
------------------------
* `RLlib Models and Preprocessors Overview <rllib-models.html>`__
* `Built-in Models and Preprocessors <rllib-models.html#built-in-models-and-preprocessors>`__
* `Custom Models <rllib-models.html#custom-models>`__
* `Custom Preprocessors <rllib-models.html#custom-preprocessors>`__
* `Customizing Policy Graphs <rllib-models.html#customizing-policy-graphs>`__

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
