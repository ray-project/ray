RLlib: Scalable Reinforcement Learning
======================================

RLlib is an open-source library for reinforcement learning that offers both high scalability and a unified API for a variety of applications.

.. image:: rllib-stack.svg

To get started, take a look over the `custom env example <https://github.com/ray-project/ray/blob/master/rllib/examples/custom_env.py>`__ and the `API documentation <rllib-training.html>`__. If you're looking to develop custom algorithms with RLlib, also check out `concepts and custom algorithms <rllib-concepts.html>`__.

Installation
------------

RLlib has extra dependencies on top of ``ray``. First, you'll need to install either `PyTorch <http://pytorch.org/>`__ or `TensorFlow <https://www.tensorflow.org>`__. Then, install the RLlib module:

.. code-block:: bash

  pip install ray[rllib]  # also recommended: ray[debug]

RLlib in 60 seconds
-------------------

This section is a brief overview of RLlib. See also the full `table of contents <rllib-toc.html>`__.

Policies and Policy Rollout
~~~~~~~~~~~~~~~~~~~~~~~~~~~

`Policies` are a core concept in RLlib. In a nutshell, policies are Python classes that define how an agent acts in an environment. In a `single-agent environment`, `rollout workers` query the policy to determine agent actions. In `multi-agent environments`, there may be an ensemble of policies, each controlling `one or more agents`:

:policy-diagram:

Policies can be implemented using `any framework`. However, for TensorFlow and PyTorch, RLlib has `build_tf_policy` and `build_torch_policy` helper functions that let you define a trainable policy in just a few lines of code:

:build-tf-policy:

Experience Batches
~~~~~~~~~~~~~~~~~~

Whether running in a single process or `distributed`, all data interchange in RLlib is in the form of `sample batches`. Sample batches encode one or more fragments of a trajectory. Typically, RLlib collects batches of size ``sample_batch_size`` from rollout workers, and concatenates one or more of these batches into a batch of size ``train_batch_size`` that is the input to SGD.

In multi-agent mode, sample batches are collected for each individual policy:

:batch-examples:

Learning
~~~~~~~~

Policies define a `learn_on_batch` function that improves the policy given a sample batch of input. For TF and Torch policies, this is implemented as a `loss function` that takes as input sample batch tensors and outputs a scalar loss.

RLlib `Trainer classes` coordinate the distributed workflow of running rollouts and optimizing policies. They do this by leveraging `policy optimizer` classes that implement computation pattern designed (i.e., synchronous or asynchronous sampling, distributed replay, etc).

Execution
~~~~~~~~~

RLlib uses `Ray` actors to seamlessly scale training from a single core to many thousands of cores in a cluster. You can control the number of rollout workers used for an algorithm by changing the ``num_workers`` parameter.

Customization
~~~~~~~~~~~~~

RLlib provides ways to customize almost all aspects of training, including the `neural network model`, `action distribution`, `losses`, and `policy definitions`.

To learn more, proceed to the `table of contents <rllib-toc.html>`__.
