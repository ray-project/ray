RLlib Developer Guide
=====================

.. note::

    This guide will take you through steps for implementing a new algorithm in RLlib. To apply existing algorithms already implemented in RLlib, please see the `user docs <rllib.html>`__.

Recipe for an RLlib algorithm
-----------------------------

Here are the steps for implementing a new algorithm in RLlib:

1. Define an algorithm-specific `Policy evaluator class <#policy-evaluators-and-optimizers>`__ (the core of the algorithm). Evaluators encapsulate framework-specific components such as the policy and loss functions. For an example, see the `A3C Evaluator implementation <https://github.com/ray-project/ray/blob/master/python/ray/rllib/a3c/a3c_evaluator.py>`__.


2. Pick an appropriate `Policy optimizer class <#policy-evaluators-and-optimizers>`__. Optimizers manage the parallel execution of the algorithm. RLlib provides several built-in optimizers for gradient-based algorithms. Advanced algorithms may find it beneficial to implement their own optimizers.


3. Wrap the two up in an `Agent class <#agents>`__. Agents are the user-facing API of RLlib. They provide the necessary "glue" and implement accessory functionality such as statistics reporting and checkpointing.

To help with implementation, RLlib provides common action distributions, preprocessors, and neural network models, found in `catalog.py <https://github.com/ray-project/ray/blob/master/python/ray/rllib/models/catalog.py>`__, which are shared by all algorithms. Note that most of these utilities are currently Tensorflow specific.

.. image:: rllib-api.svg


The Developer API
-----------------

The following APIs are the building blocks of RLlib algorithms (also take a look at the `user components overview <rllib.html#components-user-customizable-and-internal>`__).

Agents
~~~~~~

Agents implement a particular algorithm and can be used to run
some number of iterations of the algorithm, save and load the state
of training and evaluate the current policy. All agents inherit from
a common base class:

.. autoclass:: ray.rllib.agent.Agent
    :members:

Policy Evaluators and Optimizers
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. autoclass:: ray.rllib.optimizers.evaluator.Evaluator
    :members:

.. autoclass:: ray.rllib.optimizers.optimizer.Optimizer
    :members:

Sample Batches
~~~~~~~~~~~~~~

In order for Optimizers to manipulate sample data, they should be returned from Evaluators
in the SampleBatch format (a wrapper around a dict).

.. autoclass:: ray.rllib.optimizers.SampleBatch
    :members:

Models and Preprocessors
~~~~~~~~~~~~~~~~~~~~~~~~

Algorithms share neural network models which inherit from the following class:

.. autoclass:: ray.rllib.models.Model
    :members:

Currently we support fully connected and convolutional TensorFlow policies on all algorithms:

.. autoclass:: ray.rllib.models.FullyConnectedNetwork
.. autoclass:: ray.rllib.models.ConvolutionalNetwork

A3C also supports a TensorFlow LSTM policy.

.. autoclass:: ray.rllib.models.LSTM

Observations are transformed by Preprocessors before used in the model:

.. autoclass:: ray.rllib.models.preprocessors.Preprocessor
    :members:

Action Distributions
~~~~~~~~~~~~~~~~~~~~

Actions can be sampled from different distributions which have a common base
class:

.. autoclass:: ray.rllib.models.ActionDistribution
    :members:

Currently we support the following action distributions:

.. autoclass:: ray.rllib.models.Categorical
.. autoclass:: ray.rllib.models.DiagGaussian
.. autoclass:: ray.rllib.models.Deterministic

The Model Catalog
~~~~~~~~~~~~~~~~~

The Model Catalog is the mechanism for algorithms to get canonical preprocessors, models, and action distributions for varying gym environments. It enables easy reuse of these components across different algorithms.

.. autoclass:: ray.rllib.models.ModelCatalog
    :members:
