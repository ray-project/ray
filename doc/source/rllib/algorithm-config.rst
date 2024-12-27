.. include:: /_includes/rllib/we_are_hiring.rst

.. include:: /_includes/rllib/new_api_stack.rst

.. _rllib-algo-configuration-docs:

Configuring RLlib
=================

You can configure RLlib algorithms in a modular fashion by working with so-called
`AlgorithmConfig` objects.
In essence, you first create a `config = AlgorithmConfig()` object and then call methods
on it to set the desired configuration options. For example:

.. testcode::

    from ray.rllib.algorithms.algorithm_config import AlgorithmConfig

    # Create an `AlgorithmConfig` instance.
    config = AlgorithmConfig()
    # Change the learning rate.
    config.training(lr=0.0005)


Coding conventions
------------------

RLlib uses the following, more compact notation in all parts of the code for creating a config and then
modifying it through calling its methods



Generic config settings
-----------------------





Algorithm specific settings
---------------------------

Each RLlib algorithm has its own config class that inherits from `AlgorithmConfig`.
For instance, to create a `PPO` algorithm, you start with a `PPOConfig` object, to work
with a `DQN` algorithm, you start with a `DQNConfig` object, etc.

.. note::

    Each algorithm has its specific settings, but most configuration options are shared.
    We discuss the common options below, and refer to
    :ref:`the RLlib algorithms guide <rllib-algorithms-doc>` for algorithm-specific
    properties.
    Algorithms differ mostly in their `training` settings.

Below you find the basic signature of the `AlgorithmConfig` class, as well as some
advanced usage examples:

.. autoclass:: ray.rllib.algorithms.algorithm_config.AlgorithmConfig
    :noindex:

As RLlib algorithms are fairly complex, they come with many configuration options.
To make things easier, the common properties of algorithms are naturally grouped into
the following categories:

- :ref:`training options <rllib-config-train>`,
- :ref:`environment options <rllib-config-env>`,
- :ref:`deep learning framework options <rllib-config-framework>`,
- :ref:`env runner options <rllib-config-rollouts>`,
- :ref:`evaluation options <rllib-config-evaluation>`,
- :ref:`options for training with offline data <rllib-config-offline_data>`,
- :ref:`options for training multiple agents <rllib-config-multi_agent>`,
- :ref:`reporting options <rllib-config-reporting>`,
- :ref:`options for saving and restoring checkpoints <rllib-config-checkpointing>`,
- :ref:`debugging options <rllib-config-debugging>`,
- :ref:`options for adding callbacks to algorithms <rllib-config-callbacks>`,
- :ref:`Resource options <rllib-config-resources>`
- :ref:`and options for experimental features <rllib-config-experimental>`

Let's discuss each category one by one, starting with training options.

.. _rllib-config-train:

Specifying Training Options
~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. note::

    For instance, a `DQNConfig` takes a `double_q` `training` argument to specify whether
    to use a double-Q DQN, whereas in a `PPOConfig` this does not make sense.

For individual algorithms, this is probably the most relevant configuration group,
as this is where all the algorithm-specific options go.
But the base configuration for `training` of an `AlgorithmConfig` is actually quite small:

.. automethod:: ray.rllib.algorithms.algorithm_config.AlgorithmConfig.training
    :noindex:

.. _rllib-config-env:

Specifying Environments
~~~~~~~~~~~~~~~~~~~~~~~

.. automethod:: ray.rllib.algorithms.algorithm_config.AlgorithmConfig.environment
    :noindex:


.. _rllib-config-framework:

Specifying Framework Options
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. automethod:: ray.rllib.algorithms.algorithm_config.AlgorithmConfig.framework
    :noindex:


.. _rllib-config-rollouts:

Specifying Rollout Workers
~~~~~~~~~~~~~~~~~~~~~~~~~~

.. automethod:: ray.rllib.algorithms.algorithm_config.AlgorithmConfig.env_runners
    :noindex:


.. _rllib-config-evaluation:

Specifying Evaluation Options
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. automethod:: ray.rllib.algorithms.algorithm_config.AlgorithmConfig.evaluation
    :noindex:


.. _rllib-config-offline_data:

Specifying Offline Data Options
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. automethod:: ray.rllib.algorithms.algorithm_config.AlgorithmConfig.offline_data
    :noindex:


.. _rllib-config-multi_agent:

Specifying Multi-Agent Options
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. automethod:: ray.rllib.algorithms.algorithm_config.AlgorithmConfig.multi_agent
    :noindex:


.. _rllib-config-reporting:

Specifying Reporting Options
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. automethod:: ray.rllib.algorithms.algorithm_config.AlgorithmConfig.reporting
    :noindex:


.. _rllib-config-checkpointing:

Specifying Checkpointing Options
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. automethod:: ray.rllib.algorithms.algorithm_config.AlgorithmConfig.checkpointing
    :noindex:


.. _rllib-config-debugging:

Specifying Debugging Options
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. automethod:: ray.rllib.algorithms.algorithm_config.AlgorithmConfig.debugging
    :noindex:


.. _rllib-config-callbacks:

Specifying Callback Options
~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. automethod:: ray.rllib.algorithms.algorithm_config.AlgorithmConfig.callbacks
    :noindex:


.. _rllib-config-resources:

Specifying Resources
~~~~~~~~~~~~~~~~~~~~

You can control the degree of parallelism used by setting the ``num_env_runners``
hyperparameter for most algorithms. The Algorithm will construct that many
"remote worker" instances (`see RolloutWorker class <https://github.com/ray-project/ray/blob/master/rllib/evaluation/rollout_worker.py>`__)
that are constructed as ray.remote actors, plus exactly one "local worker", an ``EnvRunner`` object that isn't a
ray actor, but lives directly inside the Algorithm.
For most algorithms, learning updates are performed on the local worker and sample collection from
one or more environments is performed by the remote workers (in parallel).
For example, setting ``num_env_runners=0`` will only create the local worker, in which case both
sample collection and training will be done by the local worker.
On the other hand, setting ``num_env_runners=5`` will create the local worker (responsible for training updates)
and 5 remote workers (responsible for sample collection).

Since learning is most of the time done on the local worker, it may help to provide one or more GPUs
to that worker via the ``num_gpus`` setting.
Similarly, you can control the resource allocation to remote workers with ``num_cpus_per_env_runner``, ``num_gpus_per_env_runner``, and ``custom_resources_per_env_runner``.

The number of GPUs can be fractional quantities (for example, 0.5) to allocate only a fraction
of a GPU. For example, with DQN you can pack five algorithms onto one GPU by setting
``num_gpus: 0.2``. See `this fractional GPU example here <https://github.com/ray-project/ray/blob/master/rllib/examples/gpus/fractional_gpus.py>`__
as well that also demonstrates how environments (running on the remote workers) that
require a GPU can benefit from the ``num_gpus_per_env_runner`` setting.

For synchronous algorithms like PPO and A2C, the driver and workers can make use of
the same GPU. To do this for an amount of ``n`` GPUS:

.. code-block:: python

    gpu_count = n
    num_gpus = 0.0001 # Driver GPU
    num_gpus_per_env_runner = (gpu_count - num_gpus) / num_env_runners

.. Original image: https://docs.google.com/drawings/d/14QINFvx3grVyJyjAnjggOCEVN-Iq6pYVJ3jA2S6j8z0/edit?usp=sharing
.. image:: images/rllib-config.svg

If you specify ``num_gpus`` and your machine does not have the required number of GPUs
available, a RuntimeError will be thrown by the respective worker. On the other hand,
if you set ``num_gpus=0``, your policies will be built solely on the CPU, even if
GPUs are available on the machine.

.. automethod:: ray.rllib.algorithms.algorithm_config.AlgorithmConfig.resources
    :noindex:


.. _rllib-config-experimental:

Specifying Experimental Features
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. automethod:: ray.rllib.algorithms.algorithm_config.AlgorithmConfig.experimental
    :noindex:

