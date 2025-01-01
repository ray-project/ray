.. include:: /_includes/rllib/we_are_hiring.rst

.. include:: /_includes/rllib/new_api_stack.rst

.. _rllib-getting-started:

Getting Started with RLlib
==========================

All RLlib experiments are run using an ``Algorithm`` class which holds a policy for environment interaction.
Through the algorithm's interface, you can train the policy, compute actions, or store your algorithm's state (checkpointing).
In multi-agent training, the algorithm manages the querying and optimization of multiple policies at once.

.. image:: images/rllib-api.svg

In this guide, we will explain in detail RLlib's Python API for running learning experiments.


.. _rllib-training-api:

Using the Python API
--------------------

The Python API provides all the flexibility required for applying RLlib to any type of problem.

Let's start with an example of the API's basic usage.
We first create a `PPOConfig` instance and set some properties through the config class' various methods.
For example, we can set the RL environment we want to use by calling the config's `environment` method.
To scale our algorithm and define, how many environment workers (EnvRunners) we want to leverage, we can call
the `env_runners` method.
After we `build` the `PPO` Algorithm from its configuration, we can `train` it for a number of
iterations (here `10`) and `save` the resulting policy periodically (here every `5` iterations).

.. literalinclude:: ./doc_code/getting_started.py
    :language: python
    :start-after: rllib-first-config-begin
    :end-before: rllib-first-config-end


All RLlib algorithms are compatible with the :ref:`Tune API <tune-api-ref>`.
This enables them to be easily used in experiments with :ref:`Ray Tune <tune-main>`.
For example, the following code performs a simple hyper-parameter sweep of PPO.


.. literalinclude:: ./doc_code/getting_started.py
    :dedent: 4
    :language: python
    :start-after: rllib-tune-config-begin
    :end-before: rllib-tune-config-end

Tune will schedule the trials to run in parallel on your Ray cluster:

::

    == Status ==
    Using FIFO scheduling algorithm.
    Resources requested: 4/4 CPUs, 0/0 GPUs
    Result logdir: ~/ray_results/my_experiment
    PENDING trials:
     - PPO_CartPole-v1_2_lr=0.0001:	PENDING
    RUNNING trials:
     - PPO_CartPole-v1_0_lr=0.01:	RUNNING [pid=21940], 16 s, 4013 ts, 22 rew
     - PPO_CartPole-v1_1_lr=0.001:	RUNNING [pid=21942], 27 s, 8111 ts, 54.7 rew

``Tuner.fit()`` returns an ``ResultGrid`` object that allows further analysis
of the training results and retrieving the checkpoint(s) of the trained agent.

.. literalinclude:: ./doc_code/getting_started.py
    :dedent: 0
    :language: python
    :start-after: rllib-tuner-begin
    :end-before: rllib-tuner-end

.. note::

    You can find your checkpoint's version by
    looking into the ``rllib_checkpoint.json`` file inside your checkpoint directory.

Loading and restoring a trained algorithm from a checkpoint is simple.
Let's assume you have a local checkpoint directory called ``checkpoint_path``.
To load newer RLlib checkpoints (version >= 1.0), use the following code:


.. code-block:: python

    from ray.rllib.algorithms.algorithm import Algorithm
    algo = Algorithm.from_checkpoint(checkpoint_path)


For older RLlib checkpoint versions (version < 1.0), you can
restore an algorithm through:

.. code-block:: python

    from ray.rllib.algorithms.ppo import PPO
    algo = PPO(config=config, env=env_class)
    algo.restore(checkpoint_path)


Computing Actions
~~~~~~~~~~~~~~~~~

The simplest way to programmatically compute actions from a trained agent is to
use ``Algorithm.compute_single_action()``.
This method preprocesses and filters the observation before passing it to the agent
policy.
Here is a simple example of testing a trained agent for one episode:

.. literalinclude:: ./doc_code/getting_started.py
    :language: python
    :start-after: rllib-compute-action-begin
    :end-before: rllib-compute-action-end

For more advanced usage on computing actions and other functionality,
you can consult the :ref:`RLlib Algorithm API documentation <rllib-algorithm-api>`.

Accessing Policy State
~~~~~~~~~~~~~~~~~~~~~~

It is common to need to access a algorithm's internal state, for instance to set
or get model weights.

In RLlib algorithm state is replicated across multiple *rollout workers* (Ray actors)
in the cluster.
However, you can easily get and update this state between calls to ``train()``
via ``Algorithm.env_runner_group.foreach_worker()``
or ``Algorithm.env_runner_group.foreach_worker_with_index()``.
These functions take a lambda function that is applied with the worker as an argument.
These functions return values for each worker as a list.

You can also access just the "master" copy of the algorithm state through
``Algorithm.get_policy()`` or ``Algorithm.env_runner``,
but note that updates here may not be immediately reflected in
your rollout workers (if you have configured ``num_env_runners > 0``).
Here's a quick example of how to access state of a model:

.. literalinclude:: ./doc_code/getting_started.py
    :language: python
    :start-after: rllib-get-state-begin
    :end-before: rllib-get-state-end

Accessing Model State
~~~~~~~~~~~~~~~~~~~~~

Similar to accessing policy state, you may want to get a reference to the
underlying neural network model being trained. For example, you may want to
pre-train it separately, or otherwise update its weights outside of RLlib.
This can be done by accessing the ``model`` of the policy.

.. note::

    To run these examples, you need to install a few extra dependencies, namely
    `pip install "gym[atari]" "gym[accept-rom-license]" atari_py`.

Below you find three explicit examples showing how to access the model state of
an algorithm.

.. dropdown:: **Example: Preprocessing observations for feeding into a model**


    Then for the code:

    .. literalinclude:: doc_code/training.py
        :language: python
        :start-after: __preprocessing_observations_start__
        :end-before: __preprocessing_observations_end__

.. dropdown:: **Example: Querying a policy's action distribution**

    .. literalinclude:: doc_code/training.py
        :language: python
        :start-after: __query_action_dist_start__
        :end-before: __query_action_dist_end__

.. dropdown:: **Example: Getting Q values from a DQN model**

    .. literalinclude:: doc_code/training.py
        :language: python
        :start-after: __get_q_values_dqn_start__
        :end-before: __get_q_values_dqn_end__

    This is especially useful when used with
    `custom model classes <rllib-models.html>`__.

.. _rllib-algo-configuration:

Configuring RLlib Algorithms
----------------------------

You can configure RLlib algorithms in a modular fashion by working with so-called
`AlgorithmConfig` objects.
In essence, you first create a `config = AlgorithmConfig()` object and then call methods
on it to set the desired configuration options.
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


.. _rllib-config-experimental:

Specifying Experimental Features
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. automethod:: ray.rllib.algorithms.algorithm_config.AlgorithmConfig.experimental
    :noindex:

Debugging RLlib Experiments
---------------------------

Eager Mode
~~~~~~~~~~

Policies built with ``build_tf_policy`` (most of the reference algorithms are)
can be run in eager mode by setting the
``"framework": "tf2"`` / ``"eager_tracing": true`` config options.
This will tell RLlib to execute the model forward pass, action distribution,
loss, and stats functions in eager mode.

Eager mode makes debugging much easier, since you can now use line-by-line
debugging with breakpoints or Python ``print()`` to inspect
intermediate tensor values.
However, eager can be slower than graph mode unless tracing is enabled.


Episode Traces
~~~~~~~~~~~~~~

You can use the `data output API <rllib-offline.html>`__ to save episode traces
for debugging. For example, the following command will run PPO while saving episode
traces to ``/tmp/debug``.

.. code-block:: bash

    cd rllib/tuned_examples/ppo
    python cartpole_ppo.py --output /tmp/debug

    # episode traces will be saved in /tmp/debug, for example
    output-2019-02-23_12-02-03_worker-2_0.json
    output-2019-02-23_12-02-04_worker-1_0.json

Log Verbosity
~~~~~~~~~~~~~

You can control the log level via the ``"log_level"`` flag. Valid values are "DEBUG",
"INFO", "WARN" (default), and "ERROR". This can be used to increase or decrease the
verbosity of internal logging.
For example:

.. code-block:: bash

    cd rllib/tuned_examples/ppo

    python atari_ppo.py --env ALE/Pong-v5 --log-level INFO
    python atari_ppo.py --env ALE/Pong-v5 --log-level DEBUG

The default log level is ``WARN``. We strongly recommend using at least ``INFO``
level logging for development.

Stack Traces
~~~~~~~~~~~~

You can use the ``ray stack`` command to dump the stack traces of all the
Python workers on a single node. This can be useful for debugging unexpected
hangs or performance issues.

Next Steps
----------

- To check how your application is doing, you can use the :ref:`Ray dashboard <observability-getting-started>`.
