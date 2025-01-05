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


.. _rllib-scaling-guide:

RLlib Scaling Guide
-------------------

Here are some rules of thumb for scaling training with RLlib.

1. If the environment is slow and cannot be replicated (e.g., since it requires interaction with physical systems), then you should use a sample-efficient off-policy algorithm such as :ref:`DQN <dqn>` or :ref:`SAC <sac>`. These algorithms default to ``num_env_runners: 0`` for single-process operation. Make sure to set ``num_gpus: 1`` if you want to use a GPU. Consider also batch RL training with the `offline data <rllib-offline.html>`__ API.

2. If the environment is fast and the model is small (most models for RL are), use time-efficient algorithms such as :ref:`PPO <ppo>`, or :ref:`IMPALA <impala>`.
These can be scaled by increasing ``num_env_runners`` to add rollout workers. It may also make sense to enable `vectorization <rllib-env.html#vectorized>`__ for
inference. Make sure to set ``num_gpus: 1`` if you want to use a GPU. If the learner becomes a bottleneck, you can use multiple GPUs for learning by setting
``num_gpus > 1``.

3. If the model is compute intensive (e.g., a large deep residual network) and inference is the bottleneck, consider allocating GPUs to workers by setting ``num_gpus_per_env_runner: 1``. If you only have a single GPU, consider ``num_env_runners: 0`` to use the learner GPU for inference. For efficient use of GPU time, use a small number of GPU workers and a large number of `envs per worker <rllib-env.html#vectorized>`__.

4. Finally, if both model and environment are compute intensive, then enable `remote worker envs <rllib-env.html#vectorized>`__ with `async batching <rllib-env.html#vectorized>`__ by setting ``remote_worker_envs: True`` and optionally ``remote_env_batch_wait_ms``. This batches inference on GPUs in the rollout workers while letting envs run asynchronously in separate actors, similar to the `SEED <https://ai.googleblog.com/2020/03/massively-scaling-reinforcement.html>`__ architecture. The number of workers and number of envs per worker should be tuned to maximize GPU utilization.

In case you are using lots of workers (``num_env_runners >> 10``) and you observe worker failures for whatever reasons, which normally interrupt your RLlib training runs, consider using
the config settings ``ignore_env_runner_failures=True``, ``restart_failed_env_runners=True``, or ``restart_failed_sub_environments=True``:

``restart_failed_env_runners``: When set to True (default), your Algorithm will attempt to restart any failed EnvRunner and replace it with a newly created one. This way, your number of workers will never decrease, even if some of them fail from time to time.
``ignore_env_runner_failures``: When set to True, your Algorithm will not crash due to an EnvRunner error, but continue for as long as there is at least one functional worker remaining. This setting is ignored when ``restart_failed_env_runners=True``.
``restart_failed_sub_environments``: When set to True and there is a failure in one of the vectorized sub-environments in one of your EnvRunners, RLlib tries to recreate only the failed sub-environment and re-integrate the newly created one into your vectorized env stack on that EnvRunner.

Note that only one of ``ignore_env_runner_failures`` or ``restart_failed_env_runners`` should be set to True (they are mutually exclusive settings). However,
you can combine each of these with the ``restart_failed_sub_environments=True`` setting.
Using these options will make your training runs much more stable and more robust against occasional OOM or other similar "once in a while" errors on the EnvRunners
themselves or inside your custom environments.


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
