.. include:: /_includes/rllib/we_are_hiring.rst

.. include:: /_includes/rllib/new_api_stack.rst

.. _rllib-getting-started:

Getting Started
===============

.. _rllib-in-60min:

RLlib in 60 minutes
-------------------

.. figure:: images/rllib-index-header.svg

In this tutorial, you learn how to design, customize, and run an end-to-end RLlib learning experiment
from scratch. This includes picking and configuring an Algorithm, running a couple of training iterations,
saving the state of your Algorithm from time to time, running a separate evaluation loop,
and finally utilizing one of the checkpoints to deploy your trained model in an environment outside of RLlib
and compute actions through it.

You also learn how to optionally customize your RL environment and your neural network model.

Installation
~~~~~~~~~~~~

First, install RLlib and `PyTorch <https://pytorch.org>`__, as shown below:

.. code-block:: bash

    pip install "ray[rllib]" "gymnasium[atari,accept-rom-license,mujoco]" torch


.. _rllib-python-api:

Python API
~~~~~~~~~~

RLlib's Python API provides all the flexibility required for applying the library to any
type of RL problem.

You manage experiments in RLlib through an instance of the :py:class:`~ray.rllib.algorithms.algorithm.Algorithm`
class. An :py:class:`~ray.rllib.algorithms.algorithm.Algorithm` typically holds a neural
network for computing actions, called "policy", the :ref:`RL environment <rllib-key-concepts-environments>`
you want to optimize against, a loss function, an optimizer, and some code describing the
algorithm's execution logic, like determining when to take which particular steps.

In multi-agent training, :py:class:`~ray.rllib.algorithms.algorithm.Algorithm`
manages the querying and optimization of multiple policies at once.

Through the algorithm's interface, you can train the policy, compute actions, or store your
algorithm's state through checkpointing.





Let's start with an example of the API's basic usage.
We first create a `PPOConfig` instance and set some properties through the config class' various methods.
For example, we can set the RL environment we want to use by calling the config's `environment` method.
To scale our algorithm and define, how many environment workers (EnvRunners) we want to leverage, we can call
the `env_runners` method.
After we `build` the `PPO` Algorithm from its configuration, we can `train` it for a number of
iterations (here `10`) and `save` the resulting policy periodically (here every `5` iterations).


.. testcode::

    from ray.rllib.algorithms.ppo import PPOConfig

    # Configure the Algorithm (PPO).
    config = (
        PPOConfig()
        .environment("CartPole-v1")
        .env_runners(num_env_runners=1)
    )
    # Build the Algorithm (PPO).
    ppo = config.build()

    # Train for 10 iterations.
    for i in range(10):
        result = ppo.train()
        result.pop("config")
        print(result)

        # Checkpoint every 5 iterations.
        if i % 5 == 0:
            checkpoint_dir = ppo.save_to_path()
            print(f"Algorithm checkpoint saved in: {checkpoint_dir}")



.. _rllib-with-ray-tune:

RLlib with Ray Tune
~~~~~~~~~~~~~~~~~~~

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
To load newer RLlib checkpoints (version >= 2.1), use the following code:


.. code-block:: python

    from ray.rllib.algorithms.algorithm import Algorithm

    algo = Algorithm.from_checkpoint(checkpoint_path)


Customizing your RL environment
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

In the preceding examples, your :ref:`RL environment <rllib-key-concepts-environments>` was a `Farama gymnasium <gymnasium.farama.org>`__
pre-registered one, like ``CartPole-v1``. However, if you would like to run your experiments against a custom one,
see this tab below for a less-than-50-lines example.

See here for an :ref:`in-depth guide on how to setup RL environments in RLlib <rllib-environments-doc>` and how to customize them.

.. dropdown:: Quickstart: Custom RL environment
    :animate: fade-in-slide-down

    .. testcode::

        import gymnasium as gym
        from ray.rllib.algorithms.ppo import PPOConfig

        # Define your custom env class by subclassing gymnasium.Env:

        class ParrotEnv(gym.Env):
            """Environment in which the agent learns to repeat the seen observations.

            Observations are float numbers indicating the to-be-repeated values,
            e.g. -1.0, 5.1, or 3.2.
            The action space is the same as the observation space.
            Rewards are `r=-abs([observation] - [action])`, for all steps.
            """
            def __init__(self, config=None):
                # Since actions should repeat observations, their spaces must be the same.
                self.observation_space = gym.spaces.Box(-1.0, 1.0, (1,), np.float32)
                self.action_space = self.observation_space
                self._cur_obs = None
                self._episode_len = 0

            def reset(self, *, seed=None, options=None):
                """Resets the environment, starting a new episode."""
                # Reset the episode len.
                self._episode_len = 0
                # Sample a random number from our observation space.
                self._cur_obs = self.observation_space.sample()
                # Return initial observation.
                return self._cur_obs, {}

            def step(self, action):
                """Takes a single step in the episode given `action`."""
                # Set `terminated` and `truncated` flags to True after 10 steps.
                self._episode_len += 1
                terminated = truncated = self._episode_len >= 10
                # Compute the reward: `r = -abs([obs] - [action])`
                reward = -sum(abs(self._cur_obs - action))
                # Set a new observation (random sample).
                self._cur_obs = self.observation_space.sample()
                return self._cur_obs, reward, terminated, truncated, {}

        # Point your config to your custom env class:
        config = (
            PPOConfig()
            .environment(ParrotEnv)  # add `env_config=[some Box space] to customize the env
        )

        # Build a PPO algorithm and train it.
        ppo_w_custom_env = config.build_algo()
        ppo_w_custom_env.train()

    .. testcode::
        :hide:

        # Test that our setup is working.
        ppo_w_custom_env.stop()


Customizing your models
~~~~~~~~~~~~~~~~~~~~~~~

In the preceding examples, RLlib provided a default neural network model for you, because you didn't specify anything
in your AlgorithmConfig. If you would like to either reconfigure the type and size of RLlib's default models, for example define
the number of hidden layers and their activation functions, or even write your own custom models from scratch using PyTorch, see here
for a detailed guide on how to do so.


Deploying your models and computing actions
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The simplest way to programmatically compute actions from a trained :py:class:`~ray.rllib.algorithms.algorithm.Algorithm`
is to get the :py:class:`~ray.rllib.core.rl_module.rl_module.RLModule` through :py:meth:`~ray.rllib.algorithms.algorithm.Algorithm.get_module`,
then call the module's :py:meth:`~ray.rllib.core.rl_module.rl_module.forward_inference` method.

Here is an example of how to test a trained agent for one episode:

.. testcode::

    import gymnasium as gym
    import numpy as np
    import torch
    from ray.rllib.core.rl_module import RLModule

    env = gym.make("CartPole-v1")

    # Get the RLModule from the up and running Algorithm instance:
    rl_module = ppo.get_module()

    episode_return = 0
    terminated = truncated = False

    obs, info = env.reset()

    while not terminated and not truncated:
        # Compute the next action from a batch (B=1) of observations.
        obs_batch = torch.from_numpy(obs).unsqueeze(0)  # add batch B=1 dimension
        # Extract the logits from the output and dissolve batch again.
        action_logits = rl_module.forward_inference({"obs": obs_batch})[
            "action_dist_inputs"
        ][0]
        # PPO's default RLModule produces action logits (from which
        # you have to sample an action or use the max-likelihood one).
        action = numpy.argmax(action_logits.numpy())
        # Send the action to the environment for the next step.
        obs, reward, terminated, truncated, info = env.step(action)
        episode_return += reward

    print(f"Reached episode return of {episode_return}.")


If you don't have your Algorithm instance up and running anymore and would like to create the trained RLModule
from a checkpoint, you can do the following instead.
Note that `best_checkpoint` is the highest performing Algorithm checkpoint you created
in the preceding experiment. To learn more about checkpoints and their structure, see this :ref:`checkpointing guide <rllib-checkpointing-docs>`.

.. testcode::

    from pathlib import Path

    # Create only the neural network (RLModule) from our checkpoint.
    rl_module = RLModule.from_checkpoint(
        Path(best_checkpoint.path) / "learner_group" / "learner" / "rl_module"
    )["default_policy"]

    # Do the same computations with `rl_module` as in the preceding code snippet.


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


.. Debugging RLlib Experiments
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
