
.. include:: /_includes/rllib/we_are_hiring.rst

.. include:: /_includes/rllib/new_api_stack.rst


.. _rllib-key-concepts:

Key concepts
============

To help you get a high-level understanding of how the library works, on this page, you learn about the
key concepts and general architecture of RLlib.

.. figure:: images/rllib_key_concepts.svg
    :width: 800
    :align: left

    **RLlib overview:** The central component of RLlib is the :py:class:`~ray.rllib.algorithms.algorithm.Algorithm`
    class, acting as a runtime for executing your RL experiments.
    Your gateway into using an :ref:`Algorithm <rllib-key-concepts-algorithms>` is the
    :py:class:`~ray.rllib.algorithms.algorithm_config.AlgorithmConfig` (<span style="color: #cfe0e1ff;">**cyan**</span>) class, allowing
    you to manage all available config settings, for example the learning rate.
    Most :py:class:`~ray.rllib.algorithms.algorithm.Algorithm` objects have
    :py:class:`~ray.rllib.env.env_runner.EnvRunner` actors (<span style="color: #d0e2f3;">**blue**</span>) to collect training samples
    from the :ref:`RL environment <rllib-key-concepts-environments>`,
    :py:class:`~ray.rllib.core.learner.learner.Learner` actors (<span style="color: #fff2ccff;">**yellow**</span>)
    to compute gradients and to update your :ref:`models <rllib-key-concepts-rl-modules>`.
    The model's weights are synchronized after model updates.


.. _rllib-key-concepts-algorithms:

Algorithms
----------

.. tip::
    The following is a quick overview of what an **RLlib Algorithm** is.

    .. todo (sven): Change the following link to the actual algorithm page, once done. Right now, it's pointing to the algos-overview page, instead!

    See :ref:`here for a detailed description of how to use RLlib's algorithms <rllib-algorithms-doc>`.

The RLlib `Algorithm` class serves as a runtime for your RL experiments, bringing together all components required
for learning a solution to your RL environment. It exposes a powerful Python API for controlling your experiment runs.

Each :py:class:`~ray.rllib.algorithms.algorithm.Algorithm` class is managed by its respective
:py:class:`~ray.rllib.algorithms.algorithm_config.AlgorithmConfig` class. For example, to configure a
:py:class:`~ray.rllib.algorithms.ppo.ppo.PPO` ("Proximal Policy Optimization") instance, you should use
the :py:class:`~ray.rllib.algorithms.ppo.ppo.PPOConfig` class.

An algorithm sets up its :py:class:`~ray.rllib.env.env_runner_group.EnvRunnerGroup` (containing ``n``
:py:class:`~ray.rllib.env.env_runner.EnvRunner` `actors <actors.html>`__) and
its :py:class:`~ray.rllib.core.learner.learner_group.LearnerGroup`
(containing ``m`` :py:class:`~ray.rllib.core.learner.learner.Learner` `actors <actors.html>`__)
scaling sample collection and training, respectively, from a single core to many thousands of cores in a cluster.

.. todo: Separate out our scaling guide into its own page in new PR

See this `scaling guide <rllib-training.html#scaling-guide>`__ for more details here.

:py:class:`~ray.rllib.algorithms.algorithm.Algorithm` also subclasses from the :ref:`Tune Trainable API <tune-60-seconds>`
for easy experiment management and hyperparameter tuning.

You have two ways to interact with and run an :py:class:`~ray.rllib.algorithms.algorithm.Algorithm`.

- You can create and manage an instance of it directly through the Python API.
- You can use Ray Tune to more easily tune the hyperparameters for a particular problem.

The following example shows these equivalent ways of interacting with the ``PPO`` ("Proximal Policy Optimization") algorithm of RLlib:

.. tab-set::

    .. tab-item:: Manage ``Algorithm`` instance directly

        .. testcode::

            from ray.rllib.algorithms.ppo import PPOConfig

            # Configure.
            config = (
                PPOConfig()
                .environment("CartPole-v1")
                .training(
                    train_batch_size_per_learner=2000,
                    lr=0.0004,
                )
            )

            # Build the Algorithm.
            algo = config.build()

            # Train for one iteration, which is 2000 timesteps (1 train batch).
            print(algo.train())


    .. tab-item:: Run ``Algorithm`` through Ray Tune

        .. testcode::

            from ray import train, tune
            from ray.rllib.algorithms.ppo import PPOConfig

            # Configure.
            config = (
                PPOConfig()
                .environment("CartPole-v1")
                .training(
                    train_batch_size_per_learner=2000,
                    lr=0.0004,
                )
            )

            # Train through Ray Tune.
            results = tune.Tuner(
                "PPO",
                param_space=config,
                # Train for 4000 timesteps (2 iterations).
                run_config=train.RunConfig(stop={"num_env_steps_sampled_lifetime": 4000}),
            ).fit()


.. _rllib-key-concepts-environments:

RL environments
---------------

.. tip::
    The following is a quick overview of what an **RL environment** is.
    See :ref:`here for a detailed description of how to use RL environments in RLlib <rllib-environments-doc>`.

A reinforcement learning (RL) environment is a structured space where one or more agents interact and learn to achieve specific goals.
It defines an observation space (the structure and shape of observable tensors at each timesteo),
an action space (the available actions for the agents at each time step), a reward function,
and the rules that govern the environment transitions.

Environments may vary in complexity, from simple tasks like navigating a grid world to highly intricate systems like autonomous
driving simulators, robotic control environments, or multi-agent games.

.. figure:: images/envs/env_loop_concept.svg
    :width: 900
    :align: left

    A simple **RL environment** where an agent starts with an initial observation after the ``reset()`` method has been called
    on the environment.
    The agent, possibly controlled by a neural network policy, sends actions to the environmant's ``step()`` method,
    such as "right" or "jump", and a reward based on environment specific rules is returned (here, +5 for reaching the goal,
    0 otherwise). The environment also returns, whether the episode has been completed or not.

RLlib plays through many such episodes during a training iteration to collect **episodes**, which
contain observations, taken actions, received rewards and the ``done`` flags, and are eventually used to construct
a train batch for model updating.


.. _rllib-key-concepts-rl-modules:

RLModules
---------

.. tip::
    The following is a quick overview of what an **RLlib RLModule** is.
    See :ref:`here for a detailed description of how to use RLModules <rllib-rlmodule-guide>`.

`RLModules <rllib-rlmodule.html>`__ are deep-learning framework-specific neural network containers.
They are used by RLlib's :ref:`EnvRunners <rllib-key-concepts-env-runners>` for computing actions in the
:ref:`RL environment <rllib-key-concepts-environments>` and by RLlib's :ref:`Learners <rllib-key-concepts-learners>` for
computing losses and gradients.

.. figure:: images/rl_modules/rl_module_overview.svg

    **RLModule overview**: *(left)* A minimal :py:class:`~ray.rllib.core.rl_module.rl_module.RLModule` contains a neural network
    and defines its exploration-, inference- and training logic to map observations to actions.
    *(right)* In more complex setups, a :py:class:`~ray.rllib.core.rl_module.multi_rl_module.MultiRLModule` contains
    many submodules, each itself an :py:class:`~ray.rllib.core.rl_module.rl_module.RLModule` instance and
    identified by a ``ModuleID``. This way, arbitrarily complex multi-model and multi-agent algorithms
    can be implemented.

In a nutshell, ``RLModules`` carry the neural network models and define how to use them during the three phases of the models'
RL lifecycle: Exploration (collecting training data), inference (production/deployment),
and training (computing loss function inputs).

.. link to new RLModule docs

You can chose to use :ref:`RLlib's built-in default models and configure these <rllib_default_rl_modules_docs>` as needed
(change number of layers and size, activation functions, etc..) or :ref:`write your own custom models in PyTorch <rllib-implementing-custom-rl-modules>`,
allowing you to implement any architecture and computation logic.

.. figure:: images/rl_modules/rl_module_in_env_runner.svg
    :width: 400

    **An RLModule inside an EnvRunner actor**: The :py:class:`~ray.rllib.env.env_runner.EnvRunner` operates on its own copy of your
    (usually inference-only) :py:class:`~ray.rllib.core.rl_module.rl_module.RLModule`, using it to only compute actions.

A copy of the user's RLModule is located inside each :py:class:`~ray.rllib.env.env_runner.EnvRunner` actor
managed by the :py:class:`~ray.rllib.env.env_runner_group.EnvRunnerGroup` of the Algorithm and another one in each
:py:class:`~ray.rllib.core.learner.learner.Learner` actor managed by the :py:class:`~ray.rllib.core.learner.learner_group.LearnerGroup`
of the Algorithm.

.. figure:: images/rl_modules/rl_module_in_learner.svg
    :width: 400

    **An RLModule inside a Learner actor**: The :py:class:`~ray.rllib.core.learner.learner.:Learner` operates on its own copy of your (complete)
    :py:class:`~ray.rllib.core.rl_module.rl_module.RLModule`, computing the loss function inputs, the loss itself,
    and model gradients, then updating your RLModule through its optimizer.

The EnvRunner copy is normally kept in an ``inference_only`` version, meaning those components that aren't
required for pure action computations (for example, a value function) may be missing to save memory.


.. _rllib-key-concepts-episodes:

Episodes
--------

.. tip::
    The following is a quick overview of what an **Episode** is.
    See :ref:`here for a detailed description of how to use RLlib's Episodes <single-agent-episode-docs>`.

All training data in RLlib is interchanged in the form of :ref:`Episodes <single-agent-episode-docs>`.

The :py:class`~ray.rllib.env.single_agent_episode.SingleAgentEpisode` class is used for
describing single-agent trajectories, whereas the
:py:class`~ray.rllib.env.multi_agent_episode.MultiAgentEpisode` class contains several
such single-agent episodes and also stores information about the stepping times- and patterns of
the individual agents with respect to each other.

Both ``Episode`` classes store the entire (trajectory) data generated while stepping through an :ref:`environment <rllib-key-concepts-environments>`.
This includes the observations, info dicts, actions taken,
rewards received, and any model computations along the way, such as RNN-states, action logits,
or action log probs.

.. tip::
    See here for `RLlib's standardized columns used across all classes <https://github.com/ray-project/ray/blob/master/rllib/core/columns.py>`__.
    Note also that episodes conveniently don't have to store any "next obs" information as these always overlap
    almost completely with the information in "obs", thus saving about 50% of memory (observations are often the
    largest piece in a trajectory). Same is true for "state_in" and "state_out" information for stateful networks: Only
    the "state_out" information is kept.

Typically, RLlib generates episode (chunks) of size ``config.rollout_fragment_length`` through the :ref:`EnvRunner <rllib-key-concepts-env-runners>`
actors in the Algorithm's :ref:`EnvRunnerGroup <rllib-key-concepts-env-runners>`, and sends as many episode chunks to each
:ref:`Learner <rllib-key-concepts-learners>` actor as required to build a final training batch of exactly size
``config.train_batch_size_per_learner``.

A typical :py:class:`~ray.rllib.env.single_agent_episode.SingleAgentEpisode` object looks something like the following:

.. code-block:: python

    # A SingleAgentEpisode of length 20 has roughly the following schematic structure.
    # Note that after these 20 steps, you have 20 actions and rewards, but 21 observations and info dicts
    # due to the initial "reset" observation/infos.
    episode = {
        'obs': np.ndarray((21, 4), dtype=float32),  # 21 due to reset obs
        'infos': [{}, {}, {}, {}, .., {}, {}],  # infos are always lists of dicts
        'actions': np.ndarray((20,), dtype=int64),  # Discrete(4) action space
        'rewards': np.ndarray((20,), dtype=float32),
        'extra_model_outputs': {
            'action_dist_inputs': np.ndarray((20, 4), dtype=float32),  # Discrete(4) action space
        },
        'is_terminated': False,  # <- single bool
        'is_truncated': True,  # <- single bool
    }

For complex observations, for example ``gym.spaces.Dict``, the episode holds all observations in a struct entirely analogous
to the observation space, with numpy arrays at the leafs of that dict:

.. code-block:: python

    episode_w_complex_observations = {
        'obs': {
            "camera": np.ndarray((21, 64, 64, 3), dtype=float32),  # RGB images
            "sensors": {
                "front": np.ndarray((21, 15), dtype=float32),  # 1D tensors
                "rear": np.ndarray((21, 5), dtype=float32),  # another batch of 1D tensors
            },
        },
        ...

Since all values are kept in numpy arrays, this allows for efficient encoding and transmission across the network.

In `multi-agent mode <rllib-concepts.html#policies-in-multi-agent>`__,
:py:class:`~ray.rllib.env.multi_agent_episode.MultiAgentEpisode` are collected by the EnvRunnerGroup instead.

.. note::
    The Ray team is working on a detailed description of the
    :py:class:`~ray.rllib.env.multi_agent_episode.MultiAgentEpisode` class.


.. _rllib-key-concepts-env-runners:

EnvRunner: Combining RL environment and RLModule
------------------------------------------------

Given the :ref:`RL environment <rllib-key-concepts-environments>` and an :ref:`RLModule <rllib-key-concepts-rlmodules>`,
an :py:class`~ray.rllib.env.env_runner.EnvRunner` produces lists of :ref:`Episodes <rllib-key-concepts-episodes>`.

Thereby, the ``EnvRunner`` executes a classic "environment interaction loop".
Efficient sample collection can be burdensome to get right, especially when leveraging
environment vectorization, stateful (recurrent) neural networks,
or when operating in a multi-agent setting.

RLlib provides two built-in :py:class`~ray.rllib.env.env_runner.EnvRunner` classes,
:py:class`~ray.rllib.env.single_agent_env_runner.SingleAgentEnvRunner` and
:py:class`~ray.rllib.env.multi_agent_env_runner.MultiAgentEnvRunner` that
manage all of this automatically. RLlib picks the correct type based on your
configuration, in particular the `config.environment()` and `config.multi_agent()`
settings. To check, whether your config is multi-agent, call the
:py:meth:`~ray.rllib.algorithms.algorithm_config.AlgorithmConfig.is_multi_agent` method.

RLlib always bundles several EnvRunner actors through the :py:class`~ray.rllib.env.env_runner_group.EnvRunnerGroup` API, but
you can also use an :py:class`~ray.rllib.env.env_runner.EnvRunner` standalone to produce lists of Episodes by calling its
:py:meth:`~ray.rllib.env.env_runner.EnvRunner.sample` method (or ``EnvRunner.sample.remote()`` in the distributed Ray actor setup).

Here is an example of creating a set of remote :py:class:`~ray.rllib.env.env_runner.EnvRunner` actors
and using them to gather experiences in parallel:

.. testcode::

    import tree  # pip install dm_tree
    import ray
    from ray.rllib.algorithms.ppo import PPOConfig
    from ray.rllib.env.single_agent_env_runner import SingleAgentEnvRunner

    # Configure the EnvRunners.
    config = (
        PPOConfig()
        .environment("Acrobot-v1")
        .env_runners(num_env_runners=2, num_envs_per_env_runner=1)
    )
    # Create the EnvRunner actors.
    env_runners = [
        ray.remote(SingleAgentEnvRunner).remote(config=config)
        for _ in range(config.num_env_runners)
    ]

    # Gather lists of `SingleAgentEpisode`s (each EnvRunner actor returns one
    # such list with exactly two episodes in it).
    episodes = ray.get([
        er.sample.remote(num_episodes=3)
        for er in env_runners
    ])
    # Two (remote) EnvRunners used.
    assert len(episodes) == 2
    # Each EnvRunner returns three episodes
    assert all(len(eps_list) == 3 for eps_list in episodes)

    # Report the returns of all episodes collected
    for episode in tree.flatten(episodes):
        print("R=", episode.get_return())


.. _rllib-key-concepts-learners:

Learner: Combining RLModule, loss function and optimizer
--------------------------------------------------------

.. tip::
    The following is a quick overview of what an RLlib **Learner** is.
    See :ref:`here for a detailed description of how to use the Learner class <learner-guide>`.


Given the :ref:`RLModule <rllib-key-concepts-rl-modules>` and one or more optimizers and loss functions,
a :py:class`~ray.rllib.core.learner.learner.Learner` computes losses and gradients, then updates the RLModule.

The input data for such an update step comes in as a list of :ref:`episodes <rllib-key-concepts-episodes>`,
which are converted either by the Learner's own "learner connector" pipeline or by an external one into the
final train batch.

:py:class`~ray.rllib.core.learner.learner.Learner` instances are algorithm-specific, mostly due to the various
loss functions used by different RL algorithms.

RLlib always bundles several :py:class`~ray.rllib.core.learner.learner.Learner` actors through
the :py:class`~ray.rllib.core.learner.learner_group.LearnerGroup` API applying automatic distributed-data parallelism
on the training data in case of more than one Learner actor.
But you can also use a :py:class`~ray.rllib.core.learner.learner.Learner` standalone to update your RLModule
with a lists of Episodes.

Here is an example of creating a single (remote) :py:class:`~ray.rllib.core.learner.learner.Learner`
and calling its :py:meth:`~ray.rllib.core.learner.learner.Learner.update_from_episodes` method.

.. testcode::

    import gymnasium as gym
    import ray
    from ray.rllib.algorithms.ppo import PPOConfig
    from ray.rllib.core.rl_module.default_model_config import DefaultModelConfig

    # Configure the Learner.
    config = (
        PPOConfig()
        .environment("Acrobot-v1")
        .training(lr=0.0001)
        .rl_module(model_config=DefaultModelConfig(fcnet_hiddens=[64, 32]))
    )
    ppo_learner_class = config.get_default_learner_class()
    # Create the Learner actor.
    learner_actor = ray.remote(ppo_learner_class).remote(
        config=config,
        module_spec=config.get_multi_rl_module_spec(env=gym.make("Acrobot-v1")),
    )

    # Build the Learner.
    ray.get(learner_actor.build.remote())

    # Perform an update from the list of episodes we got from the `EnvRunners` above.
    learner_results = ray.get(learner_actor.update_from_episodes.remote(
        episodes=tree.flatten(episodes)
    ))
    print(learner_results["default_policy"]["policy_loss"])
