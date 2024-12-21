
.. include:: /_includes/rllib/we_are_hiring.rst

.. include:: /_includes/rllib/new_api_stack.rst


.. _rllib-key-concepts:

Key concepts
============

To help you get a high-level understanding of how the library works, on this page, you
learn about the key concepts and general architecture of RLlib.

.. figure:: images/rllib-new-api-stack-simple.svg
    :width: 800
    :align: center

The central component of the library is the :py:class:`~ray.rllib.algorithms.algorithm.Algorithm`
class, acting as a container for all other components required to train your models on an RL problem.

Your gateway into using an :py:class:`~ray.rllib.algorithms.algorithm.Algorithm` is the
:py:class:`~ray.rllib.algorithms.algorithm_config.AlgorithmConfig` (<span style="color: #cfe0e1ff;">**cyan**</span>) class, allowing
you to manage any possible config settings, such as the learning rate, model architecture, compute resources used, or fault-tolerance behavior.
You can then either use your config with Ray Tune or call its :py:meth:`~ray.rllib.algorithms.algorithm_config.AlgorithmConfig.build`
method to construct an :py:class:`~ray.rllib.algorithms.algorithm.Algorithm` instance and work with it directly.

Once built from your config, an :py:class:`~ray.rllib.algorithms.algorithm.Algorithm` contains an
:py:class:`~ray.rllib.env.env_runner_group.EnvRunnerGroup` (<span style="color: #d0e2f3;">**blue**</span>) with distributed, scalable
subcomponents to collect training samples from the RL environment,
a :py:class:`~ray.rllib.core.learner.learner_group.LearnerGroup` (<span style="color: #fff2ccff;">**yellow**</span>) with
distributed, scalable subcomponents to compute gradients and update your models,
and a :py:meth:`~ray.rllib.algorithms.algorithm.Algorithm.training_step` method (<span style="color: #f4ccccff;">**red**</span>), defining
**WHEN** the the algorithm should do **WHAT**.

Copies of the models being trained are located in both :py:class:`~ray.rllib.env.env_runner_group.EnvRunnerGroup`
and :py:class:`~ray.rllib.core.learner.learner_group.LearnerGroup` and the model's weights are synchronized after
model updates.


.. _rllib-key-concepts-algorithms:

Algorithms
----------

The RLlib `Algorithm` class brings all required components together, making learning of different
RL environments accessible through the library's Python APIs.

Each :py:class:`~ray.rllib.algorithms.algorithm.Algorithm` class is managed by its respective
:py:class:`~ray.rllib.algorithms.algorithm_config.AlgorithmConfig` class. For example, to configure a
:py:class:`~ray.rllib.algorithms.ppo.ppo.PPO` ("Proximal Policy Optimization") instance, you should use
the :py:class:`~ray.rllib.algorithms.ppo.ppo.PPOConfig` class.

An algorithm sets up its :py:class:`~ray.rllib.env.env_runner_group.EnvRunnerGroup` and
:py:class:`~ray.rllib.core.learner.learner_group.LearnerGroup`, both of which use `Ray actors <actors.html>`__ to scale sample collection
and training from a single core to many thousands of cores in a cluster.

.. TODO: Separate out our scaling guide into its own page in new PR

See this `scaling guide <rllib-training.html#scaling-guide>`__ for more details here.

:py:class:`~ray.rllib.algorithms.algorithm.Algorithm` also subclasses from the :ref:`Tune Trainable API <tune-60-seconds>`
for easy experiment management and hyperparameter tuning.

You have two ways to interact with an :py:class:`~ray.rllib.algorithms.algorithm.Algorithm`.

- You can create and train an instance of it directly through the Python API.
- You can use Ray Tune to more easily tune the hyperparameters for the particular problem you are trying to optimize.

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


    .. tab-item:: Manage ``Algorithm`` through Ray Tune

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


.. _rllib-key-concepts-rl-modules:

RLModules
---------

`RLModules <rllib-rlmodule.html>`__ are framework-specific neural network containers.

.. TODO: update with new RLModule figure in other PR

.. figure:: images/rllib-blabla.png

    **RLModule overview**: *(left)* A minimal :py:class:`~ray.rllib.core.rl_module.rl_module.RLModule` contains a neural network
    and defines its exploration-, inference- and training logic to map observations to actions.
    *(right)* In more complex setups, a :py:class:`~ray.rllib.core.rl_module.multi_rl_module.MultiRLModule` contains
    many submodules, each itself an :py:class:`~ray.rllib.core.rl_module.rl_module.RLModule` instance and
    identified by a ``ModuleID``. This way, arbitrarily complex multi-model and multi-agent algorithms
    can be implemented.

In a nutshell, ``RLModules`` carry the neural networks and define how to use them during the three phases of a model's
reinforcement learning lifecycle: Exploration (collecting training data), inference (production/deployment),
and training (computing loss function inputs).

.. link to new RLModule docs

You can chose to use :ref:`RLlib's built-in default models and configure these <blabla>` as needed
(change number of layers and size, activation functions, etc..) or :ref:`write your own custom models in PyTorch <blabla>`,
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

The EnvRunner copy is normally kept in an ``inference_only`` version, meaning components not required for
pure action computation (such as a value function) may be missing to save memory.


.. _rllib-key-concepts-environments:

RL environments
---------------

Solving a problem in RL begins with an **environment**. In the simplest definition of RL:

  An **agent** interacts with an **environment** and receives a reward.

An environment in RL is the agent's world, it is a simulation of the problem to be solved.

.. image:: images/env_key_concept1.png

An RLlib environment consists of:

1. all possible actions (**action space**)
2. a complete description of the environment, nothing hidden (**state space**)
3. an observation by the agent of certain parts of the state (**observation space**)
4. **reward**, which is the only feedback the agent receives per action.

The model that tries to maximize the expected sum over all future rewards is called a **policy**. The policy is a function mapping the environment's observations to an action to take, usually written **π** (s(t)) -> a(t). Below is a diagram of the RL iterative learning process.

.. image:: images/env_key_concept2.png

The RL simulation feedback loop repeatedly collects data, for one (single-agent case) or multiple (multi-agent case) policies, trains the policies on these collected data, and makes sure the policies' weights are kept in sync. Thereby, the collected environment data contains observations, taken actions, received rewards and so-called **done** flags, indicating the boundaries of different episodes the agents play through in the simulation.

The simulation iterations of action -> reward -> next state -> train -> repeat, until the end state, is called an **episode**, or in RLlib, a **rollout**.
The most common API to define environments is the `Farama-Foundation Gymnasium <rllib-env.html#gymnasium>`__ API, which we also use in most of our examples.


.. _rllib-key-concepts-episodes:

Episodes
--------

Whether running in a single process or a `large cluster <rllib-training.html#specifying-resources>`__,
all data in RLlib is interchanged in the form of `sample batches <https://github.com/ray-project/ray/blob/master/rllib/policy/sample_batch.py>`__.
Sample batches encode one or more fragments of a trajectory.
Typically, RLlib collects batches of size ``rollout_fragment_length`` from rollout workers, and concatenates one or
more of these batches into a batch of size ``train_batch_size`` that is the input to SGD.

A typical sample batch looks something like the following when summarized.
Since all values are kept in arrays, this allows for efficient encoding and transmission across the network:

.. code-block:: python

    sample_batch = { 'action_logp': np.ndarray((200,), dtype=float32, min=-0.701, max=-0.685, mean=-0.694),
        'actions': np.ndarray((200,), dtype=int64, min=0.0, max=1.0, mean=0.495),
        'dones': np.ndarray((200,), dtype=bool, min=0.0, max=1.0, mean=0.055),
        'infos': np.ndarray((200,), dtype=object, head={}),
        'new_obs': np.ndarray((200, 4), dtype=float32, min=-2.46, max=2.259, mean=0.018),
        'obs': np.ndarray((200, 4), dtype=float32, min=-2.46, max=2.259, mean=0.016),
        'rewards': np.ndarray((200,), dtype=float32, min=1.0, max=1.0, mean=1.0),
        't': np.ndarray((200,), dtype=int64, min=0.0, max=34.0, mean=9.14)
    }

In `multi-agent mode <rllib-concepts.html#policies-in-multi-agent>`__,
sample batches are collected separately for each individual policy.
These batches are wrapped up together in a ``MultiAgentBatch``,
serving as a container for the individual agents' sample batches.


.. _rllib-key-concepts-env-runners:

EnvRunners
----------

Given an RL environment and RLModule, an EnvRunner produces lists of
`Episodes <https://github.com/ray-project/ray/blob/master/rllib/policy/sample_batch.py>`__.
This is your classic "environment interaction loop".
Efficient sample collection can be burdensome to get right, especially when leveraging environment vectorization,
recurrent neural networks, or when operating in a multi-agent setup.

RLlib provides the :py:class`~ray.rllib.env.env_runner.EnvRunner` classes that manage all of this, and most RLlib algorithms
use either the `single-agent- <>`__ or the `multi-agent version <>`__ of it.

You can use an EnvRunner standalone to produce lists of Episodes. This can be done by calling its
:py:meth:`~ray.rllib.env.env_runner.EnvRunner.sample` method (or ``EnvRunner.sample.remote()`` in the distributed, ray actor setup).

Here is an example of creating a set of remote :py:class:`~ray.rllib.env.env_runner.EnvRunner` actors and using them to gather experiences in parallel:

.. testcode::

    import tree  # pip install dm_tree
    from ray.rllib.algorithms.ppo import PPOConfig

    # Configure the EnvRunnerGroup.
    config = (
        PPOConfig()
        .environment("Acrobot-v1")
        .env_runners(num_env_runners=2, num_envs_per_env_runner=1)
    )
    # Create the EnvRunnerGroup.
    env_runner_group = EnvRunnerGroup(config=config)

    # Gather lists of `SingleAgentEpisode`s (each EnvRunner actor returns one
    # such list with exactly 2 episodes in it).
    episodes = env_runner_group.foreach_env_runner(
        lambda env_runner: env_runner.sample(num_episodes=2),
        local_env_runner=False,  # don't sample with the local EnvRunner instance
    )
    # 2 (remote) EnvRunners used.
    assert len(episodes) == 2
    # Each EnvRunner returns 3 episodes
    assert all(len(eps_list) == 3 for eps_list in episodes)

    # Report the returns of all episodes collected
    for episode in tree.flatten(episodes):
        print("R=", episode.get_return())


.. _rllib-key-concepts-learners:

Learners, loss functions and optimizers
---------------------------------------



.. Training Step Method (``Algorithm.training_step()``)
.. Moved to new rllib-algorithm-api.rst file in other PR (branch: `docs_redo_algorithm_api`)
