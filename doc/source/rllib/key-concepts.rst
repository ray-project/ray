
.. include:: /_includes/rllib/we_are_hiring.rst

.. include:: /_includes/rllib/new_api_stack.rst


.. _rllib-key-concepts:

Key concepts
============

To help you get a high-level understanding of how the library works, on this page, you learn
about the key concepts and general architecture of RLlib.

.. figure:: images/rllib-new-api-stack-simple.svg
    :width: 800
    :align: center

The central component of the library is the :py:class:`~ray.rllib.algorithms.algorithm.Algorithm`
class, acting as a container for all other components required to train your models.

Your gateway into using an :py:class:`~ray.rllib.algorithms.algorithm.Algorithm` is the
:py:class:`~ray.rllib.algorithms.algorithm_config.AlgorithmConfig` (<span style="color: #cfe0e1ff;">**cyan**</span>) class, allowing
you to manage your config settings (such as the learning rate, model architecture, compute resources used, or fault-tolerance behavior),
and then either use the config with Ray Tune or build the :py:class:`~ray.rllib.algorithms.algorithm_config.AlgorithmConfig` instance
and work with it directly.

Once built from your config, an :py:class:`~ray.rllib.algorithms.algorithm.Algorithm` contains an
:py:class:`~ray.rllib.env.env_runner_group.EnvRunnerGroup` (<span style="color: #d0e2f3;">**blue**</span>) with scalable
subcomponents to collect training samples from the RL environment,
a :py:class:`~ray.rllib.core.learner.learner_group.LearnerGroup` (<span style="color: #fff2ccff;">**yellow**</span>) with
scalable subcomponents to compute gradients and update models,
and a :py:meth:`~ray.rllib.algorithms.algorithm.Algorithm.training_step` method (<span style="color: #f4ccccff;">**red**</span>), defining what
the algorithm should do and when.

Copies of the models being trained are located in both :py:class:`~ray.rllib.env.env_runner_group.EnvRunnerGroup`
and :py:class:`~ray.rllib.core.learner.learner_group.LearnerGroup` and the model's weights are synchronized after
model updates.


.. _rllib-key-concepts-algorithms:

Algorithms
----------

RLlib Algorithms bring all required components together, making learning of different tasks accessible via the library's Python API.
Each :py:class:`~ray.rllib.algorithms.algorithm.Algorithm` class is managed by its respective :py:class:`~ray.rllib.algorithms.algorithm_config.AlgorithmConfig` class,
for example to configure a :py:class:`~ray.rllib.algorithms.ppo.ppo.PPO` instance,
you should use the :py:class:`~ray.rllib.algorithms.ppo.ppo.PPOConfig` class. As a matter of fact, you never have to construct an
:py:class:`~ray.rllib.algorithms.algorithm.Algorithm` instance directly yourself, you can work with the algorithm solely through its
config.

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

    **RLModule overview**: A minimal :py:class:`~ray.rllib.core.rl_module.rl_module.RLModule` contains a neural network
    and defines its exploration-, inference- and training logic to map observations to actions.
    In more complex setups, a :py:class:`~ray.rllib.core.rl_module.multi_rl_module.MultiRLModule` contains
    many submodules, each itself an :py:class:`~ray.rllib.core.rl_module.rl_module.RLModule` instance and
    identified by a ``ModuleID``. This way, arbitrarily complex multi-model and multi-agent algorithms
    can be implemented.

In a nutshell, they carry the neural networks and define how to use them during the three phases of a model's
reinforcement learning lifecycle: Exploration (collecting training data), inference (production/deployment),
and training (computing loss function inputs).

.. link to new RLModule docs

You can chose to use :ref:`RLlib's built-in default models and configure these <blabla>` as needed
(change number of layers and size, activation functions, etc..) or :ref:`write your own custom models in PyTorch <blabla>`
thus implement any architecture and computation logic.

.. figure:: images/rl_modules/rl_module_in_env_runner.svg
    :width: 400

    **

A copy of the user's RLModule is located inside each :py:class:`~ray.rllib.env.env_runner.EnvRunner` actor
managed by the :py:class:`~ray.rllib.env.env_runner_group.EnvRunnerGroup` of the Algorithm and another one in each
:py:class:`~ray.rllib.core.learner.learner.Learner` actor managed by the :py:class:`~ray.rllib.core.learner.learner_group.LearnerGroup`
of the Algorithm.

.. figure:: images/rl_modules/rl_module_in_learner.svg
    :width: 400

    **

The EnvRunner copy is normally kept in an ``inference_only`` version, meaning components not required for
pure action computation (such as a value function) may be missing to save memory.



.. _rllib-key-concepts-environments:

EnvRunners and RL environments
------------------------------

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


Learners, loss functions and optimizers
---------------------------------------


.. _policy-evaluation:

Policy evaluation
-----------------

Given an environment and policy, policy evaluation produces `batches <https://github.com/ray-project/ray/blob/master/rllib/policy/sample_batch.py>`__ of experiences. This is your classic "environment interaction loop". Efficient policy evaluation can be burdensome to get right, especially when leveraging vectorization, RNNs, or when operating in a multi-agent environment. RLlib provides a `RolloutWorker <https://github.com/ray-project/ray/blob/master/rllib/evaluation/rollout_worker.py>`__ class that manages all of this, and this class is used in most RLlib algorithms.

You can use rollout workers standalone to produce batches of experiences. This can be done by calling ``worker.sample()`` on a worker instance, or ``worker.sample.remote()`` in parallel on worker instances created as Ray actors (see `EnvRunnerGroup <https://github.com/ray-project/ray/blob/master/rllib/env/env_runner_group.py>`__).

Here is an example of creating a set of rollout workers and using them gather experiences in parallel. The trajectories are concatenated, the policy learns on the trajectory batch, and then we broadcast the policy weights to the workers for the next round of rollouts:

.. code-block:: python

    # Setup policy and rollout workers.
    env = gym.make("CartPole-v1")
    policy = CustomPolicy(env.observation_space, env.action_space, {})
    workers = EnvRunnerGroup(
        policy_class=CustomPolicy,
        env_creator=lambda c: gym.make("CartPole-v1"),
        num_env_runners=10)

    while True:
        # Gather a batch of samples.
        T1 = SampleBatch.concat_samples(
            ray.get([w.sample.remote() for w in workers.remote_workers()]))

        # Improve the policy using the T1 batch.
        policy.learn_on_batch(T1)

        # The local worker acts as a "parameter server" here.
        # We put the weights of its `policy` into the Ray object store once (`ray.put`)...
        weights = ray.put({"default_policy": policy.get_weights()})
        for w in workers.remote_workers():
            # ... so that we can broacast these weights to all rollout-workers once.
            w.set_weights.remote(weights)


.. Training Step Method (``Algorithm.training_step()``)
.. Moved to new rllib-algorithm-api.rst file in other PR (branch: `docs_redo_algorithm_api`)
