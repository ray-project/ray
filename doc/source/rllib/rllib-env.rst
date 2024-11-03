.. include:: /_includes/rllib/we_are_hiring.rst

.. include:: /_includes/rllib/new_api_stack.rst

.. _rllib-environments-doc:

Environments
============

In online reinforcement learning (RL), an algorithm trains a policy
(neural network) by collecting data on-the-fly using one or more RL
environments or simulators. The agent navigates within these environments choosing actions
governed by this policy. The goal of the algorithm is to train the policy such that its
action choices maximize the cumulative reward over the agent's lifetime.

.. _gymnasium:

Farama Gymnasium
----------------

RLlib relies on `Farama's Gymnasium API <https://gymnasium.farama.org/>`__
as its main environment interface for **single-agent** training. To implement
custom logic with `gymnasium` and integrate it into an RLlib setup, refer to
the `SimpleCorridor example
<https://github.com/ray-project/ray/blob/master/rllib/examples/envs/custom_gym_env.py>`__.

.. tip::

    Not all action spaces are compatible with all RLlib algorithms. See the
    `algorithm overview <rllib-algorithms.html#available-algorithms-overview>`__
    for details. In particular, pay attention to which algorithms support discrete-
    and which support continuous action spaces (or both).

For more on building a custom `Farama Gymnasium
<https://gymnasium.farama.org/>`__ environment, consult the
`gymnasium.Env class definition
<https://github.com/Farama-Foundation/Gymnasium/blob/main/gymnasium/core.py>`__.

For **multi-agent** training, see :ref:`RLlib-supported APIs and RLlib's
multi-agent API <rllib-multi-agent-environments-doc>`.

.. _configuring-environments:

Configuring Environments
------------------------

To specify the RL environment, you can provide either a string name or a
Python class that subclasses `gymnasium.Env
<https://github.com/Farama-Foundation/Gymnasium/blob/main/gymnasium/core.py>`__.

Specifying by String
~~~~~~~~~~~~~~~~~~~~

String values are interpreted as `registered gymnasium environment names
<https://gymnasium.farama.org/>`__ by default.

For example:

.. testcode::
    from ray.rllib.algorithms.ppo import PPOConfig

    config = (
        PPOConfig()
        # Configure the RL environment to use as a string (by name), which
        # is registered with Farama's gymnasium.
        .environment("Acrobot-v1")
    )
    algo = config.build()
    print(algo.train())

.. tip::

    For all supported environment names registered with Farama, refer to these
    resources:

    * `Toy Text <https://gymnasium.farama.org/environments/toy_text/>`__
    * `Classic Control <https://gymnasium.farama.org/environments/classic_control/>`__
    * `Atari <https://gymnasium.farama.org/environments/atari/>`__
    * `MuJoCo <https://gymnasium.farama.org/environments/mujoco/>`__
    * `Box2D <https://gymnasium.farama.org/environments/box2d/>`__

Specifying by Subclass of gymnasium.Env
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

If you are using a custom subclass of `gymnasium.Env class <https://github.com/Farama-Foundation/Gymnasium/blob/main/gymnasium/core.py>`__,
you can pass the class itself rather than a registered string. Your subclass must accept
a single ``config`` argument in the constructor (which may default to `None`).

For example:

.. testcode::

    import gymnasium as gym
    import numpy as np
    from ray.rllib.algorithms.ppo import PPOConfig

    class MyDummyEnv(gym.Env):
        # Write the constructor and provide a single `config` arg
        # (which may be set to None by default).
        def __init__(self, config=None):
            # As per gymnasium standard, provide observation- and action spaces in your
            # constructor.
            self.observation_space = gym.spaces.Box(-1.0, 1.0, (1,), np.float32)
            self.action_space = gym.spaces.Discrete(2)

        def reset(self, seed=None, options=None):
            # Return (reset) observation and info dict.
            return np.array([1.0]), {}

        def step(self, action):
            # Return next observation, reward, terminated, truncated, and info dict.
            return np.array([1.0]), 1.0, False, False, {}

    config = (
        PPOConfig()
        .environment(
            MyDummyEnv,
            env_config={},  # `config` to pass to your env class
        )
    )
    algo = config.build()
    print(algo.train())

Alternatively, you can register an environment creator function (or lambda)
with Ray Tune. This function must take a single ``config`` parameter and
return a `gymnasium.Env
<https://github.com/Farama-Foundation/Gymnasium/blob/main/gymnasium/core.py>`__ instance.

For example:

.. testcode::

    from ray.tune.registry import register_env

    def env_creator(config):
        return MyDummyEnv(config)  # return a gymnasium.Env instance.

    register_env("my_env", env_creator)
    config = (
        PPOConfig()
        .environment("my_env")  # <- Tune registered string pointing to your custom env creator
    )
    algo = config.build()
    print(algo.train())

For a complete example using a custom environment, see the `custom_gym_env.py
example script <https://github.com/ray-project/ray/blob/master/rllib/examples/envs/custom_gym_env.py>`__.

.. warning::

    Due to Ray's distributed nature, gymnasium's own registry is incompatible
    with Ray. Always use the registration methods documented here to ensure
    remote Ray actors can access the environment.

In the example above, the ``env_creator`` function takes a ``config`` argument.
This config is primarily a dictionary containing required constructor options.
However, you can also access additional properties within ``config`` (ex.
``config.worker_index`` to get the remote EnvRunner index or ``config.num_workers``
for the total number of EnvRunners used). This can help customize environments
within an ensemble.

For example:

.. code-block:: python

    class EnvDependingOnWorkerAndVectorIndex(gym.Env):
        def __init__(self, config):
            # pick actual env based on worker and env indexes
            self.env = gym.make(
                choose_env_for(config.worker_index, config.vector_index)
            )
            self.action_space = self.env.action_space
            self.observation_space = self.env.observation_space

        def reset(self, seed, options):
            return self.env.reset(seed, options)

        def step(self, action):
            return self.env.step(action)

    register_env("multi_env", lambda config: MultiEnv(config))

.. tip::

    When using logging within an environment, the configuration must be done
    inside the environment (running within Ray workers). Pre-Ray logging
    configurations will be ignored. Use the following code to connect to Ray's
    logging instance:
    `import logging; logger = logging.getLogger('ray.rllib')`

Performance
~~~~~~~~~~~

.. tip::

    See the `scaling guide <rllib-training.html#scaling-guide>`__ for more
    on RLlib training.

There are two methods to scale sample collection with RLlib and gymnasium environments:

1. **Distribute across multiple processes:** RLlib creates multiple
:py:class:`~ray.rllib.envs.env_runner.EnvRunner` (Ray actors) for experience collection,
controlled through the :py:class:`~ray.rllib.algorithms.algorithm_config.AlgorithmConfig`:
``config.env_runners(num_env_runners=..)``.

2. **Vectorization within a single process:** Many environments achieve high
frame rates per core but are limited by policy evaluation latency. To address
this, create multiple environments per process and thus batch policy evaluations
across these vectorized environments. Set ``config.env_runners(num_envs_per_env_runner=..)``
to create more than one environment per :py:class:`~ray.rllib.envs.env_runner.EnvRunner`
actor.


.. image:: images/throughput.png

Combining vectorization and distributed execution (as in the figure above)
allows for enhanced throughput. For instance, PongNoFrameskip-v4 on GPU
scales from 2.4k to ~200k actions/s, while Pendulum-v1 on CPU scales from
15k to 1.5M actions/s across CPU and GPU setups.

Expensive Environments
~~~~~~~~~~~~~~~~~~~~~~

Some environments may require substantial resources to initialize. RLlib
creates ``num_env_runners + 1`` copies of the environment, as one is needed
for the driver process. To reduce overhead, defer environment initialization
until ``reset()`` is called.

Vectorized
----------

RLlib auto-vectorizes Gym environments for batch evaluation when
``num_envs_per_env_runner`` is set, or you can define a custom environment
class that subclasses `VectorEnv
<https://github.com/ray-project/ray/blob/master/rllib/env/vector_env.py>`__
to implement ``vector_step()`` and ``vector_reset()``.

By default, only policy inference is auto-vectorized. Set ``"remote_worker_envs":
True`` to create environments in Ray actors and step them in parallel. Use
``remote_env_batch_wait_ms`` to control the inference batching level.
