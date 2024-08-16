.. include:: /_includes/rllib/we_are_hiring.rst

.. include:: /_includes/rllib/new_api_stack.rst

.. _rllib-environments-doc:

Environments
============

When running an online reinforcement learning Algorithm, the data to train the policy (neural network) is collected
on-the-fly using one or more simulators, also known as "RL environments".
The to-be-trained agent, whose actions are controlled by the policy, is able to walk around in the environments by choosing various actions,
thereby trying to explore which tactics and strategies yield - in the long run - the highest sum of rewards over the agent's lifetime.


.. _gymnasium:

Farama Gymnasium
----------------

RLlib uses `Farama's Gymnasium API <https://gymnasium.farama.org/>`__ as its only environment interface for **single-agent** training.
You may find the `SimpleCorridor example <https://github.com/ray-project/ray/blob/master/rllib/examples/envs/custom_gym_env.py>`__
useful as a reference on how to implement your own custom logic with `gymnasium` and then plug your environment class into an RLlib
setup for running your experiments.

.. tip::

    Not all environments' action spaces work with all of RLlib's algorithms.
    See the `algorithm overview <rllib-algorithms.html#available-algorithms-overview>`__ for more information.

For more information on how to implement a custom `Farama Gymnasium <https://gymnasium.farama.org/>`__ environment, see the
`gymnasium.Env class definition <https://github.com/Farama-Foundation/Gymnasium/blob/main/gymnasium/core.py>`__.

For **multi-agent** training, see :ref:`here for the RLlib-supported APIs and RLlib's own multi-agent API <rllib-multi-agent-environments-doc>`.


.. _configuring-environments:

Configuring Environments
------------------------

You can pass either a string name or a Python class (subclass of
`gymnasium.Env <https://github.com/Farama-Foundation/Gymnasium/blob/main/gymnasium/core.py>`__) to specify the RL environment
to be trained against.

Specifying by string
~~~~~~~~~~~~~~~~~~~~

By default, string values will be interpreted as `gymnasium environment names <https://gymnasium.farama.org/>`__.

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

    For all supported environment names, registered in Farama, see these links here:

    * `Toy Text <https://gymnasium.farama.org/environments/toy_text/>`__
    * `Classic Control <https://gymnasium.farama.org/environments/classic_control/>`__
    * `Atari <https://gymnasium.farama.org/environments/atari/>`__
    * `MuJoCo <https://gymnasium.farama.org/environments/mujoco/>`__
    * `Box2D <https://gymnasium.farama.org/environments/box2d/>`__


Specifying by subclass of gymnasium.Env
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

In case you configure your environment as a (custom) subclass of the
`gymnasium.Env class <https://github.com/Farama-Foundation/Gymnasium/blob/main/gymnasium/core.py>`__,
you can pass your class instead of the registered string. Note that your custom subclass must accept a
single ``config`` argument in the constructor.

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


Alternatively to providing your `gymnasium.Env <https://github.com/Farama-Foundation/Gymnasium/blob/main/gymnasium/core.py>`__ subclass
directly, you can register a "env creator" function (or lambda) with Ray Tune under a name (str).
Your env creator must take the same single ``config`` parameter and return an instance of
`gymnasium.Env <https://github.com/Farama-Foundation/Gymnasium/blob/main/gymnasium/core.py>`__.

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


For a full runnable code example using the custom environment API, see the `custom_gym_env.py example script <https://github.com/ray-project/ray/blob/master/rllib/examples/envs/custom_gym_env.py>`__.

.. warning::

    Due the distributed nature of Ray, the gymnasium registry isn't compatible with Ray.
    Instead, always use the registration flows documented above to ensure all remote Ray actors can access the environment.

In the above example, note that the ``env_creator`` function takes a ``config`` arg. This should always be a dict containing any needed constructor options.
In your custom ``env_creator`` function, however, you can also access additional properties (not keys) in this ``config``, for example ``config.worker_index``
to get the index of the remote EnvRunner (starting from 1 and going up to your configured ``num_env_runners``), ``config.vector_index`` to get the worker
env id within the worker (if ``num_envs_per_env_runner > 0``), as well as ``config.num_workers`` to get the total number of remote EnvRunners used.
This can be useful if you want to train over an ensemble of different environments and would like for individual environment copies to behave slightly
differently.
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

    When using logging inside an environment, the logging configuration needs to be done inside the environment,
    which runs inside Ray workers. Any configurations outside of the environment, e.g., before starting Ray will be ignored.
    Use the following code snippet to connect to the Ray logging instance from within your code:
    `import logging; logger = logging.getLogger('ray.rllib')`


Performance
~~~~~~~~~~~

.. tip::

    Also check out the `scaling guide <rllib-training.html#scaling-guide>`__ for RLlib training.

There are two ways to scale sample collection with Gym environments:

    1. **Vectorization within a single process:** Though many envs can achieve high frame rates per core, their throughput is limited in practice by policy evaluation between steps. For example, even small TensorFlow models incur a couple milliseconds of latency to evaluate. This can be worked around by creating multiple envs per process and batching policy evaluations across these envs.

      You can configure ``{"num_envs_per_env_runner": M}`` to have RLlib create ``M`` concurrent environments per worker. RLlib auto-vectorizes Gym environments via `VectorEnv.wrap() <https://github.com/ray-project/ray/blob/master/rllib/env/vector_env.py>`__.

    2. **Distribute across multiple processes:** You can also have RLlib create multiple processes (Ray actors) for experience collection. In most algorithms this can be controlled by setting the ``{"num_env_runners": N}`` config.

.. image:: images/throughput.png

You can also combine vectorization and distributed execution, as shown in the above figure. Here we plot just the throughput of RLlib policy evaluation from 1 to 128 CPUs. PongNoFrameskip-v4 on GPU scales from 2.4k to ∼200k actions/s, and Pendulum-v1 on CPU from 15k to 1.5M actions/s. One machine was used for 1-16 workers, and a Ray cluster of four machines for 32-128 workers. Each worker was configured with ``num_envs_per_env_runner=64``.

Expensive Environments
~~~~~~~~~~~~~~~~~~~~~~

Some environments may be very resource-intensive to create. RLlib will create ``num_env_runners + 1`` copies of the environment since one copy is needed for the driver process. To avoid paying the extra overhead of the driver copy, which is needed to access the env's action and observation spaces, you can defer environment initialization until ``reset()`` is called.


Vectorized
----------

RLlib will auto-vectorize Gym envs for batch evaluation if the ``num_envs_per_env_runner`` config is set, or you can define a custom environment class that subclasses `VectorEnv <https://github.com/ray-project/ray/blob/master/rllib/env/vector_env.py>`__ to implement ``vector_step()`` and ``vector_reset()``.

Note that auto-vectorization only applies to policy inference by default. This means that policy inference will be batched, but your envs will still be stepped one at a time. If you would like your envs to be stepped in parallel, you can set ``"remote_worker_envs": True``. This will create env instances in Ray actors and step them in parallel. These remote processes introduce communication overheads, so this only helps if your env is very expensive to step / reset.

When using remote envs, you can control the batching level for inference with ``remote_env_batch_wait_ms``. The default value of 0ms means envs execute asynchronously and inference is only batched opportunistically. Setting the timeout to a large value will result in fully batched inference and effectively synchronous environment stepping. The optimal value depends on your environment step / reset time, and model inference speed.
