RLlib Environments
==================

RLlib works with several different types of environments, including `OpenAI Gym <https://gym.openai.com/>`__, user-defined, multi-agent, and also batched environments.

.. image:: rllib-envs.svg

In the high-level agent APIs, environments are identified with string names. By default, the string will be interpreted as a gym `environment name <https://gym.openai.com/envs>`__, however you can also register custom environments by name:

.. code-block:: python

    import ray
    from ray.tune.registry import register_env
    from ray.rllib.agents import ppo

    def env_creator(env_config):
        import gym
        return gym.make("CartPole-v0")  # or return your own custom env

    register_env("my_env", env_creator)
    ray.init()
    trainer = ppo.PPOAgent(env="my_env", config={
        "env_config": {},  # config to pass to env creator
    })

    while True:
        print(trainer.train())

Configuring Environments
------------------------

In the above example, note that the ``env_creator`` function takes in an ``env_config`` object. This is a dict containing options passed in through your agent. You can also access ``env_config.worker_index`` and ``env_config.vector_index`` to get the worker id and env id within the worker (if ``num_envs_per_worker > 0``). This can be useful if you want to train over an ensemble of different environments, for example:

.. code-block:: python

    class MultiEnv(gym.Env):
        def __init__(self, env_config):
            # pick actual env based on worker and env indexes
            self.env = gym.make(
                choose_env_for(env_config.worker_index, env_config.vector_index))
            self.action_space = self.env.action_space
            self.observation_space = self.env.observation_space
        def reset(self):
            return self.env.reset()
        def step(self, action):
            return self.env.step(action)

    register_env("multienv", lambda config: MultiEnv(config))

OpenAI Gym
----------

RLlib uses Gym as its environment interface for single-agent training. For more information on how to implement a custom Gym environment, see the `gym.Env class definition <https://github.com/openai/gym/blob/master/gym/core.py>`__. You may also find the `SimpleCorridor <https://github.com/ray-project/ray/blob/master/examples/custom_env/custom_env.py>`__ and `Carla simulator <https://github.com/ray-project/ray/blob/master/examples/carla/env.py>`__ example env implementations useful as a reference.

Performance
~~~~~~~~~~~

There are two ways to scale experience collection with Gym environments:

    1. **Vectorization within a single process:** Though many envs can very achieve high frame rates per core, their throughput is limited in practice by policy evaluation between steps. For example, even small TensorFlow models incur a couple milliseconds of latency to evaluate. This can be worked around by creating multiple envs per process and batching policy evaluations across these envs.

      You can configure ``{"num_envs_per_worker": M}`` to have RLlib create ``M`` concurrent environments per worker. RLlib auto-vectorizes Gym environments via `VectorEnv.wrap() <https://github.com/ray-project/ray/blob/master/python/ray/rllib/env/vector_env.py>`__.

    2. **Distribute across multiple processes:** You can also have RLlib create multiple processes (Ray actors) for experience collection. In most algorithms this can be controlled by setting the ``{"num_workers": N}`` config.

.. image:: throughput.png

You can also combine vectorization and distributed execution, as shown in the above figure. Here we plot just the throughput of RLlib policy evaluation from 1 to 128 CPUs. PongNoFrameskip-v4 on GPU scales from 2.4k to ∼200k actions/s, and Pendulum-v0 on CPU from 15k to 1.5M actions/s. One machine was used for 1-16 workers, and a Ray cluster of four machines for 32-128 workers. Each worker was configured with ``num_envs_per_worker=64``.


Vectorized
----------

RLlib will auto-vectorize Gym envs for batch evaluation if the ``num_envs_per_worker`` config is set, or you can define a custom environment class that subclasses `VectorEnv <https://github.com/ray-project/ray/blob/master/python/ray/rllib/env/vector_env.py>`__ to implement ``vector_step()`` and ``vector_reset()``.

Multi-Agent
-----------

A multi-agent environment is one which has multiple acting entities per step, e.g., in a traffic simulation, there may be multiple "car" and "traffic light" agents in the environment. The model for multi-agent in RLlib as follows: (1) as a user you define the number of policies available up front, and (2) a function that maps agent ids to policy ids. This is summarized by the below figure:

.. image:: multi-agent.svg

The environment itself must subclass the `MultiAgentEnv <https://github.com/ray-project/ray/blob/master/python/ray/rllib/env/multi_agent_env.py>`__ interface, which can returns observations and rewards from multiple ready agents per step:

.. code-block:: python

    # Example: using a multi-agent env
    > env = MultiAgentTrafficEnv(num_cars=20, num_traffic_lights=5)

    # Observations are a dict mapping agent names to their obs. Not all agents
    # may be present in the dict in each time step.
    > print(env.reset())
    {
        "car_1": [[...]],
        "car_2": [[...]],
        "traffic_light_1": [[...]],
    }

    # Actions should be provided for each agent that returned an observation.
    > new_obs, rewards, dones, infos = env.step(actions={"car_1": ..., "car_2": ...})

    # Similarly, new_obs, rewards, dones, etc. also become dicts
    > print(rewards)
    {"car_1": 3, "car_2": -1, "traffic_light_1": 0}

    # Individual agents can early exit; env is done when "__all__" = True
    > print(dones)
    {"car_2": True, "__all__": False}

If all the agents will be using the same algorithm class to train, then you can setup multi-agent training as follows:

.. code-block:: python

    trainer = pg.PGAgent(env="my_multiagent_env", config={
        "multiagent": {
            "policy_graphs": {
                "car1": (PGPolicyGraph, car_obs_space, car_act_space, {"gamma": 0.85}),
                "car2": (PGPolicyGraph, car_obs_space, car_act_space, {"gamma": 0.99}),
                "traffic_light": (PGPolicyGraph, tl_obs_space, tl_act_space, {}),
            },
            "policy_mapping_fn":
                lambda agent_id:
                    "traffic_light"  # Traffic lights are always controlled by this policy
                    if agent_id.startswith("traffic_light_")
                    else random.choice(["car1", "car2"])  # Randomly choose from car policies
            },
        },
    })

    while True:
        print(trainer.train())

RLlib will create three distinct policies and route agent decisions to its bound policy. When an agent first appears in the env, ``policy_mapping_fn`` will be called to determine which policy it is bound to. RLlib reports separate training statistics for each policy in the return from ``train()``, along with the combined reward.

Here is a simple `example training script <https://github.com/ray-project/ray/blob/master/python/ray/rllib/examples/multiagent_cartpole.py>`__ in which you can vary the number of agents and policies in the environment. For how to use multiple training methods at once (here DQN and PPO), see the `two-trainer example <https://github.com/ray-project/ray/blob/master/python/ray/rllib/examples/multiagent_two_trainers.py>`__.

To scale to hundreds of agents, MultiAgentEnv batches policy evaluations across multiple agents internally. It can also be auto-vectorized by setting ``num_envs_per_worker > 1``.

Agent-Driven
------------

In many situations, it does not make sense for an environment to be "stepped" by RLlib. For example, if a policy is to be used in a web serving system, then it is more natural for an agent to query a service that serves policy decisions, and for that service to learn from experience over time.

RLlib provides the `ServingEnv <https://github.com/ray-project/ray/blob/master/python/ray/rllib/env/serving_env.py>`__ class for this purpose. Unlike other envs, ServingEnv has its own thread of control. At any point, agents on that thread can query the current policy for decisions via ``self.get_action()`` and reports rewards via ``self.log_returns()``. This can be done for multiple concurrent episodes as well.

For example, ServingEnv can be used to implement a simple REST policy `server <https://github.com/ray-project/ray/tree/master/python/ray/rllib/examples/serving>`__ that learns over time using RLlib. In this example RLlib runs with ``num_workers=0`` to avoid port allocation issues, but in principle this could be scaled by increasing ``num_workers``.

Offline Data
~~~~~~~~~~~~

ServingEnv also provides a ``self.log_action()`` call to support off-policy actions. This allows the client to make independent decisions, e.g., to compare two different policies, and for RLlib to still learn from those off-policy actions. Note that this requires the algorithm used to support learning from off-policy decisions (e.g., DQN).

The ``log_action`` API of ServingEnv can be used to ingest data from offline logs. The pattern would be as follows: First, some policy is followed to produce experience data which is stored in some offline storage system. Then, RLlib creates a number of workers that use a ServingEnv to read the logs in parallel and ingest the experiences. After a round of training completes, the new policy can be deployed to collect more experiences.

Note that envs can read from different partitions of the logs based on the ``worker_index`` attribute of the `env context <https://github.com/ray-project/ray/blob/master/python/ray/rllib/env/env_context.py>`__ passed into the environment constructor.

Batch Asynchronous
------------------

The lowest-level "catch-all" environment supported by RLlib is `AsyncVectorEnv <https://github.com/ray-project/ray/blob/master/python/ray/rllib/env/async_vector_env.py>`__. AsyncVectorEnv models multiple agents executing asynchronously in multiple environments. A call to ``poll()`` returns observations from ready agents keyed by their environment and agent ids, and actions for those agents can be sent back via ``send_actions()``. This interface can be subclassed directly to support batched simulators such as `ELF <https://github.com/facebookresearch/ELF>`__.

Under the hood, all other envs are converted to AsyncVectorEnv by RLlib so that there is a common internal path for policy evaluation.
