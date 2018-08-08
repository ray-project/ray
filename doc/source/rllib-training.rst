RLlib Training APIs
===================

Getting Started
---------------

At a high level, RLlib provides an ``Agent`` class which
holds a policy for environment interaction. Through the agent interface, the policy can
be trained, checkpointed, or an action computed.

.. image:: rllib-api.svg

You can train a simple DQN agent with the following command

.. code-block:: bash

    python ray/python/ray/rllib/train.py --run DQN --env CartPole-v0

By default, the results will be logged to a subdirectory of ``~/ray_results``.
This subdirectory will contain a file ``params.json`` which contains the
hyperparameters, a file ``result.json`` which contains a training summary
for each episode and a TensorBoard file that can be used to visualize
training process with TensorBoard by running

.. code-block:: bash

     tensorboard --logdir=~/ray_results


The ``train.py`` script has a number of options you can show by running

.. code-block:: bash

    python ray/python/ray/rllib/train.py --help

The most important options are for choosing the environment
with ``--env`` (any OpenAI gym environment including ones registered by the user
can be used) and for choosing the algorithm with ``--run``
(available options are ``PPO``, ``PG``, ``A3C``, ``IMPALA``, ``ES``, ``DDPG``, ``DQN``, ``APEX``, and ``APEX_DDPG``).

Specifying Parameters
~~~~~~~~~~~~~~~~~~~~~

Each algorithm has specific hyperparameters that can be set with ``--config``, in addition to a number of `common hyperparameters <https://github.com/ray-project/ray/blob/master/python/ray/rllib/agents/agent.py>`__. See the
`algorithms documentation <rllib-algorithms.html>`__ for more information.

In an example below, we train A3C by specifying 8 workers through the config flag.
function that creates the env to refer to it by name. The contents of the env_config agent config field will be passed to that function to allow the environment to be configured. The return type should be an OpenAI gym.Env. For example:


.. code-block:: bash

    python ray/python/ray/rllib/train.py --env=PongDeterministic-v4 \
        --run=A3C --config '{"num_workers": 8}'

Evaluating Trained Agents
~~~~~~~~~~~~~~~~~~~~~~~~~

In order to save checkpoints from which to evaluate agents,
set ``--checkpoint-freq`` (number of training iterations between checkpoints)
when running ``train.py``.


An example of evaluating a previously trained DQN agent is as follows:

.. code-block:: bash

    python ray/python/ray/rllib/rollout.py \
          ~/ray_results/default/DQN_CartPole-v0_0upjmdgr0/checkpoint-1 \
          --run DQN --env CartPole-v0

The ``rollout.py`` helper script reconstructs a DQN agent from the checkpoint
located at ``~/ray_results/default/DQN_CartPole-v0_0upjmdgr0/checkpoint-1``
and renders its behavior in the environment specified by ``--env``.

Tuned Examples
~~~~~~~~~~~~~~

Some good hyperparameters and settings are available in
`the repository <https://github.com/ray-project/ray/blob/master/python/ray/rllib/tuned_examples>`__
(some of them are tuned to run on GPUs). If you find better settings or tune
an algorithm on a different domain, consider submitting a Pull Request!

You can run these with the ``train.py`` script as follows:

.. code-block:: bash

    python ray/python/ray/rllib/train.py -f /path/to/tuned/example.yaml

Python API
----------

The Python API provides the needed flexibility for applying RLlib to new problems. You will need to use this API if you wish to use custom environments, preprocesors, or models with RLlib.

Here is an example of the basic usage:

.. code-block:: python

    import ray
    import ray.rllib.agents.ppo as ppo

    ray.init()
    config = ppo.DEFAULT_CONFIG.copy()
    agent = ppo.PPOAgent(config=config, env="CartPole-v0")

    # Can optionally call agent.restore(path) to load a checkpoint.

    for i in range(1000):
       # Perform one iteration of training the policy with PPO
       result = agent.train()
       print("result: {}".format(result))

       if i % 100 == 0:
           checkpoint = agent.save()
           print("checkpoint saved at", checkpoint)

.. note::

    It's recommended that you run RLlib agents with `Tune <tune.html>`__, for easy experiment management and visualization of results. Just set ``"run": AGENT_NAME, "env": ENV_NAME`` in the experiment config.

All RLlib agents are compatible with the `Tune API <tune.html#concepts>`__. This enables them to be easily used in experiments with `Tune <tune.html>`__. For example, the following code performs a simple hyperparam sweep of PPO:

.. code-block:: python

    import ray
    import ray.tune as tune

    ray.init()
    tune.run_experiments({
        "my_experiment": {
            "run": "PPO",
            "env": "CartPole-v0",
            "stop": {"episode_reward_mean": 200},
            "config": {
                "num_workers": 1,
                "sgd_stepsize": tune.grid_search([0.01, 0.001, 0.0001]),
            },
        },
    })

Tune will schedule the trials to run in parallel on your Ray cluster:

::

    == Status ==
    Using FIFO scheduling algorithm.
    Resources requested: 4/4 CPUs, 0/0 GPUs
    Result logdir: /home/eric/ray_results/my_experiment
    PENDING trials:
     - PPO_CartPole-v0_2_sgd_stepsize=0.0001:	PENDING
    RUNNING trials:
     - PPO_CartPole-v0_0_sgd_stepsize=0.01:	RUNNING [pid=21940], 16 s, 4013 ts, 22 rew
     - PPO_CartPole-v0_1_sgd_stepsize=0.001:	RUNNING [pid=21942], 27 s, 8111 ts, 54.7 rew

Accessing Global State
~~~~~~~~~~~~~~~~~~~~~~
It is common to need to access an agent's internal state, e.g., to set or get internal weights. In RLlib an agent's state is replicated across multiple *policy evaluators* (Ray actors) in the cluster. However, you can easily get and update this state between calls to ``train()`` via ``agent.optimizer.foreach_evaluator()`` or ``agent.optimizer.foreach_evaluator_with_index()``. These functions take a lambda function that is applied with the evaluator as an arg. You can also return values from these functions and those will be returned as a list.

You can also access just the "master" copy of the agent state through ``agent.optimizer.local_evaluator``, but note that updates here may not be reflected in remote replicas if you have configured ``num_workers > 0``.

REST API
--------

In some cases (i.e., when interacting with an external environment) it makes more sense to interact with RLlib as if were an independently running service, rather than RLlib hosting the simulations itself. This is possible via RLlib's serving env `interface <rllib-envs.html#serving>`__.

.. autoclass:: ray.rllib.utils.policy_client.PolicyClient
    :members:

.. autoclass:: ray.rllib.utils.policy_server.PolicyServer
    :members:

For a full client / server example that you can run, see the example `client script <https://github.com/ray-project/ray/blob/master/python/ray/rllib/examples/serving/cartpole_client.py>`__ and also the corresponding `server script <https://github.com/ray-project/ray/blob/master/python/ray/rllib/examples/serving/cartpole_server.py>`__, here configured to serve a policy for the toy CartPole-v0 environment.
