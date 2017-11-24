RLLib: A Scalable Reinforcement Learning Library
================================================

This document describes Ray's reinforcement learning library.
It currently supports the following algorithms:

-  `Proximal Policy Optimization <https://arxiv.org/abs/1707.06347>`__ which
   is a proximal variant of `TRPO <https://arxiv.org/abs/1502.05477>`__.

-  Evolution Strategies which is decribed in `this
   paper <https://arxiv.org/abs/1703.03864>`__. Our implementation
   borrows code from
   `here <https://github.com/openai/evolution-strategies-starter>`__.

-  `The Asynchronous Advantage Actor-Critic <https://arxiv.org/abs/1602.01783>`__
   based on `the OpenAI starter agent <https://github.com/openai/universe-starter-agent>`__.

- `Deep Q Network (DQN) <https://arxiv.org/abs/1312.5602>`__.

Proximal Policy Optimization scales to hundreds of cores and several GPUs,
Evolution Strategies to clusters with thousands of cores and
the Asynchronous Advantage Actor-Critic scales to dozens of cores
on a single node.

These algorithms can be run on any `OpenAI Gym MDP <https://github.com/openai/gym>`__,
including custom ones written and registered by the user.

Getting Started
---------------

You can train an example DQN agent with the following command

::

    python ray/python/ray/rllib/train.py --run DQN --env CartPole-v0

By default, the results will be logged to a subdirectory of ``/tmp/ray``.
This subdirectory will contain a file ``params.json`` which contains the
hyperparameters, a file ``result.json`` which contains a training summary
for each episode and a TensorBoard file that can be used to visualize
training process with TensorBoard by running

::

     tensorboard --logdir=/tmp/ray


The ``train.py`` script has a number of options you can show by running

::

    python ray/python/ray/rllib/train.py --help

The most important options are for choosing the environment
with ``--env`` (any OpenAI gym environment including ones registered by the user
can be used) and for choosing the algorithm with ``--run``
(available options are ``PPO``, ``A3C``, ``ES`` and ``DQN``).

Specifying Parameters
~~~~~~~~~~~~~~~~~~~~~

Each algorithm has specific hyperparameters that can be set with ``--config`` - see the
``DEFAULT_CONFIG`` variable in
`PPO <https://github.com/ray-project/ray/blob/master/python/ray/rllib/ppo/ppo.py>`__,
`A3C <https://github.com/ray-project/ray/blob/master/python/ray/rllib/a3c/a3c.py>`__,
`ES <https://github.com/ray-project/ray/blob/master/python/ray/rllib/es/es.py>`__ and
`DQN <https://github.com/ray-project/ray/blob/master/python/ray/rllib/dqn/dqn.py>`__.

In an example below, we train A3C by specifying 8 workers through the config flag.
::

    python ray/python/ray/rllib/train.py --env=PongDeterministic-v4 --run=A3C --config '{"num_workers": 8}'

Tuned Examples
--------------

Some good hyperparameters and settings are available in
`the repository <https://github.com/ray-project/ray/blob/master/python/ray/rllib/test/tuned_examples.sh>`__
(some of them are tuned to run on GPUs). If you find better settings or tune
an algorithm on a different domain, consider submitting a Pull Request!

The User API
------------

You will be using this part of the API if you run the existing algorithms
on a new problem. Note that the API is not considered to be stable yet.
Here is an example how to use it:

::

    import ray
    import ray.rllib.ppo as ppo

    ray.init()

    config = ppo.DEFAULT_CONFIG.copy()
    alg = ppo.PPOAgent(config=config, env="CartPole-v1")

    # Can optionally call alg.restore(path) to load a checkpoint.

    for i in range(10):
       # Perform one iteration of the algorithm.
       result = alg.train()
       print("result: {}".format(result))
       print("checkpoint saved at path: {}".format(alg.save()))

Custom Environments
~~~~~~~~~~~~~~~~~~~

To train against a custom environment, i.e. one not in the gym catalog, you
can pass a function that returns an env instead of an env id. For example:

::

    import ray
    from ray.tune.registry import get_registry, register_env
    from ray.rllib import ppo

    env_creator = lambda: create_my_env()
    env_creator_key = "custom_env"
    register_env(env_creator_key, env_creator)

    ray.init()
    alg = ppo.PPOAgent(env=env_creator_key, registry=get_registry())

The Developer API
-----------------

This part of the API will be useful if you need to change existing RL algorithms
or implement new ones. Note that the API is not considered to be stable yet.

Agents
~~~~~~

Agents implement a particular algorithm and can be used to run
some number of iterations of the algorithm, save and load the state
of training and evaluate the current policy. All agents inherit from
a common base class:

.. autoclass:: ray.rllib.agent.Agent
    :members:

Models
~~~~~~

Models are subclasses of the Model class:

.. autoclass:: ray.rllib.models.Model

Currently we support fully connected and convolutional TensorFlow policies on all algorithms:

.. autofunction:: ray.rllib.models.FullyConnectedNetwork
.. autofunction:: ray.rllib.models.ConvolutionalNetwork

A3C also supports a TensorFlow LSTM policy.

.. autofunction:: ray.rllib.models.LSTM

Action Distributions
~~~~~~~~~~~~~~~~~~~~

Actions can be sampled from different distributions which have a common base
class:

.. autoclass:: ray.rllib.models.ActionDistribution
    :members:

Currently we support the following action distributions:

.. autofunction:: ray.rllib.models.Categorical
.. autofunction:: ray.rllib.models.DiagGaussian
.. autofunction:: ray.rllib.models.Deterministic

The Model Catalog
~~~~~~~~~~~~~~~~~

The Model Catalog is a mechanism for picking good default values for
various gym environments. Here is an example usage:
::

    dist_class, dist_dim = ModelCatalog.get_action_dist(env.action_space)
    model = ModelCatalog.get_model(inputs, dist_dim)
    dist = dist_class(model.outputs)
    action_op = dist.sample()


.. autoclass:: ray.rllib.models.ModelCatalog
    :members:

Using RLLib on a cluster
------------------------

First create a cluster as described in `managing a cluster with parallel ssh`_.
You can then run RLLib on this cluster by passing the address of the main redis
shard into ``train.py`` with ``--redis-address``.

Using RLLib with Ray.tune
-------------------------

All Agents implemented in RLLib support the
`Trainable <http://ray.readthedocs.io/en/latest/tune.html#ray.tune.trainable.Trainable>`__ interface.

Here is an example of using Ray.tune with RLLib:

::

    python ray/python/ray/rllib/train.py -f tuned_examples/cartpole-grid-search-example.yaml

Here is an example using the Python API.

::

    from ray.tune.tune import run_experiments
    from ray.tune.variant_generator import grid_search


    experiment = {
        'cartpole-ppo': {
            'run': 'PPO',
            'env': 'CartPole-v0',
            'resources': {
                'cpu': 2,
                'driver_cpu_limit': 1},
            'stop': {
                'episode_reward_mean': 200,
                'time_total_s': 180
            },
            'config': {
                'num_sgd_iter': grid_search([1, 4]),
                'num_workers': 2,
                'sgd_batchsize': grid_search([128, 256, 512])
            }
        }
    }

    run_experiments(experiment)

.. _`managing a cluster with parallel ssh`: http://ray.readthedocs.io/en/latest/using-ray-on-a-large-cluster.html
