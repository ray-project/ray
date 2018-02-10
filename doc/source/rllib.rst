Ray RLlib: A Scalable Reinforcement Learning Library
====================================================

Ray RLlib is a reinforcement learning library that aims to provide both performance and composability:

- Performance
    - High performance algorithm implementions
    - Pluggable distributed RL execution strategies

- Composability
    - Integration with the `Ray Tune <tune.html>`__ hyperparam tuning tool
    - Support for multiple frameworks (TensorFlow, PyTorch)
    - Scalable primitives for developing new algorithms
    - Shared models between algorithms

You can find the code for RLlib `here on GitHub <https://github.com/ray-project/ray/tree/master/python/ray/rllib>`__, and the NIPS symposium paper `here <https://arxiv.org/abs/1712.09381>`__.

RLlib currently provides the following algorithms:

-  `Proximal Policy Optimization (PPO) <https://arxiv.org/abs/1707.06347>`__ which
   is a proximal variant of `TRPO <https://arxiv.org/abs/1502.05477>`__.

-  `The Asynchronous Advantage Actor-Critic (A3C) <https://arxiv.org/abs/1602.01783>`__.

- `Deep Q Networks (DQN) <https://arxiv.org/abs/1312.5602>`__.

-  Evolution Strategies, as described in `this
   paper <https://arxiv.org/abs/1703.03864>`__. Our implementation
   is adapted from
   `here <https://github.com/openai/evolution-strategies-starter>`__.

These algorithms can be run on any `OpenAI Gym MDP <https://github.com/openai/gym>`__,
including custom ones written and registered by the user.

Installation
------------

RLlib has extra dependencies on top of **ray**. You might also want to clone the Ray repo for convenient access to RLlib helper scripts:

.. code-block:: bash

  pip install 'ray[rllib]'
  git clone https://github.com/ray-project/ray

For usage of PyTorch models, visit the `PyTorch website <http://pytorch.org/>`__
for instructions on installing PyTorch.

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

    python ray/python/ray/rllib/eval.py \
          ~/ray_results/default/DQN_CartPole-v0_0upjmdgr0/checkpoint-1 \
          --run DQN --env CartPole-v0


The ``eval.py`` helper script reconstructs a DQN agent from the checkpoint
located at ``~/ray_results/default/DQN_CartPole-v0_0upjmdgr0/checkpoint-1``
and renders its behavior in the environment specified by ``--env``.

Tuned Examples
--------------

Some good hyperparameters and settings are available in
`the repository <https://github.com/ray-project/ray/blob/master/python/ray/rllib/tuned_examples>`__
(some of them are tuned to run on GPUs). If you find better settings or tune
an algorithm on a different domain, consider submitting a Pull Request!

Python User API
---------------

The Python API provides the needed flexibility for applying RLlib to new problems. You will need to use this API if you wish to use custom environments, preprocesors, or models with RLlib.

Here is an example of the basic usage:

.. code-block:: python

    import ray
    import ray.rllib.ppo as ppo

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

Components: User-customizable and Internal
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The following diagram provides a conceptual overview of data flow between different components in RLlib. We start with an ``Environment``, which given an action produces an observation. The observation is preprocessed by a ``Preprocessor`` and ``Filter`` (e.g. for running mean normalization) before being sent to a neural network ``Model``. The model output is in turn interpreted by an ``ActionDistribution`` to determine the next action.

.. image:: rllib-components.svg

The components highlighted in green above are *User-customizable*, which means RLlib provides APIs for swapping in user-defined implementations, as described in the next sections. The purple components are *RLlib internal*, which means they currently can only be modified by changing the RLlib source code.

For more information about these components, also see the `RLlib Developer Guide <rllib-dev.html>`__.

Custom Environments
~~~~~~~~~~~~~~~~~~~

To train against a custom environment, i.e. one not in the gym catalog, you
can register a function that creates the env to refer to it by name. The contents of the
``env_config`` agent config field will be passed to that function to allow the
environment to be configured. The return type should be an `OpenAI gym.Env <https://github.com/openai/gym/blob/master/gym/core.py>`__. For example:

.. code-block:: python

    import ray
    from ray.tune.registry import register_env
    from ray.rllib import ppo

    def env_creator(env_config):
        import gym
        return gym.make("CartPole-v0")  # or return your own custom env

    env_creator_name = "custom_env"
    register_env(env_creator_name, env_creator)

    ray.init()
    agent = ppo.PPOAgent(env=env_creator_name, config={
        "env_config": {},  # config to pass to env creator
    })

For a code example of a custom env, see the `SimpleCorridor example <https://github.com/ray-project/ray/blob/master/examples/custom_env/custom_env.py>`__. For a more complex example, also see the `Carla RLlib env <https://github.com/ray-project/ray/blob/master/examples/carla/env.py>`__.

Custom Preprocessors and Models
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

RLlib includes default preprocessors and models for common gym
environments, but you can also specify your own as follows. At a high level, your neural
network model needs to take an input tensor of the preprocessed observation shape and
output a vector of the size specified in the constructor. The interfaces for
these custom classes can be found in the
`RLlib Developer Guide <rllib-dev.html>`__.

.. code-block:: python

    import ray
    from ray.rllib.models import ModelCatalog, Model
    from ray.rllib.models.preprocessors import Preprocessor

    class MyPreprocessorClass(Preprocessor):
        def _init(self):
            self.shape = ...

        def transform(self, observation):
            return ...

    class MyModelClass(Model):
        def _init(self, inputs, num_outputs, options):
            layer1 = slim.fully_connected(inputs, 64, ...)
            layer2 = slim.fully_connected(inputs, 64, ...)
            ...
            return layerN, layerN_minus_1

    ModelCatalog.register_custom_preprocessor("my_prep", MyPreprocessorClass)
    ModelCatalog.register_custom_model("my_model", MyModelClass)

    ray.init()
    agent = ppo.PPOAgent(env="CartPole-v0", config={
        "model": {
            "custom_preprocessor": "my_prep",
            "custom_model": "my_model",
            "custom_options": {},  # extra options to pass to your classes
        },
    })

For a full example of a custom model in code, see the `Carla RLlib model <https://github.com/ray-project/ray/blob/master/examples/carla/models.py>`__ and associated `training scripts <https://github.com/ray-project/ray/tree/master/examples/carla>`__. The ``CarlaModel`` class defined there operates over a composite (Tuple) observation space including both images and scalar measurements.

External Data API
~~~~~~~~~~~~~~~~~
*coming soon!*


Using RLlib with Ray Tune
-------------------------

All Agents implemented in RLlib support the
`tune Trainable <tune.html#ray.tune.trainable.Trainable>`__ interface.

Here is an example of using the command-line interface with RLlib:

.. code-block:: bash

    python ray/python/ray/rllib/train.py -f tuned_examples/cartpole-grid-search-example.yaml

Here is an example using the Python API. The same config passed to ``Agents`` may be placed
in the ``config`` section of the experiments.

.. code-block:: python

    import ray
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
        },
        # put additional experiments to run concurrently here
    }

    ray.init()
    run_experiments(experiment)

For an advanced example of using Population Based Training (PBT) with RLlib,
see the `PPO + PBT Walker2D training example <https://github.com/ray-project/ray/blob/master/python/ray/tune/examples/pbt_ppo_example.py>`__.

Contributing to RLlib
---------------------

See the `RLlib Developer Guide <rllib-dev.html>`__.
