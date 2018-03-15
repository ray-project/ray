Ray RLlib: Scalable Reinforcement Learning
==========================================

This README provides a brief technical overview of RLlib. See also the `user documentation <http://ray.readthedocs.io/en/latest/rllib.html>`__ and `paper <https://arxiv.org/abs/1712.09381>`__.

Ray RLlib is an RL execution toolkit built on the Ray distributed execution framework. RLlib implements a collection of distributed *policy optimizers* that make it easy to use a variety of training strategies with existing RL algorithms written in frameworks such as PyTorch, TensorFlow, and Theano. This enables complex architectures for RL training (e.g., Ape-X, IMPALA), to be implemented *once* and reused many times across different RL algorithms and libraries.

RLlib's policy optimizers serve as the basis for RLlib's reference algorithms, which include:

-  `Proximal Policy Optimization (PPO) <https://arxiv.org/abs/1707.06347>`__ which
   is a proximal variant of `TRPO <https://arxiv.org/abs/1502.05477>`__.

-  `The Asynchronous Advantage Actor-Critic (A3C) <https://arxiv.org/abs/1602.01783>`__.

- `Deep Q Networks (DQN) <https://arxiv.org/abs/1312.5602>`__.

- `Ape-X Distributed Prioritized Experience Replay <https://arxiv.org/abs/1803.00933>`__.

-  Evolution Strategies, as described in `this
   paper <https://arxiv.org/abs/1703.03864>`__. Our implementation
   is adapted from
   `here <https://github.com/openai/evolution-strategies-starter>`__.

These algorithms can be run on any OpenAI Gym MDP, including custom ones written and registered by the user.

RLlib's distributed policy optimizers can also be used by any existing algorithm or RL library that implements the policy evaluator interface (optimizers/policy_evaluator.py).


Training API
------------

All RLlib algorithms implement a common training API (agent.py), which enables multiple algorithms to be easily evaluated:

::

    # Train a model on a single environment
    python train.py --env CartPole-v0 --run PPO

    # Integration with ray.tune for hyperparam evaluation
    python train.py -f tuned_examples/cartpole-grid-search-example.yaml

Policy Optimizer abstraction
----------------------------

RLlib's gradient-based algorithms are composed using two abstractions: policy evaluators (optimizers/policy_evaluator.py) and policy optimizers (optimizers/policy_optimizer.py). Policy optimizers serve as the "control plane" of algorithms and implement a particular distributed optimization strategy for RL. Evaluators implement the algorithm "data plane" and encapsulate the model graph. Once an evaluator for an algorithm is implemented, it is compatible with any policy optimizer.

This pluggability enables complex architectures for distributed training to be defined _once_ and reused many times across different algorithms and RL libraries.

These are the currently available optimizers:

-  ``AsyncOptimizer`` is an asynchronous RL optimizer, i.e. like A3C. It asynchronously pulls and applies gradients from evaluators, sending updated weights back as needed.
-  ``LocalSyncOptimizer`` is a simple synchronous RL optimizer. It pulls samples from remote evaluators, concatenates them, and then updates a local model. The updated model weights are then broadcast to all remote evalutaors.
-  ``LocalSyncReplayOptimizer`` adds experience replay to LocalSyncOptimizer (e.g., for DQNs).
-  ``LocalMultiGPUOptimizer`` This optimizer performs SGD over a number of local GPUs, and pins experience data in GPU memory to amortize the copy overhead for multiple SGD passes.
-  ``ApexOptimizer`` This implements the distributed experience replay algorithm for DQN and DDPG and is designed to run in a cluster setting.

Common utilities
----------------

RLlib defines common action distributions, preprocessors, and neural network models, found in ``models/catalog.py``, which are shared by all algorithms. More information on these classes can be found in the `RLlib Developer Guide <http://ray.readthedocs.io/en/latest/rllib-dev.html>`__.
