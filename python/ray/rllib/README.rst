Ray RLlib: A Scalable Reinforcement Learning Library
====================================================

This README provides a brief technical overview of RLlib. See also the `user documentation <http://ray.readthedocs.io/en/latest/rllib.html>`__ and `NIPS symposium paper <https://arxiv.org/abs/1712.09381>`__.

RLlib currently provides the following algorithms:

-  `Proximal Policy Optimization (PPO) <https://arxiv.org/abs/1707.06347>`__ which
   is a proximal variant of `TRPO <https://arxiv.org/abs/1502.05477>`__.

-  `The Asynchronous Advantage Actor-Critic (A3C) <https://arxiv.org/abs/1602.01783>`__.

- `Deep Q Networks (DQN) <https://arxiv.org/abs/1312.5602>`__.

-  Evolution Strategies, as described in `this
   paper <https://arxiv.org/abs/1703.03864>`__. Our implementation
   is adapted from
   `here <https://github.com/openai/evolution-strategies-starter>`__.

These algorithms can be run on any OpenAI Gym MDP, including custom ones written and registered by the user.


Training API
------------

All RLlib algorithms implement a common training API (agent.py), which enables multiple algorithms to be easily evaluated:

::

    # Train a model on a single environment
    python train.py --env CartPole-v0 --run PPO

    # Integration with ray.tune for hyperparam evaluation
    python train.py -f tuned_examples/cartpole-grid-search-example.yaml

Policy Evaluator and Optimizer abstractions
-------------------------------------------

RLlib's gradient-based algorithms are composed using two abstractions: Evaluators (evaluator.py) and Optimizers (optimizers/optimizer.py). Optimizers encapsulate a particular distributed optimization strategy for RL. Evaluators encapsulate the model graph, and once implemented, any Optimizer may be "plugged in" to any algorithm that implements the Evaluator interface.

This pluggability enables optimization strategies to be re-used and improved across different algorithms and deep learning frameworks (RLlib's optimizers work with both TensorFlow and PyTorch, though currently only A3C has a PyTorch graph implementation).

These are the currently available optimizers:

-  ``AsyncOptimizer`` is an asynchronous RL optimizer, i.e. like A3C. It asynchronously pulls and applies gradients from evaluators, sending updated weights back as needed.
-  ``LocalSyncOptimizer`` is a simple synchronous RL optimizer. It pulls samples from remote evaluators, concatenates them, and then updates a local model. The updated model weights are then broadcast to all remote evalutaors.
-  ``LocalMultiGPUOptimizer`` (currently available for PPO) This optimizer performs SGD over a number of local GPUs, and pins experience data in GPU memory to amortize the copy overhead for multiple SGD passes.
-  ``AllReduceOptimizer`` (planned) This optimizer would use the Allreduce primitive to scalably synchronize weights among a number of remote GPU workers.

Common utilities
----------------

RLlib defines common action distributions, preprocessors, and neural network models, found in ``models/catalog.py``, which are shared by all algorithms. More information on these classes can be found in the `RLlib Developer Guide <http://ray.readthedocs.io/en/latest/rllib-dev.html>`__.
