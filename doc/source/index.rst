Ray
===

.. raw:: html

  <embed>
    <a href="https://github.com/ray-project/ray"><img style="position: absolute; top: 0; right: 0; border: 0;" src="https://camo.githubusercontent.com/365986a132ccd6a44c23a9169022c0b5c890c387/68747470733a2f2f73332e616d617a6f6e6177732e636f6d2f6769746875622f726962626f6e732f666f726b6d655f72696768745f7265645f6161303030302e706e67" alt="Fork me on GitHub" data-canonical-src="https://s3.amazonaws.com/github/ribbons/forkme_right_red_aa0000.png"></a>
  </embed>

*Ray is a fast and simple framework for building and running distributed applications.*

Ray comes with libraries that accelerate deep learning and reinforcement learning development:

- `Tune`_: Scalable Hyperparameter Search
- `RLlib`_: Scalable Reinforcement Learning
- `Distributed Training <distributed_training.html>`__

Install Ray with: ``pip install ray``. For nightly wheels, see the `Installation page <installation.html>`__.

View the `codebase on GitHub`_.

.. _`codebase on GitHub`: https://github.com/ray-project/ray


Quick Start
-----------

.. code-block:: python

    ray.init()

    @ray.remote
    def f(x):
        return x * x

    futures = [f.remote(i) for i in range(4)]
    print(ray.get(futures))

To use Ray's actor model:

.. code-block:: python

    ray.init()

    @ray.remote
    class Counter():
        def __init__(self):
            self.n = 0

        def inc(self):
            self.n += 1

        def read(self):
            return self.n

    counters = [Counter.remote() for i in range(4)]
    [c.increment.remote() for c in counters]
    futures = [c.read.remote() for c in counters]
    print(ray.get(futures))


Ray programs can run on a single machine, and can also seamlessly scale to large clusters. To execute the above Ray script in the cloud, just download `this configuration file <https://github.com/ray-project/ray/blob/master/python/ray/autoscaler/aws/example-full.yaml>`__, and run:

``ray submit [CLUSTER.YAML] example.py --start``

See more details in the `Cluster Launch page <autoscaling.html>`_.

Tune Quick Start
----------------

`Tune`_ is a scalable framework for hyperparameter search built on top of Ray with a focus on deep learning and deep reinforcement learning.

.. code-block:: python

   import torch.optim as optim
   from ray import tune
   from ray.tune.examples.mnist_pytorch import get_data_loaders, Net, train, test

   def train_mnist(config):
       train_loader, test_loader = get_data_loaders()
       model = Net(config)
       optimizer = optim.SGD(model.parameters(), lr=config["lr"])
       for i in range(10):
           train(model, optimizer, train_loader)
           acc = test(model, test_loader)
           tune.track.log(mean_accuracy=acc)

   analysis = tune.run(
       train_mnist,
       stop={"mean_accuracy": 0.98},
       config={"lr": tune.grid_search([0.001, 0.01, 0.1])})

   print("Best config: ", analysis.get_best_config())

.. _`Tune`: tune.html

RLlib Quick Start
-----------------

`RLlib`_ is an open-source library for reinforcement learning built on top of Ray that offers both high scalability and a unified API for a variety of applications.

.. code-block:: bash

  pip install tensorflow  # or tensorflow-gpu
  pip install ray[rllib]  # also recommended: ray[debug]

.. code-block:: python

    import gym
    from gym.spaces import Discrete, Box
    from ray import tune

    class SimpleCorridor(gym.Env):
        def __init__(self, config):
            self.end_pos = config["corridor_length"]
            self.cur_pos = 0
            self.action_space = Discrete(2)
            self.observation_space = Box(0.0, self.end_pos, shape=(1, ))

        def reset(self):
            self.cur_pos = 0
            return [self.cur_pos]

        def step(self, action):
            if action == 0 and self.cur_pos > 0:
                self.cur_pos -= 1
            elif action == 1:
                self.cur_pos += 1
            done = self.cur_pos >= self.end_pos
            return [self.cur_pos], 1 if done else 0, done, {}

    tune.run(
        "PPO",
        config={
            "env": SimpleCorridor,
            "num_workers": 4,
            "env_config": {"corridor_length": 5}})

.. _`RLlib`: rllib.html

Contact
-------
The following are good places to discuss Ray.

1. `ray-dev@googlegroups.com`_: For discussions about development or any general
   questions.
2. `StackOverflow`_: For questions about how to use Ray.
3. `GitHub Issues`_: For bug reports and feature requests.

.. _`ray-dev@googlegroups.com`: https://groups.google.com/forum/#!forum/ray-dev
.. _`GitHub Issues`: https://github.com/ray-project/ray/issues
.. _`StackOverflow`: https://stackoverflow.com/questions/tagged/ray


.. toctree::
   :maxdepth: 1
   :caption: Installation

   installation.rst

.. toctree::
   :maxdepth: 1
   :caption: Using Ray

   walkthrough.rst
   actors.rst
   using-ray-with-gpus.rst
   user-profiling.rst
   inspect.rst
   configure.rst
   advanced.rst
   troubleshooting.rst
   package-ref.rst
   examples.rst

.. toctree::
   :maxdepth: 1
   :caption: Cluster Setup

   autoscaling.rst
   using-ray-on-a-cluster.rst
   deploy-on-kubernetes.rst

.. toctree::
   :maxdepth: 1
   :caption: Tune

   tune.rst
   tune-tutorial.rst
   tune-usage.rst
   tune-distributed.rst
   tune-schedulers.rst
   tune-searchalg.rst
   tune-package-ref.rst
   tune-design.rst
   tune-examples.rst
   tune-contrib.rst

.. toctree::
   :maxdepth: 1
   :caption: RLlib

   rllib.rst
   rllib-training.rst
   rllib-env.rst
   rllib-models.rst
   rllib-algorithms.rst
   rllib-offline.rst
   rllib-concepts.rst
   rllib-examples.rst
   rllib-dev.rst
   rllib-package-ref.rst

.. toctree::
   :maxdepth: 1
   :caption: Experimental

   distributed_training.rst
   pandas_on_ray.rst
   signals.rst
   async_api.rst

.. toctree::
   :maxdepth: 1
   :caption: Examples

   example-rl-pong.rst
   example-parameter-server.rst
   example-newsreader.rst
   example-resnet.rst
   example-a3c.rst
   example-lbfgs.rst
   example-streaming.rst
   using-ray-with-tensorflow.rst

.. toctree::
   :maxdepth: 1
   :caption: Development and Internals

   install-source.rst
   development.rst
   profiling.rst
   internals-overview.rst
   fault-tolerance.rst
   contrib.rst
