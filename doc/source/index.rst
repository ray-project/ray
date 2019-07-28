Ray
===

.. raw:: html

  <embed>
    <a href="https://github.com/ray-project/ray"><img style="position: absolute; top: 0; right: 0; border: 0;" src="https://camo.githubusercontent.com/365986a132ccd6a44c23a9169022c0b5c890c387/68747470733a2f2f73332e616d617a6f6e6177732e636f6d2f6769746875622f726962626f6e732f666f726b6d655f72696768745f7265645f6161303030302e706e67" alt="Fork me on GitHub" data-canonical-src="https://s3.amazonaws.com/github/ribbons/forkme_right_red_aa0000.png"></a>
  </embed>

Ray is a fast and simple framework for building and running distributed applications.

Ray comes with libraries that accelerate deep learning and reinforcement learning development:

- `Tune`_: Scalable Hyperparameter Search
- `RLlib`_: Scalable Reinforcement Learning
- `Distributed Training <distributed_training.html>`__

Install Ray with: ``pip install ray``.

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
    ray.get(futures)

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
    ray.get(futures)


To execute the above Ray script on AWS, you can download `this configuration file <https://github.com/ray-project/ray/blob/master/python/ray/autoscaler/aws/example-full.yaml>`__:

``ray submit [CLUSTER.YAML] example.py --start``

See more details in the `Cluster Launch page <autoscaling.html>`_.

Tune Quick Start
----------------

Tune is a scalable framework for hyperparameter search with a focus on deep learning and deep reinforcement learning.

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

RLlib is an open-source library for reinforcement learning that offers both high scalability and a unified API for a variety of applications.

.. code-block:: bash

  pip install tensorflow  # or tensorflow-gpu
  pip install ray[rllib]  # also recommended: ray[debug]

.. code-block:: python

    import ray
    import ray.rllib.agents.ppo as ppo
    from ray.tune.logger import pretty_print

    ray.init()
    config = ppo.DEFAULT_CONFIG.copy()
    config["num_gpus"] = 0
    config["num_workers"] = 1
    trainer = ppo.PPOTrainer(config=config, env="CartPole-v0")

    # Can optionally call trainer.restore(path) to load a checkpoint.

    for i in range(1000):
       # Perform one iteration of training the policy with PPO
       result = trainer.train()
       print(pretty_print(result))

       if i % 100 == 0:
           checkpoint = trainer.save()
           print("checkpoint saved at", checkpoint)

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
   configure.rst
   serialization.rst
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
   :caption: Tune

   tune.rst
   tune-usage.rst
   tune-schedulers.rst
   tune-searchalg.rst
   tune-package-ref.rst
   tune-design.rst
   tune-examples.rst
   tune-contrib.rst

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
