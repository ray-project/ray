.. image:: https://github.com/ray-project/ray/raw/master/doc/source/images/ray_header_logo.png

.. image:: https://travis-ci.com/ray-project/ray.svg?branch=master
    :target: https://travis-ci.com/ray-project/ray

.. image:: https://readthedocs.org/projects/ray/badge/?version=latest
    :target: http://docs.ray.io/en/latest/?badge=latest

|


**Ray is a fast and simple framework for building and running distributed applications.**

Ray is packaged with the following libraries for accelerating machine learning workloads:

- `Tune`_: Scalable Hyperparameter Tuning
- `RLlib`_: Scalable Reinforcement Learning
- `RaySGD <https://docs.ray.io/en/latest/raysgd/raysgd.html>`__: Distributed Training Wrappers

Install Ray with: ``pip install ray``. For nightly wheels, see the
`Installation page <https://docs.ray.io/en/latest/installation.html>`__.

**NOTE:** `We are deprecating Python 2 support soon.`_

.. _`We are deprecating Python 2 support soon.`: https://github.com/ray-project/ray/issues/6580

Quick Start
-----------

Execute Python functions in parallel.

.. code-block:: python

    import ray
    ray.init()

    @ray.remote
    def f(x):
        return x * x

    futures = [f.remote(i) for i in range(4)]
    print(ray.get(futures))

To use Ray's actor model:

.. code-block:: python


    import ray
    ray.init()

    @ray.remote
    class Counter(object):
        def __init__(self):
            self.n = 0

        def increment(self):
            self.n += 1

        def read(self):
            return self.n

    counters = [Counter.remote() for i in range(4)]
    [c.increment.remote() for c in counters]
    futures = [c.read.remote() for c in counters]
    print(ray.get(futures))


Ray programs can run on a single machine, and can also seamlessly scale to large clusters. To execute the above Ray script in the cloud, just download `this configuration file <https://github.com/ray-project/ray/blob/master/python/ray/autoscaler/aws/example-full.yaml>`__, and run:

``ray submit [CLUSTER.YAML] example.py --start``

Read more about `launching clusters <https://docs.ray.io/en/latest/autoscaling.html>`_.

Tune Quick Start
----------------

.. image:: https://github.com/ray-project/ray/raw/master/doc/source/images/tune-wide.png

`Tune`_ is a library for hyperparameter tuning at any scale.

- Launch a multi-node distributed hyperparameter sweep in less than 10 lines of code.
- Supports any deep learning framework, including PyTorch, TensorFlow, and Keras.
- Visualize results with `TensorBoard <https://www.tensorflow.org/get_started/summaries_and_tensorboard>`__.
- Choose among scalable SOTA algorithms such as `Population Based Training (PBT)`_, `Vizier's Median Stopping Rule`_, `HyperBand/ASHA`_.
- Tune integrates with many optimization libraries such as `Facebook Ax <http://ax.dev>`_, `HyperOpt <https://github.com/hyperopt/hyperopt>`_, and `Bayesian Optimization <https://github.com/fmfn/BayesianOptimization>`_ and enables you to scale them transparently.

To run this example, you will need to install the following:

.. code-block:: bash

    $ pip install ray[tune] torch torchvision filelock


This example runs a parallel grid search to train a Convolutional Neural Network using PyTorch.

.. code-block:: python


    import torch.optim as optim
    from ray import tune
    from ray.tune.examples.mnist_pytorch import (
        get_data_loaders, ConvNet, train, test)


    def train_mnist(config):
        train_loader, test_loader = get_data_loaders()
        model = ConvNet()
        optimizer = optim.SGD(model.parameters(), lr=config["lr"])
        for i in range(10):
            train(model, optimizer, train_loader)
            acc = test(model, test_loader)
            tune.track.log(mean_accuracy=acc)


    analysis = tune.run(
        train_mnist, config={"lr": tune.grid_search([0.001, 0.01, 0.1])})

    print("Best config: ", analysis.get_best_config(metric="mean_accuracy"))

    # Get a dataframe for analyzing trial results.
    df = analysis.dataframe()

If TensorBoard is installed, automatically visualize all trial results:

.. code-block:: bash

    tensorboard --logdir ~/ray_results

.. _`Tune`: https://docs.ray.io/en/latest/tune.html
.. _`Population Based Training (PBT)`: https://docs.ray.io/en/latest/tune-schedulers.html#population-based-training-pbt
.. _`Vizier's Median Stopping Rule`: https://docs.ray.io/en/latest/tune-schedulers.html#median-stopping-rule
.. _`HyperBand/ASHA`: https://docs.ray.io/en/latest/tune-schedulers.html#asynchronous-hyperband

RLlib Quick Start
-----------------

.. image:: https://github.com/ray-project/ray/raw/master/doc/source/images/rllib-wide.jpg

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

.. _`RLlib`: https://docs.ray.io/en/latest/rllib.html


More Information
----------------

- `Documentation`_
- `Tutorial`_
- `Blog`_
- `Ray paper`_
- `Ray HotOS paper`_
- `RLlib paper`_
- `Tune paper`_

.. _`Documentation`: http://docs.ray.io/en/latest/index.html
.. _`Tutorial`: https://github.com/ray-project/tutorial
.. _`Blog`: https://ray-project.github.io/
.. _`Ray paper`: https://arxiv.org/abs/1712.05889
.. _`Ray HotOS paper`: https://arxiv.org/abs/1703.03924
.. _`RLlib paper`: https://arxiv.org/abs/1712.09381
.. _`Tune paper`: https://arxiv.org/abs/1807.05118

Getting Involved
----------------

- `ray-dev@googlegroups.com`_: For discussions about development or any general
  questions.
- `StackOverflow`_: For questions about how to use Ray.
- `GitHub Issues`_: For reporting bugs and feature requests.
- `Pull Requests`_: For submitting code contributions.
- `Meetup Group`_: Join our meetup group.
- `Community Slack`_: Join our Slack workspace.
- `Twitter`_: Follow updates on Twitter.

.. _`ray-dev@googlegroups.com`: https://groups.google.com/forum/#!forum/ray-dev
.. _`GitHub Issues`: https://github.com/ray-project/ray/issues
.. _`StackOverflow`: https://stackoverflow.com/questions/tagged/ray
.. _`Pull Requests`: https://github.com/ray-project/ray/pulls
.. _`Meetup Group`: https://www.meetup.com/Bay-Area-Ray-Meetup/
.. _`Community Slack`: https://forms.gle/9TSdDYUgxYs8SA9e8
.. _`Twitter`: https://twitter.com/raydistributed
