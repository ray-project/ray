.. _gentle-intro:

============================
A Gentle Introduction to Ray
============================

.. include:: basics.rst

This tutorial will provide a tour of the core features of Ray.

First, install Ray with: ``pip install ray``, and now we can execute some Python in parallel.

Parallelizing Python Functions with Ray Tasks
=============================================

First, import ray and ``init`` the Ray service.
Then decorate your function with ``@ray.remote`` to declare that you want to run this function
remotely. Lastly, call that function with ``.remote()`` instead of calling it normally. This remote call yields a future, or ``ObjectRef`` that you can then
fetch with ``ray.get``.

.. code-block:: python

    import ray
    ray.init()

    @ray.remote
    def f(x):
        return x * x

    futures = [f.remote(i) for i in range(4)]
    print(ray.get(futures)) # [0, 1, 4, 9]

In the above code block we defined some Ray Tasks. While these are great for stateless operations, sometimes you
must maintain the state of your application. You can do that with Ray Actors.

Parallelizing Python Classes with Ray Actors
==============================================

Ray provides actors to allow you to parallelize an instance of a class in Python.
When you instantiate a class that is a Ray actor, Ray will start a remote instance
of that class in the cluster. This actor can then execute remote method calls and
maintain its own internal state.

.. code-block:: python

    import ray
    ray.init() # Only call this once.

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
    print(ray.get(futures)) # [1, 1, 1, 1]


An Overview of the Ray Libraries
================================

Ray has a rich ecosystem of libraries and frameworks built on top of it. The main ones being:

- :doc:`../tune/index`
- :ref:`rllib-index`
- :ref:`sgd-index`
- :ref:`rayserve`


Tune Quick Start
----------------

`Tune`_ is a library for hyperparameter tuning at any scale. With Tune, you can launch a multi-node distributed hyperparameter sweep in less than 10 lines of code. Tune supports any deep learning framework, including PyTorch, TensorFlow, and Keras.

.. note::

    To run this example, you will need to install the following:

    .. code-block:: bash

        $ pip install 'ray[tune]'


This example runs a small grid search with an iterative training function.

.. literalinclude:: ../../../python/ray/tune/tests/example.py
   :language: python
   :start-after: __quick_start_begin__
   :end-before: __quick_start_end__

If TensorBoard is installed, automatically visualize all trial results:

.. code-block:: bash

    tensorboard --logdir ~/ray_results

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

Where to go next?
=================


Visit the `Walkthrough <walkthrough.html>`_ page a more comprehensive overview of Ray features.

Ray programs can run on a single machine, and can also seamlessly scale to large clusters. To execute the above Ray script in the cloud, just download `this configuration file <https://github.com/ray-project/ray/blob/master/python/ray/autoscaler/aws/example-full.yaml>`__, and run:

``ray submit [CLUSTER.YAML] example.py --start``

Read more about `:ref:launching clusters <cluster-index>`.
