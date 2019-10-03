Ray
===

.. raw:: html

  <embed>
    <a href="https://github.com/ray-project/ray"><img style="position: absolute; top: 0; right: 0; border: 0;" src="https://camo.githubusercontent.com/365986a132ccd6a44c23a9169022c0b5c890c387/68747470733a2f2f73332e616d617a6f6e6177732e636f6d2f6769746875622f726962626f6e732f666f726b6d655f72696768745f7265645f6161303030302e706e67" alt="Fork me on GitHub" data-canonical-src="https://s3.amazonaws.com/github/ribbons/forkme_right_red_aa0000.png"></a>
  </embed>

.. image:: https://github.com/ray-project/ray/raw/master/doc/source/images/ray_header_logo.png

**Ray is a fast and simple framework for building and running distributed applications.**

.. tip:: Join our `community slack <https://forms.gle/9TSdDYUgxYs8SA9e8>`_ to discuss Ray!

Ray is packaged with the following libraries for accelerating machine learning workloads:

- `Tune`_: Scalable Hyperparameter Tuning
- `RLlib`_: Scalable Reinforcement Learning
- `Distributed Training <distributed_training.html>`__

Install Ray with: ``pip install ray``. For nightly wheels, see the `Installation page <installation.html>`__.

View the `codebase on GitHub`_.

.. _`codebase on GitHub`: https://github.com/ray-project/ray



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
    class Counter():
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

Visit the `Walkthrough <walkthrough.html>`_ page a more comprehensive overview of Ray features.

Ray programs can run on a single machine, and can also seamlessly scale to large clusters. To execute the above Ray script in the cloud, just download `this configuration file <https://github.com/ray-project/ray/blob/master/python/ray/autoscaler/aws/example-full.yaml>`__, and run:

``ray submit [CLUSTER.YAML] example.py --start``

Read more about `launching clusters <autoscaling.html>`_.

Tune Quick Start
----------------

`Tune`_ is a library for hyperparameter tuning at any scale. With Tune, you can launch a multi-node distributed hyperparameter sweep in less than 10 lines of code. Tune supports any deep learning framework, including PyTorch, TensorFlow, and Keras.

.. note::

    To run this example, you will need to install the following:

    .. code-block:: bash

        $ pip install ray torch torchvision filelock


This example runs a small grid search to train a CNN using PyTorch and Tune.

.. literalinclude:: ../../python/ray/tune/tests/example.py
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


More Information
----------------

- `Tutorial`_
- `Blog`_
- `Ray paper`_
- `Ray HotOS paper`_
- `RLlib paper`_
- `Tune paper`_

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

.. _`ray-dev@googlegroups.com`: https://groups.google.com/forum/#!forum/ray-dev
.. _`GitHub Issues`: https://github.com/ray-project/ray/issues
.. _`StackOverflow`: https://stackoverflow.com/questions/tagged/ray
.. _`Pull Requests`: https://github.com/ray-project/ray/pulls



.. toctree::
   :maxdepth: -1
   :caption: Installation

   installation.rst

.. toctree::
   :maxdepth: -1
   :caption: Using Ray

   walkthrough.rst
   actors.rst
   using-ray-with-gpus.rst
   serialization.rst
   memory-management.rst
   configure.rst
   troubleshooting.rst
   advanced.rst
   package-ref.rst

.. toctree::
   :maxdepth: -1
   :caption: Cluster Setup

   autoscaling.rst
   using-ray-on-a-cluster.rst
   deploy-on-kubernetes.rst
   deploying-on-slurm.rst

.. toctree::
   :maxdepth: -1
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
   :maxdepth: -1
   :caption: RLlib

   rllib.rst
   rllib-toc.rst
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
   :maxdepth: -1
   :caption: Experimental

   distributed_training.rst
   tf_distributed_training.rst
   pandas_on_ray.rst
   projects.rst
   signals.rst
   async_api.rst
   serve.rst

.. toctree::
   :maxdepth: -1
   :caption: Examples

   auto_examples/overview.rst
   auto_examples/plot_lbfgs.rst
   auto_examples/plot_newsreader.rst
   auto_examples/plot_hyperparameter.rst
   auto_examples/plot_pong_example.rst
   auto_examples/plot_resnet.rst
   auto_examples/plot_streaming.rst
   auto_examples/plot_parameter_server.rst
   auto_examples/plot_example-a3c.rst
   using-ray-with-tensorflow.rst
   using-ray-with-pytorch.rst

.. toctree::
   :maxdepth: -1
   :caption: Development and Internals

   development.rst
   profiling.rst
   fault-tolerance.rst
   getting-involved.rst
