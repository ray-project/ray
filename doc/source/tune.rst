Tune: Scalable Hyperparameter Search
====================================

.. image:: images/tune.png
    :scale: 30%
    :align: center

Tune is a scalable framework for hyperparameter search with a focus on deep learning and deep reinforcement learning. You can find the code for Tune `here on GitHub <https://github.com/ray-project/ray/tree/master/python/ray/tune>`__.

Take a look at `the User Guide <tune-usage.html>`__ for a comprehensive overview on how to use Tune's features.

Features
--------

*  Ease of use: go from running one experiment on a single machine to running on a large distributed cluster without changing your code.

*  Mix and match search execution (Trial Schedulers) and search algorithms to accelerate your training - such as using Model-Based Optimization (HyperOpt) with HyperBand.

*  Integration with visualization tools such as `TensorBoard <https://www.tensorflow.org/get_started/summaries_and_tensorboard>`__, `rllab's VisKit <https://media.readthedocs.org/pdf/rllab/latest/rllab.pdf>`__, and a `parallel coordinates visualization <https://en.wikipedia.org/wiki/Parallel_coordinates>`__.

*  Flexible trial variant generation, including grid search, random search, and conditional parameter distributions.

*  Resource-aware scheduling, including support for concurrent runs of algorithms that may themselves be parallel and distributed and support for trials with varying GPU requirements.

*  Scalable hyperparameter optimization and model search techniques such as:

   -  `Population Based Training (PBT) <tune-schedulers.html#population-based-training-pbt>`__

   -  `Median Stopping Rule <tune-schedulers.html#median-stopping-rule>`__

   -  `HyperBand <tune-schedulers.html#asynchronous-hyperband>`__


Getting Started
---------------

Installation
~~~~~~~~~~~~

You'll need to first `install ray <installation.html>`__ to import Tune.

.. code-block:: bash

    pip install ray


Quick Start
~~~~~~~~~~~

This example runs a small grid search over a neural network training function using Tune, reporting status on the command line until the stopping condition of ``mean_accuracy >= 99`` is reached. Tune works with any deep learning framework.

Tune uses Ray as a backend, so we will first import and initialize Ray.

.. code-block:: python

    import ray
    import ray.tune as tune

    ray.init()


For the function you wish to tune, pass in a ``reporter`` object:

.. code-block:: python
   :emphasize-lines: 1,9

    def train_func(config, reporter):  # add a reporter arg
        model = ( ... )
        optimizer = SGD(model.parameters(),
                        momentum=config["momentum"])
        dataset = ( ... )

        for idx, (data, target) in enumerate(dataset):
            accuracy = model.fit(data, target)
            reporter(mean_accuracy=accuracy) # report metrics

**Finally**, configure your search and execute it on your Ray cluster:

.. code-block:: python

    all_trials = tune.run_experiments({
        "my_experiment": {
            "run": train_func,
            "stop": {"mean_accuracy": 99},
            "config": {"momentum": tune.grid_search([0.1, 0.2])}
        }
    })

Tune can be used anywhere Ray can, e.g. on your laptop with ``ray.init()`` embedded in a Python script, or in an `auto-scaling cluster <autoscaling.html>`__ for massive parallelism.

Citing Tune
-----------

If Tune helps you in your academic research, you are encouraged to cite `our paper <https://arxiv.org/abs/1807.05118>`__. Here is an example bibtex:

.. code-block:: tex

    @article{liaw2018tune,
        title={Tune: A Research Platform for Distributed Model Selection and Training},
        author={Liaw, Richard and Liang, Eric and Nishihara, Robert and Moritz, Philipp and Gonzalez, Joseph E and Stoica, Ion},
        journal={arXiv preprint arXiv:1807.05118},
        year={2018}
    }
