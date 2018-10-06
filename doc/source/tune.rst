Tune: Scalable Hyperparameter Search
====================================

.. image:: images/tune.png
    :scale: 30%
    :align: center

Tune is a scalable framework for hyperparameter search with a focus on deep learning and deep reinforcement learning.

You can find the code for Tune `here on GitHub <https://github.com/ray-project/ray/tree/master/python/ray/tune>`__.

Features
--------

*  Supports any deep learning framework, including PyTorch, TensorFlow, and Keras.

*  Choose among scalable hyperparameter and model search techniques such as:

   -  `Population Based Training (PBT) <tune-schedulers.html#population-based-training-pbt>`__

   -  `Median Stopping Rule <tune-schedulers.html#median-stopping-rule>`__

   -  `HyperBand <tune-schedulers.html#asynchronous-hyperband>`__

*  Mix and match different hyperparameter optimization approaches - such as using `HyperOpt with HyperBand`_.

*  Visualize results with `TensorBoard <https://www.tensorflow.org/get_started/summaries_and_tensorboard>`__, `parallel coordinates (Plot.ly) <https://plot.ly/python/parallel-coordinates-plot/>`__, and `rllab's VisKit <https://media.readthedocs.org/pdf/rllab/latest/rllab.pdf>`__.

*  Scale to running on a large distributed cluster without changing your code.

*  Parallelize training for models with GPU requirements or algorithms that may themselves be parallel and distributed, using Tune's `resource-aware scheduling <tune-usage.html#using-gpus-resource-allocation>`__,

Take a look at `the User Guide <tune-usage.html>`__ for a comprehensive overview on how to use Tune's features.

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
        author={Liaw, Richard and Liang, Eric and Nishihara, Robert
                and Moritz, Philipp and Gonzalez, Joseph E and Stoica, Ion},
        journal={arXiv preprint arXiv:1807.05118},
        year={2018}
    }


.. _HyperOpt with HyperBand: https://github.com/ray-project/ray/blob/master/python/ray/tune/examples/hyperopt_example.py
