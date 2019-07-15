Tune: Scalable Hyperparameter Search
====================================

.. image:: images/tune.png
    :scale: 30%
    :align: center

Tune is a scalable framework for hyperparameter search and model training with a focus on deep learning and deep reinforcement learning.

Getting Started
---------------

  * `Code <https://github.com/ray-project/ray/tree/master/python/ray/tune>`__: GitHub repository for Tune.
  * `User Guide <tune-usage.html>`__: A comprehensive overview on how to use Tune's features.
  * `Tutorial Notebook <https://github.com/ray-project/tutorial/blob/master/tune_exercises/>`__: Our tutorial notebooks of using Tune with Keras or PyTorch.

Quick Start
~~~~~~~~~~~

This example runs a small grid search over a neural network training function using Tune, reporting status on the command line until the stopping condition of ``mean_accuracy >= 98`` is reached. Tune works with any deep learning framework. Here is an example with PyTorch:

.. code-block:: python

    import torch
    import torch.optim as optim
    from ray import tune
    from ray.tune.examples.mnist import get_data_loaders, Net, train, test


    def train_mnist(config):
        train_loader, test_loader = get_data_loaders()
        model = Net(config)
        optimizer = optim.SGD(
            model.parameters(), lr=config["lr"])
        while True:
            train(model, optimizer, train_loader)
            acc = test(model, test_loader)
            tune.track.log(mean_accuracy=acc)

    analysis = tune.run(
        train_mnist,
        stop={"mean_accuracy": 0.98},
        config={"lr": tune.grid_search([0.001, 0.01, 0.1])}
    )

    print("Best config: ", analysis.get_best_config())

    # Get a dataframe for analyzing trial results:
    df = analysis.dataframe()

    # On command line, you can also use `tensorboard --logdir ~/ray_results` to visualize results.


For massive parallelism, you can import and initialize Ray, and then run `ray submit`:

.. code-block:: python

    # Append to top of your script

    import ray
    from ray import tune

    ray.init(redis_address=tune.DEFAULT_REDIS_ADDRESS)

.. code-block:: bash

    ray submit tune_cluster.yaml [TUNE_SCRIPT.py] --start


Features
--------

*  Supports any deep learning framework, including PyTorch, TensorFlow, and Keras.

*  Choose among scalable hyperparameter and model search techniques such as:

   -  `Population Based Training (PBT) <tune-schedulers.html#population-based-training-pbt>`__

   -  `Median Stopping Rule <tune-schedulers.html#median-stopping-rule>`__

   -  `HyperBand <tune-schedulers.html#asynchronous-hyperband>`__

*  Mix and match different hyperparameter optimization approaches - such as using `HyperOpt with HyperBand`_ or `Nevergrad with HyperBand`_.

*  Visualize results with `TensorBoard <https://www.tensorflow.org/get_started/summaries_and_tensorboard>`__ and `rllab's VisKit <https://github.com/vitchyr/viskit>`__.

*  Scale to running on a large distributed cluster without changing your code.

*  Parallelize training for models with GPU requirements or algorithms that may themselves be parallel and distributed, using Tune's `resource-aware scheduling <tune-usage.html#using-gpus-resource-allocation>`__,


Contribute to Tune
------------------

Take a look at our `Contributor Guide <tune-contrib.html>`__ for guidelines on contributing.


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
.. _Nevergrad with HyperBand: https://github.com/ray-project/ray/blob/master/python/ray/tune/examples/nevergrad_example.py
