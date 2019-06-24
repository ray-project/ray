Tune: Scalable Hyperparameter Search
====================================

.. image:: images/tune.png
    :scale: 30%
    :align: center

Tune is a scalable framework for hyperparameter search with a focus on deep learning and deep reinforcement learning.

You can find the code for Tune `here on GitHub <https://github.com/ray-project/ray/tree/master/python/ray/tune>`__. To get started with Tune, try going through `our tutorial of using Tune with Keras or PyTorch <https://github.com/ray-project/tutorial/blob/master/tune_exercises/>`__.

(Experimental): You can try out `the above tutorial on a free hosted server via Binder <https://mybinder.org/v2/gh/ray-project/tutorial/master?filepath=tune_exercises>`__.


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

Take a look at `the User Guide <tune-usage.html>`__ for a comprehensive overview on how to use Tune's features.

Getting Started
---------------

Installation
~~~~~~~~~~~~

You'll need to first `install ray <installation.html>`__ to import Tune.

.. code-block:: bash

    pip install ray  # also recommended: ray[debug]


Quick Start
~~~~~~~~~~~

This example runs a small grid search over a neural network training function using Tune, reporting status on the command line until the stopping condition of ``mean_accuracy >= 99`` is reached. Tune works with any deep learning framework. Here is an example with PyTorch:

.. code-block:: python

    def train_mnist(config):
        train_loader, test_loader = get_mnist_dataloaders()
        model = Net(config).to(torch.device("cpu"))
        optimizer = optim.SGD(
            model.parameters(), lr=config["lr"], momentum=config["momentum"])
        while True:
            train(model, optimizer, train_loader, torch.device("cpu"))
            acc = test(model, test_loader, torch.device("cpu"))
            track.log(mean_accuracy=acc)

    tune.run(
        train_mnist,
        stop={"mean_accuracy": 0.98},
        num_samples=10,
        config={
            "lr": tune.uniform(0.001, 0.1),
            "momentum": tune.uniform(0.1, 0.9)
        })


For massive parallelism, you can import and initialize Ray, and then run `ray submit`:

.. code-block:: python

    #your_script_using_tune.py
    import ray
    from ray import tune

    ray.init(redis_address=[ray_redis_address])
    ...
    tune.run(...)

Then, on command prompt:

.. code-block:: bash

    ray submit [CLUSTER_YAML] [your_script_using_tune.py]


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
