Tune: Scalable Hyperparameter Search
====================================

.. image:: images/tune.png
    :scale: 30%
    :align: center

Tune is a scalable framework for hyperparameter search and model training with a focus on deep learning and deep reinforcement learning.

  * Scale to running on a large distributed cluster without changing your code.
  * Launch a multi-node Tune experiment in less than 10 lines of code.
  * Supports any deep learning framework, including PyTorch, TensorFlow, and Keras.
  * Visualize results with `TensorBoard <https://www.tensorflow.org/get_started/summaries_and_tensorboard>`__.
  * Choose among scalable SOTA algorithms such as `Population Based Training (PBT)`_, `Vizier's Median Stopping Rule`_, `HyperBand/ASHA`_.

.. _`Population Based Training (PBT)`: tune-schedulers.html#population-based-training-pbt
.. _`Vizier's Median Stopping Rule`: tune-schedulers.html#median-stopping-rule
.. _`HyperBand/ASHA`: tune-schedulers.html#asynchronous-hyperband

Quick Start
-----------

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


.. image:: images/tune-start-tb.png

Distributed Quick Start
-----------------------

1. Import and initialize Ray by appending the following to your example script.

.. code-block:: python

    # Append to top of your script
    import ray
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--ray-redis-address")
    args = parser.parse_args()
    ray.init(redis_address=args.ray_redis_address)

Alternatively, download a full example script here: :download:`mnist_pytorch.py <../../python/ray/tune/examples/mnist_pytorch.py>`

2. Download the following example Ray cluster configuration as ``tune-local-default.yaml`` and replace the appropriate fields:

.. literalinclude:: ../../python/ray/tune/examples/tune-local-default.yaml
   :language: yaml

Alternatively, download it here: :download:`tune-local-default.yaml <../../python/ray/tune/examples/tune-local-default.yaml>`. See `Ray cluster docs here <autoscaling.html>`_.

3. Run ``ray submit`` like the following.

.. code-block:: bash

    ray submit tune-local-default.yaml mnist_pytorch.py --args="--ray-redis-address=localhost:6379" --start

This will start Ray on all of your machines and run a distributed hyperparameter search across them.

To summarize, here are the full set of commands:

.. code-block:: bash

    wget https://raw.githubusercontent.com/ray-project/ray/master/python/ray/tune/examples/mnist_pytorch.py
    wget https://raw.githubusercontent.com/ray-project/ray/master/python/ray/tune/tune-local-default.yaml
    ray submit tune-local-default.yaml mnist_pytorch.py --args="--ray-redis-address=localhost:6379" --start


Take a look at the `Distributed Experiments <tune-distributed.html>`_ documentation for more details, including:

 1. Setting up distributed experiments on your local cluster
 2. Using AWS and GCP
 3. Spot instance usage/pre-emptible instances, and more.

Getting Started
---------------

  * `Code <https://github.com/ray-project/ray/tree/master/python/ray/tune>`__: GitHub repository for Tune.
  * `User Guide <tune-usage.html>`__: A comprehensive overview on how to use Tune's features.
  * `Tutorial Notebook <https://github.com/ray-project/tutorial/blob/master/tune_exercises/>`__: Our tutorial notebooks of using Tune with Keras or PyTorch.

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
