Ray.tune: Efficient distributed hyperparameter search with Ray
==============================================================

This document describes Ray.tune, a hyperparameter tuning tool for long-running tasks such as RL and deep learning training. It has the following features:

-  Early stopping algorithms such as `Median Stopping Rule <https://research.google.com/pubs/pub46180.html>`__ and `HyperBand <https://arxiv.org/abs/1603.06560>`__.

-  Integration with visualization tools such as `TensorBoard <https://www.tensorflow.org/get_started/summaries_and_tensorboard>`__, `rllab's VisKit <https://media.readthedocs.org/pdf/rllab/latest/rllab.pdf>`__, and a `parallel coordinates visualization <https://en.wikipedia.org/wiki/Parallel_coordinates>`__.

-  Flexible search space configuration, including grid search, random search, and conditional parameter distributions.

-  Resource-aware scheduling, including support for concurrent runs of algorithms that may themselves be parallel and distributed.


Getting Started
--------------- 


::

    from ray.tune import register_trainable, grid_search, run_experiments

    def my_func(config, reporter):
        import time, numpy as np
        i = 0
        while True:
            reporter(timesteps_total=i, mean_accuracy=i ** config["alpha"])
            i += config["beta"]
            time.sleep(.01)

    register_trainable("my_func", my_func)

    run_experiments({
        "my_experiment": {
            "run": "my_func",
            "stop": { "mean_accuracy": 100 },
            "config": {
                "alpha": grid_search([0.2, 0.4, 0.6]),
                "beta": grid_search([1, 2]),
            },
        }
    })


This script runs a small grid search over the ``my_func`` function using ray.tune, reporting status on the command line until the stopping condition of ``mean_accuracy >= 100`` is reached:

::

    == Status ==
    Using FIFO scheduling algorithm.
    Resources used: 4/8 CPUs, 0/0 GPUs
    Tensorboard logdir: /tmp/ray/my_experiment
     - my_func_0_alpha=0.2,beta=1:	RUNNING [pid=6778], 209 s, 20604 ts, 7.29 acc
     - my_func_1_alpha=0.4,beta=1:	RUNNING [pid=6780], 208 s, 20522 ts, 53.1 acc
     - my_func_2_alpha=0.6,beta=1:	TERMINATED [pid=6789], 21 s, 2190 ts, 101 acc
     - my_func_3_alpha=0.2,beta=2:	RUNNING [pid=6791], 208 s, 41004 ts, 8.37 acc
     - my_func_4_alpha=0.4,beta=2:	RUNNING [pid=6800], 209 s, 41204 ts, 70.1 acc
     - my_func_5_alpha=0.6,beta=2:	TERMINATED [pid=6809], 10 s, 2164 ts, 100 acc

In order to report incremental progress, ``my_func`` periodically calls the ``reporter`` function injected in by Ray.tune to specify the current timestep and other metrics as defined in `ray.tune.result.TrainingResult <https://github.com/ray-project/ray/blob/master/python/ray/tune/result.py>`__.

Visualizing Results
-------------------

Ray.tune logs trial results to a unique directory per experiment, e.g. ``/tmp/ray/my_experiment`` in the above example. The log records are compatible with a number of visualization tools:

To visualize learning in tensorboard, run:

::

    $ pip install tensorboard
    $ tensorboard --logdir=/tmp/ray/my_experiment

.. image:: ray-tune-tensorboard.png

To use rllab's VisKit (you may have to install some dependencies), run:

::

    $ git clone https://github.com/rll/rllab.git
    $ python rllab/rllab/viskit/frontend.py /tmp/ray/my_experiment

.. image:: ray-tune-viskit.png

Finally, to use Ray.tune's built-in parallel coordinates visualization, open the following notebook and run its cells:

::

    $ cd $RAY_HOME/python/ray/tune
    $ jupyter-notebook ParallelCoordinatesVisualization.ipynb

.. image:: ray-tune-parcoords.png

Search space configuration
--------------------------

Early Stopping
--------------

Trainable classes and RLlib support
-----------------------------------

Command-line JSON/YAML API
--------------------------

Running in a cluster
--------------------
