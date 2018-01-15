Ray.tune: Hyperparameter Optimization Framework
===============================================

This document describes Ray.tune, a hyperparameter tuning framework for long-running tasks such as RL and deep learning training. It has the following features:

-  Early stopping algorithms such as `Median Stopping Rule <https://research.google.com/pubs/pub46180.html>`__ and `HyperBand <https://arxiv.org/abs/1603.06560>`__.

-  Integration with visualization tools such as `TensorBoard <https://www.tensorflow.org/get_started/summaries_and_tensorboard>`__, `rllab's VisKit <https://media.readthedocs.org/pdf/rllab/latest/rllab.pdf>`__, and a `parallel coordinates visualization <https://en.wikipedia.org/wiki/Parallel_coordinates>`__.

-  Flexible trial variant generation, including grid search, random search, and conditional parameter distributions.

-  Resource-aware scheduling, including support for concurrent runs of algorithms that may themselves be parallel and distributed.

You can find the code for Ray.tune `here on GitHub <https://github.com/ray-project/ray/tree/master/python/ray/tune>`__.

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
            "resources": { "cpu": 1, "gpu": 0 },
            "stop": { "mean_accuracy": 100 },
            "config": {
                "alpha": grid_search([0.2, 0.4, 0.6]),
                "beta": grid_search([1, 2]),
            },
        }
    })


This script runs a small grid search over the ``my_func`` function using ray.tune, reporting status on the command line until the stopping condition of ``mean_accuracy >= 100`` is reached (for metrics like _loss_ that decrease over time, specify `neg_mean_loss <https://github.com/ray-project/ray/blob/master/python/ray/tune/result.py#L40>`__ as a condition instead):

::

    == Status ==
    Using FIFO scheduling algorithm.
    Resources used: 4/8 CPUs, 0/0 GPUs
    Result logdir: ~/ray_results/my_experiment
     - my_func_0_alpha=0.2,beta=1:	RUNNING [pid=6778], 209 s, 20604 ts, 7.29 acc
     - my_func_1_alpha=0.4,beta=1:	RUNNING [pid=6780], 208 s, 20522 ts, 53.1 acc
     - my_func_2_alpha=0.6,beta=1:	TERMINATED [pid=6789], 21 s, 2190 ts, 101 acc
     - my_func_3_alpha=0.2,beta=2:	RUNNING [pid=6791], 208 s, 41004 ts, 8.37 acc
     - my_func_4_alpha=0.4,beta=2:	RUNNING [pid=6800], 209 s, 41204 ts, 70.1 acc
     - my_func_5_alpha=0.6,beta=2:	TERMINATED [pid=6809], 10 s, 2164 ts, 100 acc

In order to report incremental progress, ``my_func`` periodically calls the ``reporter`` function passed in by Ray.tune to return the current timestep and other metrics as defined in `ray.tune.result.TrainingResult <https://github.com/ray-project/ray/blob/master/python/ray/tune/result.py>`__.

Visualizing Results
-------------------

Ray.tune logs trial results to a unique directory per experiment, e.g. ``~/ray_results/my_experiment`` in the above example. The log records are compatible with a number of visualization tools:

To visualize learning in tensorboard, run:

::

    $ pip install tensorboard
    $ tensorboard --logdir=~/ray_results/my_experiment

.. image:: ray-tune-tensorboard.png

To use rllab's VisKit (you may have to install some dependencies), run:

::

    $ git clone https://github.com/rll/rllab.git
    $ python rllab/rllab/viskit/frontend.py ~/ray_results/my_experiment

.. image:: ray-tune-viskit.png

Finally, to view the results with a `parallel coordinates visualization <https://en.wikipedia.org/wiki/Parallel_coordinates>`__, open `ParalleCoordinatesVisualization.ipynb <https://github.com/ray-project/ray/blob/master/python/ray/tune/ParallelCoordinatesVisualization.ipynb>`__ as follows and run its cells:

::

    $ cd $RAY_HOME/python/ray/tune
    $ jupyter-notebook ParallelCoordinatesVisualization.ipynb

Trial Variant Generation
------------------------

In the above example, we specified a grid search over two parameters using the ``grid_search`` helper function. Ray.tune also supports sampling parameters from user-specified lambda functions, which can be used in combination with grid search.

The following shows grid search over two nested parameters combined with random sampling from two lambda functions. Note that the value of ``beta`` depends on the value of ``alpha``, which is represented by referencing ``spec.config.alpha`` in the lambda function. This lets you specify conditional parameter distributions.

::

    "config": {
        "alpha": lambda spec: np.random.uniform(100),
        "beta": lambda spec: spec.config.alpha * np.random.normal(),
        "nn_layers": [
            grid_search([16, 64, 256]),
            grid_search([16, 64, 256]),
        ],
    },
    "repeat": 10,

By default, each random variable and grid search point is sampled once. To take multiple random samples or repeat grid search runs, add ``repeat: N`` to the experiment config. E.g. in the above, ``"repeat": 10`` repeats the 3x3 grid search 10 times, for a total of 90 trials, each with randomly sampled values of ``alpha`` and ``beta``.

For more information on variant generation, see `variant_generator.py <https://github.com/ray-project/ray/blob/master/python/ray/tune/variant_generator.py>`__.

Early Stopping
--------------

To reduce costs, long-running trials can often be early stopped if their initial performance is not promising. Ray.tune allows early stopping algorithms to be plugged in on top of existing grid or random searches. This can be enabled by setting the ``scheduler`` parameter of ``run_experiments``, e.g.

::

    run_experiments({...}, scheduler=MedianStoppingRule())

Currently we support the following early stopping algorithms, or you can write your own that implements the `TrialScheduler <https://github.com/ray-project/ray/blob/master/python/ray/tune/trial_scheduler.py>`__ interface:

.. autoclass:: ray.tune.median_stopping_rule.MedianStoppingRule
.. autoclass:: ray.tune.hyperband.HyperBandScheduler

Checkpointing support
---------------------

To enable checkpoint / resume, the full ``Trainable`` API must be implemented (though as shown in the examples above, you can get away with just supplying a ``train(config, reporter)`` func if you don't need checkpointing). Implementing this interface is required to support resource multiplexing in schedulers such as HyperBand. For example, all `RLlib agents <https://github.com/ray-project/ray/blob/master/python/ray/rllib/agent.py>`__ implement the ``Trainable`` API.

.. autoclass:: ray.tune.trainable.Trainable
    :members:

Resource Allocation
-------------------

Ray.tune runs each trial as a Ray actor, allocating the specified GPU and CPU ``resources`` to each actor (defaulting to 1 CPU per trial). A trial will not be scheduled unless at least that amount of resources is available in the cluster, preventing the cluster from being overloaded.

If your trainable function / class creates further Ray actors or tasks that also consume CPU / GPU resources, you will also want to set ``driver_cpu_limit`` or ``driver_gpu_limit`` to tell Ray not to assign the entire resource reservation to your top-level trainable function, as described in `trial.py <https://github.com/ray-project/ray/blob/master/python/ray/tune/trial.py>`__.

Command-line JSON/YAML API
--------------------------

The JSON config passed to ``run_experiments`` can also be put in a JSON or YAML file, and the experiments run using the ``tune.py`` script. This supports the same functionality as the Python API, e.g.:

::

    cd ray/python/tune
    ./tune.py -f examples/tune_mnist_ray.yaml --scheduler=MedianStoppingRule


For more examples of experiments described by YAML files, see `RLlib tuned examples <https://github.com/ray-project/ray/tree/master/python/ray/rllib/tuned_examples>`__.

Running in a large cluster
--------------------------

The ``run_experiments`` also takes any arguments that ``ray.init()`` does. This can be used to pass in the redis address of a multi-node Ray cluster. For more details, check out the `tune.py script <https://github.com/ray-project/ray/blob/master/python/ray/tune/tune.py>`__.
