.. _tune-60-seconds:

Tune in 60 Seconds
==================


Tune takes a user-defined Python function or class and evaluates it on a set of hyperparameter configurations.

Each hyperparameter configuration evaluation is called a *trial*, and multiple trials are run in parallel. In this guide, we'll be covering the following:

.. contents:: :local:

.. image:: /images/tune-workflow.png

Trainables
----------

To allow Tune to optimize your model, Tune will need to control your training process. This is done via the Trainable API. Each *trial* corresponds to one instance of a Trainable; Tune will create multiple instances of the Trainable.

The Trainable API is where you specify how to set up your model and track intermediate training progress. There are two types of Trainables - a **function-based API** is for fast prototyping, and **class-based** API that unlocks many Tune features such as checkpointing, pausing.

.. code-block:: python

    from ray import tune

    class Trainable(tune.Trainable):
        """Tries to iteratively find the password."""

        def _setup(self, config):
            self.iter = 0
            self.password = 1024

        def _train(self):
            """Execute one step of 'training'. This function will be called iteratively"""
            self.iter += 1
            return {
                "accuracy": abs(self.iter - self.password),
                "training_iteration": self.iter  # Tune will automatically provide this.
            }

        def _stop(self):
            # perform any cleanup necessary.
            pass

Function API example:

.. code-block:: python

    def trainable(config):
        """
        Args:
            config (dict): Parameters provided from the search algorithm
                or variant generation.
        """

        while True:
            # ...
            tune.track.log(**kwargs)

.. tip:: Do not use ``tune.track.log`` within a ``Trainable`` class.

See the documentation: :ref:`trainable-docs`.

tune.run
--------

Use ``tune.run`` execute hyperparameter tuning using the core Ray APIs. This function manages your distributed experiment and provides many features such as logging, checkpointing, and early stopping.

.. code-block:: python

    # Pass in a Trainable class or function to tune.run.
    tune.run(trainable)

    # Run 10 trials (each trial is one instance of a Trainable). Tune runs in
    # parallel and automatically determines concurrency.
    tune.run(trainable, num_samples=10)

    # Run 1 trial, stop when trial has reached 10 iterations OR a mean accuracy of 0.98.
    tune.run(my_trainable, stop={"training_iteration": 10, "mean_accuracy": 0.98})

    # Run 1 trial, search over hyperparameters, stop after 10 iterations.
    hyperparameters = {"lr": tune.uniform(0, 1), "momentum": tune.uniform(0, 1)}
    tune.run(my_trainable, config=hyperparameters, stop={"training_iteration": 10})

This function will report status on the command line until all Trials stop:

.. code-block:: bash

    == Status ==
    Memory usage on this node: 11.4/16.0 GiB
    Using FIFO scheduling algorithm.
    Resources requested: 4/12 CPUs, 0/0 GPUs, 0.0/3.17 GiB heap, 0.0/1.07 GiB objects
    Result logdir: /Users/foo/ray_results/myexp
    Number of trials: 4 (4 RUNNING)
    +----------------------+----------+---------------------+-----------+--------+--------+----------------+-------+
    | Trial name           | status   | loc                 |    param1 | param2 |    acc | total time (s) |  iter |
    |----------------------+----------+---------------------+-----------+--------+--------+----------------+-------|
    | MyTrainable_a826033a | RUNNING  | 10.234.98.164:31115 | 0.303706  | 0.0761 | 0.1289 |        7.54952 |    15 |
    | MyTrainable_a8263fc6 | RUNNING  | 10.234.98.164:31117 | 0.929276  | 0.158  | 0.4865 |        7.0501  |    14 |
    | MyTrainable_a8267914 | RUNNING  | 10.234.98.164:31111 | 0.068426  | 0.0319 | 0.9585 |        7.0477  |    14 |
    | MyTrainable_a826b7bc | RUNNING  | 10.234.98.164:31112 | 0.729127  | 0.0748 | 0.1797 |        7.05715 |    14 |
    +----------------------+----------+---------------------+-----------+--------+--------+----------------+-------+

See the documentation: :ref:`tune-run-ref`.


Search Algorithms
-----------------

To optimize the hyperparameters of your training process, you will want to explore a “search space”.

Search Algorithms are Tune modules that help explore a provided search space. It will use previous results from evaluating different hyperparameters to suggest better hyperparameters. Tune has SearchAlgorithms that integrate with many popular **optimization** libraries, such as `Nevergrad <https://github.com/facebookresearch/nevergrad>`_ and `Hyperopt <https://github.com/hyperopt/hyperopt/>`_.

.. code-block:: python

    # https://github.com/hyperopt/hyperopt/
    # pip install hyperopt
    import hyperopt as hp
    from ray.tune.suggest.hyperopt import HyperOptSearch

    # Create a HyperOpt search space
    space = {"momentum": hp.uniform("momentum", 0, 20), "lr": hp.uniform("lr", 0, 1)}
    # Pass the search space into Tune's HyperOpt wrapper and maximize accuracy
    hyperopt = HyperOptSearch(space, metric="accuracy", mode="max")

    # Execute 20 trials using HyperOpt, stop after 20 iterations
    max_iters = {"training_iteration": 20}
    tune.run(trainable, search_alg=hyperopt, num_samples=20, stop=max_iters)

See the documentation: :ref:`searchalg-ref`.

Trial Schedulers
----------------

In addition, you can make your training process more efficient by stopping, pausing, or perturbing running trials.

Trial Schedulers are Tune modules that adjust and change distributed training runs during execution. These modules can stop/pause/perturb running trials, making your hyperparameter tuning process much faster. Population-based training and HyperBand are examples of popular optimization algorithms implemented as Trial Schedulers.

.. code-block:: python

    from ray.tune.schedulers import HyperBandScheduler

    # Create HyperBand scheduler and maximize accuracy
    hyperband = HyperBandScheduler(metric="accuracy", mode="max")

    # Execute 20 trials using HyperBand using a search space
    configs = {"lr": tune.uniform(0, 1), "momentum": tune.uniform(0, 1)}
    tune.run(MyTrainableClass, num_samples=20, config=configs, scheduler=hyperband)

Unlike **Search Algorithms**, Trial Schedulers do not select which hyperparameter configurations to evaluate. However, you can use them together. See the documentation: :ref:`schedulers-ref`.


Analysis
--------

After running a hyperparameter tuning job, you will want to analyze your results to determine what specific parameters are important and which hyperparameter values are the best.

``tune.run`` returns an :ref:`Analysis <tune-analysis-docs>` object which has methods you can use for analyzing your results. This object can also retrieve all training runs as dataframes, allowing you to do ad-hoc data analysis over your results.

.. code-block:: python

    analysis = tune.run(trainable, search_alg=algo, stop={"training_iteration": 20})

    # Get the best hyperparameters
    best_hyperparameters = analysis.get_best_config()

    # Get a dataframe for the max accuracy seen for each trial
    df = analysis.dataframe(metric="mean_accuracy", mode="max")
