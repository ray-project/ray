.. _tune-60-seconds:

Tune in 60 Seconds
==================

Let's quickly walk through the key concepts you need to know to use Tune. In this guide, we'll be covering the following:

.. contents::
    :local:
    :depth: 1

.. image:: /images/tune-workflow.png

Trainables
----------

Tune will optimize your training process using the Trainable API. Use this API to set up your model and train it.

There are two types of Trainables - a **function-based API** is for fast prototyping, and **class-based** API that enables checkpointing and pausing.

Let's maximize this objective function:

.. code-block:: python

    def objective(x, a, b):
        return a * (x ** 0.5) + b

Here's an example of specifying the objective function using the Function API:

.. code-block:: python

    def trainable(config):
        # config (dict): A dict of hyperparameters.

        for x in range(20):
            score = objective(x, config["a"], config["b"])

            tune.track.log(score=score)  # This sends the score to Tune.

Here's an example of specifying the objective function using Class API example:

.. code-block:: python

    from ray import tune

    class Trainable(tune.Trainable):
        def _setup(self, config):
            # config (dict): A dict of hyperparameters
            self.x = 0
            self.a = config["a"]
            self.b = config["b"]

        def _train(self):  # This is called iteratively.
            score = objective(self.x, self.a, self.b)
            self.x += 1
            return {"score": score}

.. tip:: Do not use ``tune.track.log`` within a ``Trainable`` class.

See the documentation: :ref:`trainable-docs`.

tune.run
--------

Use ``tune.run`` execute hyperparameter tuning using the core Ray APIs. This function manages your distributed experiment and provides many features such as logging, checkpointing, and early stopping.

.. code-block:: python

    # Pass in a Trainable class or function to tune.run.
    tune.run(trainable)

This function will report status on the command line until all Trials stop:

.. code-block:: bash

    == Status ==
    Memory usage on this node: 11.4/16.0 GiB
    Using FIFO scheduling algorithm.
    Resources requested: 1/12 CPUs, 0/0 GPUs, 0.0/3.17 GiB heap, 0.0/1.07 GiB objects
    Result logdir: /Users/foo/ray_results/myexp
    Number of trials: 1 (1 RUNNING)
    +----------------------+----------+---------------------+-----------+--------+--------+----------------+-------+
    | Trial name           | status   | loc                 |    param1 | param2 |    acc | total time (s) |  iter |
    |----------------------+----------+---------------------+-----------+--------+--------+----------------+-------|
    | MyTrainable_a826033a | RUNNING  | 10.234.98.164:31115 | 0.303706  | 0.0761 | 0.1289 |        7.54952 |    15 |
    +----------------------+----------+---------------------+-----------+--------+--------+----------------+-------+


You can also easily run 10 trials (each trial is one instance of a Trainable). Tune automatically determines parallelism.

.. code-block:: python

    tune.run(trainable, num_samples=10)

Finally, you can randomly sample hyperparameters:

.. code-block:: python

    space = {"x": tune.uniform(0, 1)}
    tune.run(my_trainable, config=space, num_samples=10)

See the documentation: :ref:`tune-run-ref`.


Search Algorithms
-----------------

To optimize the hyperparameters of your training process, you will want to use a Search Algorithm which will help suggest better hyperparameters.

.. code-block:: python

    # Be sure to first run `pip install hyperopt`

    import hyperopt as hp
    from ray.tune.suggest.hyperopt import HyperOptSearch

    # Create a HyperOpt search space
    space = {
        "momentum": hp.uniform("momentum", 0, 20),
        "lr": hp.uniform("lr", 0, 1)
    }

    # Specify the search space and maximize accuracy
    hyperopt = HyperOptSearch(space, metric="accuracy", mode="max")

    # Execute 20 trials using HyperOpt and stop after 20 iterations
    tune.run(
        trainable,
        search_alg=hyperopt,
        num_samples=20,
        stop={"training_iteration": 20}
    )

Tune has SearchAlgorithms that integrate with many popular **optimization** libraries, such as `Nevergrad <https://github.com/facebookresearch/nevergrad>`_ and `Hyperopt <https://github.com/hyperopt/hyperopt/>`_.

See the documentation: :ref:`searchalg-ref`.

Trial Schedulers
----------------

In addition, you can make your training process more efficient by using a Trial Scheduler.

Trial Schedulers can stop/pause/tweak the hyperparameters of running trials, making your hyperparameter tuning process much faster.

.. code-block:: python

    from ray.tune.schedulers import HyperBandScheduler

    # Create HyperBand scheduler and maximize accuracy
    hyperband = HyperBandScheduler(metric="accuracy", mode="max")

    # Execute 20 trials using HyperBand using a search space
    configs = {"lr": tune.uniform(0, 1), "momentum": tune.uniform(0, 1)}

    tune.run(
        MyTrainableClass,
        config=configs,
        num_samples=20,
        scheduler=hyperband
    )

Population-based training and HyperBand are examples of popular optimization algorithms implemented as Trial Schedulers.

Unlike **Search Algorithms**, Trial Schedulers do not select which hyperparameter configurations to evaluate. However, you can use them together.

See the documentation: :ref:`schedulers-ref`.

Analysis
--------

``tune.run`` returns an :ref:`Analysis <tune-analysis-docs>` object which has methods you can use for analyzing your training.

.. code-block:: python

    analysis = tune.run(trainable, search_alg=algo, stop={"training_iteration": 20})

    # Get the best hyperparameters
    best_hyperparameters = analysis.get_best_config()

This object can also retrieve all training runs as dataframes, allowing you to do ad-hoc data analysis over your results.

.. code-block:: python

    # Get a dataframe for the max accuracy seen for each trial
    df = analysis.dataframe(metric="mean_accuracy", mode="max")

What's Next?
~~~~~~~~~~~~


Now that you have a working understanding of Tune, check out:

 * :ref:`Tune Guides and Examples <tune-guides-overview>`: Examples and templates for using Tune with your preferred machine learning library.
 * :ref:`tune-tutorial`: A simple tutorial that walks you through the process of setting up a Tune experiment.
 * :ref:`tune-user-guide`: A comprehensive overview of Tune's features.


Further Questions or Issues?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Reach out to us if you have any questions or issues or feedback through the following channels:

1. `StackOverflow`_: For questions about how to use Ray.
2. `GitHub Issues`_: For bug reports and feature requests.

.. _`StackOverflow`: https://stackoverflow.com/questions/tagged/ray
.. _`GitHub Issues`: https://github.com/ray-project/ray/issues
