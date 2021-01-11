.. _tune-60-seconds:

============
Key Concepts
============

Let's quickly walk through the key concepts you need to know to use Tune. In this guide, we'll be covering the following:

.. contents::
    :local:
    :depth: 1

.. image:: /images/tune-workflow.png

Trainables
----------

To start, let's try to maximize this objective function:

.. code-block:: python

    def objective(x, a, b):
        return a * (x ** 0.5) + b

To use Tune, you will need to wrap this function in a lightweight :ref:`trainable API <trainable-docs>`. You can either use a :ref:`function-based version <tune-function-api>` or a :ref:`class-based version <tune-class-api>`.

.. tabs::
    .. group-tab:: Function API

        Here's an example of specifying the objective function using :ref:`the function-based Trainable API <tune-function-api>`:

        .. code-block:: python

            def trainable(config):
                # config (dict): A dict of hyperparameters.

                for x in range(20):
                    score = objective(x, config["a"], config["b"])

                    tune.report(score=score)  # This sends the score to Tune.

    .. group-tab:: Class API

        Here's an example of specifying the objective function using the :ref:`class-based API <tune-class-api>`:

        .. code-block:: python

            from ray import tune

            class Trainable(tune.Trainable):
                def setup(self, config):
                    # config (dict): A dict of hyperparameters
                    self.x = 0
                    self.a = config["a"]
                    self.b = config["b"]

                def step(self):  # This is called iteratively.
                    score = objective(self.x, self.a, self.b)
                    self.x += 1
                    return {"score": score}

        .. tip:: Do not use ``tune.report`` within a ``Trainable`` class.

See the documentation: :ref:`trainable-docs` and :ref:`examples <tune-general-examples>`.

tune.run and Trials
-------------------

Use :ref:`tune.run <tune-run-ref>` to execute hyperparameter tuning. This function manages your experiment and provides many features such as :ref:`logging <tune-logging>`, :ref:`checkpointing <tune-checkpoint>`, and :ref:`early stopping <tune-stopping>`.

.. code-block:: python

    # Pass in a Trainable class or function to tune.run.
    tune.run(trainable)

``tune.run`` will generate a couple hyperparameter configurations from its arguments, wrapping them into :ref:`Trial objects <trial-docstring>`.

Each trial has
* a hyperparameter configuration (``trial.config``), id (``trial.trial_id``)
* a resource specification (``resources_per_trial`` or ``trial.resources``)
* And other configuration values.

Each trial is also associated with one instance of a :ref:`Trainable <trainable-docs>`. You can access trial objects through the :ref:`Analysis object <tune-concepts-analysis>` provided after ``tune.run`` finishes.

``tune.run`` will execute until all trials stop or error:

.. code-block:: bash

    == Status ==
    Memory usage on this node: 11.4/16.0 GiB
    Using FIFO scheduling algorithm.
    Resources requested: 1/12 CPUs, 0/0 GPUs, 0.0/3.17 GiB heap, 0.0/1.07 GiB objects
    Result logdir: /Users/foo/ray_results/myexp
    Number of trials: 1 (1 RUNNING)
    +----------------------+----------+---------------------+-----------+--------+--------+----------------+-------+
    | Trial name           | status   | loc                 |         a |      b |  score | total time (s) |  iter |
    |----------------------+----------+---------------------+-----------+--------+--------+----------------+-------|
    | MyTrainable_a826033a | RUNNING  | 10.234.98.164:31115 | 0.303706  | 0.0761 | 0.1289 |        7.54952 |    15 |
    +----------------------+----------+---------------------+-----------+--------+--------+----------------+-------+


You can also easily run 10 trials. Tune automatically :ref:`determines how many trials will run in parallel <tune-parallelism>`.

.. code-block:: python

    tune.run(trainable, num_samples=10)

Finally, you can randomly sample or grid search hyperparameters via Tune's :ref:`search space API <tune-default-search-space>`:

.. code-block:: python

    space = {"x": tune.uniform(0, 1)}
    tune.run(my_trainable, config=space, num_samples=10)

See more documentation: :ref:`tune-run-ref`.


Search spaces
-------------

To optimize your hyperparameters, you have to define a *search space*.
A search space defines valid values for your hyperparameters and can specify
how these values are sampled (e.g. from a uniform distribution or a normal
distribution).

Tune offers various functions to define search spaces and sampling methods.
:ref:`You can find the documentation of these search space definitions here <tune-sample-docs>`.

Usually you pass your search space definition in the `config` parameter of
``tune.run()``.

Here's an example covering all search space functions. Again,
:ref:`here is the full explanation of all these functions <tune-sample-docs>`.


.. code-block :: python

    config = {
        "uniform": tune.uniform(-5, -1),  # Uniform float between -5 and -1
        "quniform": tune.quniform(3.2, 5.4, 0.2),  # Round to increments of 0.2
        "loguniform": tune.loguniform(1e-4, 1e-2),  # Uniform float in log space
        "qloguniform": tune.qloguniform(1e-4, 1e-1, 5e-4),  # Round to increments of 0.0005
        "randn": tune.randn(10, 2),  # Normal distribution with mean 10 and sd 2
        "qrandn": tune.qrandn(10, 2, 0.2),  # Round to increments of 0.2
        "randint": tune.randint(-9, 15),  # Random integer between -9 and 15
        "qrandint": tune.qrandint(-21, 12, 3),  # Round to increments of 3 (includes 12)
        "choice": tune.choice(["a", "b", "c"]),  # Choose one of these options uniformly
        "func": tune.sample_from(lambda spec: spec.config.uniform * 0.01), # Depends on other value
        "grid": tune.grid_search([32, 64, 128])  # Search over all these values
    }

Search Algorithms
-----------------

To optimize the hyperparameters of your training process, you will want to use a :ref:`Search Algorithm <tune-search-alg>` which will help suggest better hyperparameters.

.. code-block:: python

    # Be sure to first run `pip install hyperopt`

    import hyperopt as hp
    from ray.tune.suggest.hyperopt import HyperOptSearch

    # Create a HyperOpt search space
    config = {
        "a": tune.uniform(0, 1),
        "b": tune.uniform(0, 20)

        # Note: Arbitrary HyperOpt search spaces should be supported!
        # "foo": tune.randn(0, 1))
    }

    # Specify the search space and maximize score
    hyperopt = HyperOptSearch(metric="score", mode="max")

    # Execute 20 trials using HyperOpt and stop after 20 iterations
    tune.run(
        trainable,
        config=config,
        search_alg=hyperopt,
        num_samples=20,
        stop={"training_iteration": 20}
    )

Tune has SearchAlgorithms that integrate with many popular **optimization** libraries, such as :ref:`Nevergrad <nevergrad>` and :ref:`Hyperopt <tune-hyperopt>`. Tune automatically converts the provided search space into the search
spaces the search algorithms/underlying library expect.

.. note::
    We are currently in the process of implementing automatic search space
    conversions for all search algorithms. Currently this works for
    AxSearch, BayesOpt, Hyperopt and Optuna. The other search algorithms
    will follow shortly, but have to be instantiated with their respective
    search spaces at the moment.

See the documentation: :ref:`tune-search-alg`.

Trial Schedulers
----------------

In addition, you can make your training process more efficient by using a :ref:`Trial Scheduler <tune-schedulers>`.

Trial Schedulers can stop/pause/tweak the hyperparameters of running trials, making your hyperparameter tuning process much faster.

.. code-block:: python

    from ray.tune.schedulers import HyperBandScheduler

    # Create HyperBand scheduler and maximize score
    hyperband = HyperBandScheduler(metric="score", mode="max")

    # Execute 20 trials using HyperBand using a search space
    configs = {"a": tune.uniform(0, 1), "b": tune.uniform(0, 1)}

    tune.run(
        MyTrainableClass,
        config=configs,
        num_samples=20,
        scheduler=hyperband
    )

:ref:`Population-based Training <tune-scheduler-pbt>` and :ref:`HyperBand <tune-scheduler-hyperband>` are examples of popular optimization algorithms implemented as Trial Schedulers.

Unlike **Search Algorithms**, :ref:`Trial Scheduler <tune-schedulers>` do not select which hyperparameter configurations to evaluate. However, you can use them together.

See the documentation: :ref:`schedulers-ref`.

.. _tune-concepts-analysis:

Analysis
--------

``tune.run`` returns an :ref:`Analysis <tune-analysis-docs>` object which has methods you can use for analyzing your training.

.. code-block:: python

    analysis = tune.run(trainable, search_alg=algo, stop={"training_iteration": 20})

    best_trial = analysis.best_trial  # Get best trial
    best_config = analysis.best_config  # Get best trial's hyperparameters
    best_logdir = analysis.best_logdir  # Get best trial's logdir
    best_checkpoint = analysis.best_checkpoint  # Get best trial's best checkpoint
    best_result = analysis.best_result  # Get best trial's last results
    best_result_df = analysis.best_result_df  # Get best result as pandas dataframe

This object can also retrieve all training runs as dataframes, allowing you to do ad-hoc data analysis over your results.

.. code-block:: python

    # Get a dataframe with the last results for each trial
    df_results = analysis.results_df

    # Get a dataframe of results for a specific score or mode
    df = analysis.dataframe(metric="score", mode="max")


What's Next?
-------------

Now that you have a working understanding of Tune, check out:

* :doc:`/tune/user-guide`: A comprehensive overview of Tune's features.
* :ref:`tune-guides`: Tutorials for using Tune with your preferred machine learning library.
* :doc:`/tune/examples/index`: End-to-end examples and templates for using Tune with your preferred machine learning library.
* :ref:`tune-tutorial`: A simple tutorial that walks you through the process of setting up a Tune experiment.


Further Questions or Issues?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. include:: /_help.rst
