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

Tune will optimize your training process using the :ref:`Trainable API <trainable-docs>`. To start, let's try to maximize this objective function:

.. code-block:: python

    def objective(x, a, b):
        return a * (x ** 0.5) + b

Here's an example of specifying the objective function using :ref:`the function-based Trainable API <tune-function-api>`:

.. code-block:: python

    def trainable(config):
        # config (dict): A dict of hyperparameters.

        for x in range(20):
            score = objective(x, config["a"], config["b"])

            tune.report(score=score)  # This sends the score to Tune.

Now, there's two Trainable APIs - one being the :ref:`function-based API <tune-function-api>` that we demonstrated above.

The other is a :ref:`class-based API <tune-class-api>`. Here's an example of specifying the objective function using the :ref:`class-based API <tune-class-api>`:

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

tune.run
--------

Use ``tune.run`` execute hyperparameter tuning using the core Ray APIs. This function manages your experiment and provides many features such as :ref:`logging <tune-logging>`, :ref:`checkpointing <tune-checkpoint>`, and :ref:`early stopping <tune-stopping>`.

.. code-block:: python

    # Pass in a Trainable class or function to tune.run.
    tune.run(trainable)

This function will report status on the command line until all trials stop (each trial is one instance of a :ref:`Trainable <trainable-docs>`):

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


Search Algorithms
-----------------

To optimize the hyperparameters of your training process, you will want to use a :ref:`Search Algorithm <tune-search-alg>` which will help suggest better hyperparameters.

.. code-block:: python

    # Be sure to first run `pip install hyperopt`

    import hyperopt as hp
    from ray.tune.suggest.hyperopt import HyperOptSearch

    # Create a HyperOpt search space
    space = {
        "a": hp.uniform("a", 0, 1),
        "b": hp.uniform("b", 0, 20)

        # Note: Arbitrary HyperOpt search spaces should be supported!
        # "foo": hp.lognormal("foo", 0, 1))
    }

    # Specify the search space and maximize score
    hyperopt = HyperOptSearch(space, metric="score", mode="max")

    # Execute 20 trials using HyperOpt and stop after 20 iterations
    tune.run(
        trainable,
        search_alg=hyperopt,
        num_samples=20,
        stop={"training_iteration": 20}
    )

Tune has SearchAlgorithms that integrate with many popular **optimization** libraries, such as :ref:`Nevergrad <nevergrad>` and :ref:`Hyperopt <tune-hyperopt>`.

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

Analysis
--------

``tune.run`` returns an :ref:`Analysis <tune-analysis-docs>` object which has methods you can use for analyzing your training.

.. code-block:: python

    analysis = tune.run(trainable, search_alg=algo, stop={"training_iteration": 20})

    # Get the best hyperparameters
    best_hyperparameters = analysis.get_best_config()

This object can also retrieve all training runs as dataframes, allowing you to do ad-hoc data analysis over your results.

.. code-block:: python

    # Get a dataframe for the max score seen for each trial
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

Reach out to us if you have any questions or issues or feedback through the following channels:

1. `StackOverflow`_: For questions about how to use Ray.
2. `GitHub Issues`_: For bug reports and feature requests.

.. _`StackOverflow`: https://stackoverflow.com/questions/tagged/ray
.. _`GitHub Issues`: https://github.com/ray-project/ray/issues
