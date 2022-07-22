.. _tune-analysis-docs:

ResultGrid (tune.ResultGrid)
========================================

You can use the ``ResultGrid`` object for interacting with tune results.
It is returned by ``Tuner.fit()``.

.. code-block:: python

    from ray import air, tune

    tuner = tune.Tuner(
        trainable,
        run_config=air.RunConfig(name="example-experiment"),
        tune_config=tune.TuneConfig(num_samples=10),
    )
    results = tuner.fit()

Here are some example operations for obtaining a summary of your experiment:

.. code-block:: python

    # Get a dataframe for the last reported results of all of the trials
    df = results.get_dataframe()

    # Get a dataframe for the max accuracy seen for each trial
    df = results.get_dataframe()(metric="mean_accuracy", mode="max")


One may wonder how is ResultGrid different than ExperimentAnalysis. ResultGrid is supposed
to succeed ExperimentAnalysis. However, it has not reached the same feature parity yet.
For interacting with an existing experiment, located at ``local_dir``, one may do the following:

.. code-block:: python

    from ray.tune import ExperimentAnalysis
    analysis = ExperimentAnalysis("~/ray_results/example-experiment")

.. _exp-analysis-docstring:

ExperimentAnalysis (tune.ExperimentAnalysis)
--------------------------------------------

.. autoclass:: ray.tune.ExperimentAnalysis
    :members:
