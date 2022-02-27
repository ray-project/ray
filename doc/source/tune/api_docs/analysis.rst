.. _tune-analysis-docs:

Analysis (tune.analysis)
========================

You can use the ``ExperimentAnalysis`` object for analyzing results.
It is returned automatically when calling ``tune.run``.

.. code-block:: python

    analysis = tune.run(
        trainable,
        name="example-experiment",
        num_samples=10,
    )

Here are some example operations for obtaining a summary of your experiment:

.. code-block:: python

    # Get a dataframe for the last reported results of all of the trials
    df = analysis.results_df

    # Get a dataframe for the max accuracy seen for each trial
    df = analysis.dataframe(metric="mean_accuracy", mode="max")

    # Get a dict mapping {trial logdir -> dataframes} for all trials in the experiment.
    all_dataframes = analysis.trial_dataframes

    # Get a list of trials
    trials = analysis.trials

You may want to get a summary of multiple experiments that point to the same ``local_dir``.
This is also supported by the ``ExperimentAnalysis`` class.

.. code-block:: python

    from ray.tune import ExperimentAnalysis
    analysis = ExperimentAnalysis("~/ray_results/example-experiment")

.. _exp-analysis-docstring:

ExperimentAnalysis (tune.ExperimentAnalysis)
--------------------------------------------

.. autoclass:: ray.tune.ExperimentAnalysis
    :members:

TrialCheckpoint (tune.cloud.TrialCheckpoint)
--------------------------------------------

.. autoclass:: ray.tune.cloud.TrialCheckpoint
    :members: