Analysis/Logging (tune.analysis / tune.logger)
==============================================

Analyzing Results
-----------------

You can use the ``ExperimentAnalysis`` object for analyzing results. It is returned automatically from calling ``tune.run``.

.. code-block:: python

    analysis = tune.run(
        trainable,
        name="example-experiment",
        num_samples=10,
    )

Here are some example operations for obtaining a summary of your experiment:

.. code-block:: python

    # Get a dataframe for the last reported results of all of the trials
    df = analysis.dataframe()

    # Get a dataframe for the max accuracy seen for each trial
    df = analysis.dataframe(metric="mean_accuracy", mode="max")

    # Get a dict mapping {trial logdir -> dataframes} for all trials in the experiment.
    all_dataframes = analysis.trial_dataframes

    # Get a list of trials
    trials = analysis.trials

You may want to get a summary of multiple experiments that point to the same ``local_dir``. For this, you can use the ``Analysis`` class.

.. code-block:: python

    from ray.tune import Analysis
    analysis = Analysis("~/ray_results/example-experiment")

.. _exp-analysis-docstring:

ExperimentAnalysis
~~~~~~~~~~~~~~~~~~

.. autoclass:: ray.tune.ExperimentAnalysis
    :show-inheritance:
    :members:

Analysis
~~~~~~~~

.. autoclass:: ray.tune.Analysis
    :members:

.. _loggers-docstring:

Loggers (tune.logger)
---------------------

Viskit
~~~~~~

Tune automatically integrates with Viskit via the ``CSVLogger`` outputs. To use VisKit (you may have to install some dependencies), run:

.. code-block:: bash

    $ git clone https://github.com/rll/rllab.git
    $ python rllab/rllab/viskit/frontend.py ~/ray_results/my_experiment

The nonrelevant metrics (like timing stats) can be disabled on the left to show only the relevant ones (like accuracy, loss, etc.).

.. image:: /ray-tune-viskit.png


.. _logger-interface:

Logger
~~~~~~

.. autoclass:: ray.tune.logger.Logger

UnifiedLogger
~~~~~~~~~~~~~

.. autoclass:: ray.tune.logger.UnifiedLogger

TBXLogger
~~~~~~~~~

.. autoclass:: ray.tune.logger.TBXLogger

JsonLogger
~~~~~~~~~~

.. autoclass:: ray.tune.logger.JsonLogger

CSVLogger
~~~~~~~~~

.. autoclass:: ray.tune.logger.CSVLogger

MLFLowLogger
~~~~~~~~~~~~

Tune also provides a default logger for `MLFlow <https://mlflow.org>`_. You can install MLFlow via ``pip install mlflow``. An example can be found `mlflow_example.py <https://github.com/ray-project/ray/blob/master/python/ray/tune/examples/mlflow_example.py>`__. Note that this currently does not include artifact logging support. For this, you can use the native MLFlow APIs inside your Trainable definition.

.. autoclass:: ray.tune.logger.MLFLowLogger

