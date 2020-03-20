Tune Package Reference
=======================


.. contents:: :local:


Training (tune.run, Experiment)
-------------------------------

.. autofunction:: ray.tune.run

.. autofunction:: ray.tune.run_experiments

.. autofunction:: ray.tune.Experiment

Trainable (`tune.Trainable`, tune.track)
----------------------------------------

tune.Trainable
~~~~~~~~~~~~~~
.. autoclass:: ray.tune.Trainable
    :members:
    :private-members:

tune.DurableTrainable
~~~~~~~~~~~~~~~~~~~~~

.. autoclass:: ray.tune.DurableTrainable

.. autoclass:: ray.tune.function_runner.StatusReporter
    :members: __call__, logdir

tune.track
~~~~~~~~~~

.. automodule:: ray.tune.track
    :members:
    :exclude-members: init, shutdown

Sampling
--------

.. autofunction:: ray.tune.randn

.. autofunction:: ray.tune.loguniform

.. autofunction:: ray.tune.uniform

.. autofunction:: ray.tune.choice

.. autoclass:: ray.tune.sample_from

.. autofunction:: ray.tune.grid_search

Stopper
-------

.. autoclass:: ray.tune.Stopper
    :members:

Analysis (tune.analysis)
------------------------

ExperimentAnalysis
~~~~~~~~~~~~~~~~~~

.. autoclass:: ray.tune.ExperimentAnalysis
    :members:


Analysis
~~~~~~~~

.. autoclass:: ray.tune.Analysis
    :members:


Schedulers (tune.schedulers)
----------------------------

TrialScheduler
~~~~~~~~~~~~~~

.. autoclass:: ray.tune.schedulers.TrialScheduler
    :members:

FIFOScheduler
~~~~~~~~~~~~~

.. autoclass:: ray.tune.schedulers.FIFOScheduler

HyperBandScheduler
~~~~~~~~~~~~~~~~~~

.. autoclass:: ray.tune.schedulers.HyperBandScheduler

ASHAScheduler/AsyncHyperBandScheduler
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. autoclass:: ray.tune.schedulers.AsyncHyperBandScheduler

.. autoclass:: ray.tune.schedulers.ASHAScheduler

MedianStoppingRule
~~~~~~~~~~~~~~~~~~

.. autoclass:: ray.tune.schedulers.MedianStoppingRule

PopulationBasedTraining
~~~~~~~~~~~~~~~~~~~~~~~

.. autoclass:: ray.tune.schedulers.PopulationBasedTraining


Search Algorithms (tune.suggest)
--------------------------------

XXTODO this is missing

.. autoclass:: ray.tune.suggest.Repeater

.. autoclass:: ray.tune.suggest.SearchAlgorithm
    :members:

.. autoclass:: ray.tune.suggest.SuggestionAlgorithm
    :members:
    :private-members:
    :show-inheritance:

Loggers (tune.logger)
---------------------

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

XXTODO - DO NOT MERGE without adding docs to JsonLogger.

CSVLogger
~~~~~~~~~

.. autoclass:: ray.tune.logger.CSVLogger

MLFLowLogger
~~~~~~~~~~~~

.. autoclass:: ray.tune.logger.MLFLowLogger


Reporters
---------

ProgressReporter
~~~~~~~~~~~~~~~~

.. autoclass:: ray.tune.ProgressReporter

CLIReporter
~~~~~~~~~~~

.. autoclass:: ray.tune.CLIReporter

JupyterNotebookReporter
~~~~~~~~~~~~~~~~~~~~~~~

.. autoclass:: ray.tune.JupyterNotebookReporter


Internals
---------

Registry
~~~~~~~~

.. autofunction:: ray.tune.register_trainable

.. autofunction:: ray.tune.register_env

RayTrialExecutor
~~~~~~~~~~~~~~~~

.. autoclass:: ray.tune.ray_trial_executor.RayTrialExecutor
    :members:

TrialExecutor
~~~~~~~~~~~~~

.. autoclass:: ray.tune.trial_executor.TrialExecutor
    :members:

TrialRunner
~~~~~~~~~~~

.. autoclass:: ray.tune.trial_runner.TrialRunner

Trial
~~~~~
.. autofunction:: ray.tune.trial.Trial
