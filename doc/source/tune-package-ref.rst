Tune Package Reference
=======================

Training (tune.run, tune.Experiment)
------------------------------------

tune.run
~~~~~~~~

.. autofunction:: ray.tune.run

tune.run_experiments
~~~~~~~~~~~~~~~~~~~~

.. autofunction:: ray.tune.run_experiments

tune.Experiment
~~~~~~~~~~~~~~~

.. autofunction:: ray.tune.Experiment

Trainable (tune.Trainable, tune.track)
--------------------------------------

tune.Trainable
~~~~~~~~~~~~~~

.. autoclass:: ray.tune.Trainable
    :private-members:
    :members:

tune.DurableTrainable
~~~~~~~~~~~~~~~~~~~~~

.. autoclass:: ray.tune.DurableTrainable

tune.track
~~~~~~~~~~

.. automodule:: ray.tune.track
    :members:
    :exclude-members: init, shutdown

StatusReporter
~~~~~~~~~~~~~~

.. autoclass:: ray.tune.function_runner.StatusReporter
    :members: __call__, logdir

Sampling (tune.rand, tune.grid_search...)
-----------------------------------------

randn
~~~~~

.. autofunction:: ray.tune.randn

loguniform
~~~~~~~~~~

.. autofunction:: ray.tune.loguniform

uniform
~~~~~~~

.. autofunction:: ray.tune.uniform

choice
~~~~~~

.. autofunction:: ray.tune.choice

sample_from
~~~~~~~~~~~

.. autoclass:: ray.tune.sample_from

grid_search
~~~~~~~~~~~

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


TrialScheduler
~~~~~~~~~~~~~~

.. autoclass:: ray.tune.schedulers.TrialScheduler
    :members:


Search Algorithms (tune.suggest)
--------------------------------

BasicVariantGenerator
~~~~~~~~~~~~~~~~~~~~~

.. autoclass:: ray.tune.suggest.BasicVariantGenerator

AxSearch
~~~~~~~~

.. autoclass:: ray.tune.suggest.ax.AxSearch

BayesOptSearch
~~~~~~~~~~~~~~

.. autoclass:: ray.tune.suggest.bayesopt.BayesOptSearch

TuneBOHB
~~~~~~~~

.. autoclass:: ray.tune.suggest.bohb.TuneBOHB

DragonflySearch
~~~~~~~~~~~~~~~

.. autoclass:: ray.tune.suggest.dragonfly.DragonflySearch

HyperOptSearch
~~~~~~~~~~~~~~

.. autoclass:: ray.tune.suggest.hyperopt.HyperOptSearch

NevergradSearch
~~~~~~~~~~~~~~~

.. autoclass:: ray.tune.suggest.nevergrad.NevergradSearch

SigOptSearch
~~~~~~~~~~~~

.. autoclass:: ray.tune.suggest.sigopt.SigOptSearch

SkOptSearch
~~~~~~~~~~~

.. autoclass:: ray.tune.suggest.skopt.SkOptSearch

SearchAlgorithm
~~~~~~~~~~~~~~~

.. autoclass:: ray.tune.suggest.SearchAlgorithm
    :members:

SuggestionAlgorithm
~~~~~~~~~~~~~~~~~~~

.. autoclass:: ray.tune.suggest.SuggestionAlgorithm
    :members:
    :private-members:
    :show-inheritance:

Repeater
~~~~~~~~

.. autoclass:: ray.tune.suggest.Repeater

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
