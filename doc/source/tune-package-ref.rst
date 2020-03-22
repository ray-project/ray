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
    :member-order: groupwise
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

tune.randn
~~~~~~~~~~

.. autofunction:: ray.tune.randn

tune.loguniform
~~~~~~~~~~~~~~~

.. autofunction:: ray.tune.loguniform

tune.uniform
~~~~~~~~~~~~

.. autofunction:: ray.tune.uniform

tune.choice
~~~~~~~~~~~

.. autofunction:: ray.tune.choice

tune.sample_from
~~~~~~~~~~~~~~~~

.. autoclass:: ray.tune.sample_from

tune.grid_search
~~~~~~~~~~~~~~~~

.. autofunction:: ray.tune.grid_search

Stopper (tune.Stopper)
----------------------

.. autoclass:: ray.tune.Stopper
    :members: __call__, stop_all

Analysis (tune.analysis)
------------------------

ExperimentAnalysis
~~~~~~~~~~~~~~~~~~

.. autoclass:: ray.tune.ExperimentAnalysis
    :show-inheritance:
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
    :members:

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

.. autoclass:: ray.tune.trial.Trial

Resources
~~~~~~~~~

.. autoclass:: ray.tune.resources.Resources
