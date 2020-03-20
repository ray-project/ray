Tune Package Reference
=======================

ray.tune
--------

.. automodule:: ray.tune
    :members:
    :show-inheritance:
    :exclude-members: TuneError, Trainable, DurableTrainable

Training (`tune.Trainable`, tune.track)
---------------------------------------

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

Sampling
--------
.. automodule:: ray.tune
    :show-inheritance:
    :members: randint, randn, loguniform, uniform, choice, sample_from


ray.tune.schedulers
-------------------

.. automodule:: ray.tune.schedulers
    :members:
    :show-inheritance:

tune.suggest
------------

.. autoclass:: ray.tune.suggest.Repeater

.. autoclass:: ray.tune.suggest.SearchAlgorithm
    :members:

.. autoclass:: ray.tune.suggest.SuggestionAlgorithm
    :members:
    :private-members:
    :show-inheritance:

Registry
--------

.. autofunction:: ray.tune.register_trainable

.. autofunction:: ray.tune.register_env


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


Trial Executors
---------------

.. autoclass:: ray.tune.ray_trial_executor.RayTrialExecutor

.. autoclass:: ray.tune.trial_executor.TrialExecutor
