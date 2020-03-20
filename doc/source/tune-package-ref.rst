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

Test

Trial Executors
---------------

.. autoclass:: ray.tune.ray_trial_executor.RayTrialExecutor

.. autoclass:: ray.tune.trial_executor.TrialExecutor

ray.tune.schedulers
-------------------

.. automodule:: ray.tune.schedulers
    :members:
    :show-inheritance:

ray.tune.suggest
----------------

.. automodule:: ray.tune.suggest
    :members:
    :exclude-members: function, sample_from, grid_search, SuggestionAlgorithm
    :show-inheritance:

.. autoclass:: ray.tune.suggest.SuggestionAlgorithm
    :members:
    :private-members:
    :show-inheritance:

.. autoclass:: ray.tune.suggest.Repeater


Loggers
-------

.. autoclass:: ray.tune.logger.Logger

.. autoclass:: ray.tune.logger.UnifiedLogger

.. autoclass:: ray.tune.logger.TBXLogger

.. autoclass:: ray.tune.logger.JsonLogger

XXTODO - DO NOT MERGE without adding docs to JsonLogger.

.. autoclass:: ray.tune.logger.CSVLogger
