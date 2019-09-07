Tune Package Reference
=======================

ray.tune
--------

.. automodule:: ray.tune
    :members:
    :show-inheritance:
    :exclude-members: TuneError, Trainable

.. autoclass:: ray.tune.Trainable
    :members:
    :private-members:

.. autoclass:: ray.tune.function_runner.StatusReporter
    :members: __call__, logdir

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

ray.tune.track
--------------

.. automodule:: ray.tune.track
    :members:


ray.tune.logger
---------------

.. autoclass:: ray.tune.logger.Logger
    :members:
