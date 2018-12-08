Tune Package Reference
=======================

ray.tune
--------

.. automodule:: ray.tune
    :members:
    :exclude-members: TuneError, Trainable

.. autoclass:: ray.tune.Trainable
    :members:
    :private-members:


.. autoclass:: ray.tune.function_runner.StatusReporter
    :members: __call__


ray.tune.schedulers
-------------------

.. automodule:: ray.tune.schedulers
    :members:
    :show-inheritance:

ray.tune.suggest
----------------

.. automodule:: ray.tune.suggest
    :members:
    :exclude-members: function, grid_search, SuggestionAlgorithm
    :show-inheritance:

.. autoclass:: ray.tune.suggest.SuggestionAlgorithm
    :members:
    :private-members:
    :show-inheritance:


ray.tune.logger
---------------

.. autoclass:: ray.tune.logger.Logger
    :members:
