.. _air-results-ref:
.. _tune-analysis-docs:

.. _result-grid-docstring:

Tune Experiment Results (tune.ResultGrid)
=========================================

ResultGrid (tune.ResultGrid)
----------------------------

.. currentmodule:: ray

.. autosummary::
    :toctree: doc/
    :template: autosummary/class_with_autosummary.rst

    tune.ResultGrid
    tune.ResultGrid.get_best_result
    tune.ResultGrid.get_dataframe

.. _result-docstring:

Result (air.Result)
-------------------

.. autosummary::
    :toctree: doc/

    air.Result

.. _exp-analysis-docstring:


ExperimentAnalysis (tune.ExperimentAnalysis)
--------------------------------------------

.. note::

    An experiment analysis is the output of the ``tune.run`` API.
    It's now recommended to use ``Tuner.fit``, which outputs a ``ResultGrid`` object.

.. autosummary::
    :toctree: doc/

    tune.ExperimentAnalysis
