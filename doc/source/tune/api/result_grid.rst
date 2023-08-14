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

    ~tune.ResultGrid

.. autosummary::
    :toctree: doc/

    ~tune.ResultGrid.get_best_result
    ~tune.ResultGrid.get_dataframe

.. _result-docstring:

Result (air.Result)
-------------------

.. autosummary::
    :template: autosummary/class_without_autosummary.rst
    :noindex:

    ~air.Result

.. _exp-analysis-docstring:


ExperimentAnalysis (tune.ExperimentAnalysis)
--------------------------------------------

.. note::

    An `ExperimentAnalysis` is the output of the ``tune.run`` API.
    It's now recommended to use :meth:`Tuner.fit <ray.tune.Tuner.fit>`,
    which outputs a `ResultGrid` object.

.. autosummary::
    :toctree: doc/

    ~tune.ExperimentAnalysis
