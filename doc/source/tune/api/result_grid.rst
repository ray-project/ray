.. _air-results-ref:
.. _tune-analysis-docs:

.. _result-grid-docstring:

Tune Experiment Results (tune.ResultGrid)
=========================================

ResultGrid (tune.ResultGrid)
----------------------------

.. currentmodule:: ray

.. autosummary::
    :nosignatures:
    :toctree: doc/

    ~tune.ResultGrid

.. autosummary::
    :nosignatures:
    :toctree: doc/

    ~tune.ResultGrid.get_best_result
    ~tune.ResultGrid.get_dataframe

.. _result-docstring:

Result (tune.Result)
---------------------

.. autosummary::
    :nosignatures:
    :template: autosummary/class_without_autosummary.rst
    :toctree: doc/

    ~tune.Result

.. _exp-analysis-docstring:


ExperimentAnalysis (tune.ExperimentAnalysis)
--------------------------------------------

.. note::

    An `ExperimentAnalysis` is the output of the ``tune.run`` API.
    It's now recommended to use :meth:`Tuner.fit <ray.tune.Tuner.fit>`,
    which outputs a `ResultGrid` object.

.. autosummary::
    :nosignatures:
    :toctree: doc/

    ~tune.ExperimentAnalysis
