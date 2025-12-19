.. _air-results-ref:
.. _tune-analysis-docs:

.. _result-grid-docstring:

Tune Experiment Results (``tune.ResultGrid``)
=============================================

.. vale Google.Spacing = NO

ResultGrid (tune.ResultGrid)
----------------------------

.. vale Google.Spacing = YES

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

.. vale Google.Spacing = NO

Result (tune.Result)
---------------------

.. vale Google.Spacing = YES

.. autosummary::
    :nosignatures:
    :template: autosummary/class_without_autosummary.rst
    :toctree: doc/

    ~tune.Result

.. _exp-analysis-docstring:


.. vale Google.Spacing = NO

ExperimentAnalysis (tune.ExperimentAnalysis)
--------------------------------------------

.. vale Google.Spacing = YES

.. note::

    An `ExperimentAnalysis` is the output of the ``tune.run`` API.
    It's now recommended to use :meth:`Tuner.fit <ray.tune.Tuner.fit>`,
    which outputs a `ResultGrid` object.

.. autosummary::
    :nosignatures:
    :toctree: doc/

    ~tune.ExperimentAnalysis
