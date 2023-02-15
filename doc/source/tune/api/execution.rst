Tune Execution (tune.Tuner)
===========================

.. _tune-run-ref:

.. _air-tuner-ref:

Tuner
-----

.. currentmodule:: ray.tune

.. autosummary::
    :toctree: doc/
    :template: autosummary/class_with_autosummary.rst

    Tuner

.. autosummary::
    :toctree: doc/

    Tuner.fit
    Tuner.get_results

Tuner Configuration
~~~~~~~~~~~~~~~~~~~

.. autosummary::
    :toctree: doc/

    TuneConfig

.. seealso::

    The `Tuner` constructor also takes in a :class:`air.RunConfig <ray.air.RunConfig>`.

Restoring a Tuner
~~~~~~~~~~~~~~~~~

.. autosummary::
    :toctree: doc/

    Tuner.restore
    Tuner.can_restore


tune.run_experiments
--------------------

.. autosummary::
    :toctree: doc/

    run_experiments
    Experiment
