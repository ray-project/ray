Tune Execution (tune.Tuner)
===========================

.. _tune-run-ref:

Tuner
-----

.. currentmodule:: ray.tune

.. autosummary::
    :nosignatures:
    :toctree: doc/

    Tuner

.. autosummary::
    :nosignatures:
    :toctree: doc/

    Tuner.fit
    Tuner.get_results

Tuner Configuration
~~~~~~~~~~~~~~~~~~~

.. autosummary::
    :nosignatures:
    :toctree: doc/

    TuneConfig

.. seealso::

    The `Tuner` constructor also takes in a :class:`RunConfig <ray.train.RunConfig>`.

Restoring a Tuner
~~~~~~~~~~~~~~~~~

.. autosummary::
    :nosignatures:
    :toctree: doc/

    Tuner.restore
    Tuner.can_restore


tune.run_experiments
--------------------

.. autosummary::
    :nosignatures:
    :toctree: doc/

    run_experiments
    Experiment
    TuneError
