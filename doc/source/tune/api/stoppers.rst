.. _tune-stoppers:

Tune Stopping Mechanisms (tune.stopper)
=======================================

In addition to Trial Schedulers like :ref:`ASHA <tune-scheduler-hyperband>`, where a number of
trials are stopped if they perform subpar, Ray Tune also supports custom stopping mechanisms to stop trials early. They can also stop the entire experiment after a condition is met.
For instance, stopping mechanisms can specify to stop trials when they reached a plateau and the metric
doesn't change anymore.

Ray Tune comes with several stopping mechanisms out of the box. For custom stopping behavior, you can
inherit from the :class:`Stopper <ray.tune.Stopper>` class.

Other stopping behaviors are described :ref:`in the user guide <tune-stopping-ref>`.


.. _tune-stop-ref:

Stopper Interface (tune.Stopper)
--------------------------------

.. currentmodule:: ray.tune.stopper

.. autosummary::
    :toctree: doc/

    Stopper

.. autosummary::
    :toctree: doc/

    Stopper.__call__
    Stopper.stop_all

Tune Built-in Stoppers
----------------------

.. autosummary::
    :toctree: doc/

    MaximumIterationStopper
    ExperimentPlateauStopper
    TrialPlateauStopper
    TimeoutStopper
    CombinedStopper
