.. _tune-stoppers:

Stopping mechanisms (tune.stopper)
==================================

In addition to Trial Schedulers like :ref:`ASHA <tune-scheduler-hyperband>`, where a number of
trials are stopped if they perform subpar, Ray Tune also supports custom stopping mechanisms to stop trials early. They can also stop the entire experiment after a condition is met.
For instance, stopping mechanisms can specify to stop trials when they reached a plateau and the metric
doesn't change anymore.

Ray Tune comes with several stopping mechanisms out of the box. For custom stopping behavior, you can
inherit from the :class:`Stopper <ray.tune.Stopper>` class.

Other stopping behaviors are described :ref:`in the user guide <tune-stopping-ref>`.

.. contents::
    :local:
    :depth: 1


.. _tune-stop-ref:

Stopper (tune.Stopper)
----------------------

.. autoclass:: ray.tune.Stopper
    :members: __call__, stop_all

MaximumIterationStopper (tune.stopper.MaximumIterationStopper)
--------------------------------------------------------------

.. autoclass:: ray.tune.stopper.MaximumIterationStopper

ExperimentPlateauStopper (tune.stopper.ExperimentPlateauStopper)
----------------------------------------------------------------

.. autoclass:: ray.tune.stopper.ExperimentPlateauStopper

TrialPlateauStopper (tune.stopper.TrialPlateauStopper)
------------------------------------------------------

.. autoclass:: ray.tune.stopper.TrialPlateauStopper

TimeoutStopper (tune.stopper.TimeoutStopper)
--------------------------------------------

.. autoclass:: ray.tune.stopper.TimeoutStopper

CombinedStopper (tune.stopper.CombinedStopper)
----------------------------------------------

.. autoclass:: ray.tune.stopper.CombinedStopper
