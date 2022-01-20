.. _schedule-reference-docs:

Schedules API
=============

Schedules are used to compute values (in python, PyTorch or TensorFlow) based
on a (int64) timestep input. The computed values are usually float32 types.


Base Schedule class (ray.rllib.utils.schedules.schedule.Schedule)
-----------------------------------------------------------------------------

.. autoclass:: ray.rllib.utils.schedules.schedule.Schedule
    :members:


All built-in Schedule classes
-----------------------------

.. autoclass:: ray.rllib.utils.schedules.constant_schedule.ConstantSchedule
    :special-members: __init__
    :members:

.. autoclass:: ray.rllib.utils.schedules.linear_schedule.LinearSchedule
    :special-members: __init__
    :members:

.. autoclass:: ray.rllib.utils.schedules.polynomial_schedule.PolynomialSchedule
    :special-members: __init__
    :members:

.. autoclass:: ray.rllib.utils.schedules.exponential_schedule.ExponentialSchedule
    :special-members: __init__
    :members:

.. autoclass:: ray.rllib.utils.schedules.piecewise_schedule.PiecewiseSchedule
    :special-members: __init__
    :members:
