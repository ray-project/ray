:orphan:

.. _air-guides:

===========
User Guides
===========

.. _air-env-vars:

Environment variables
---------------------

Some behavior of Ray AIR can be controlled using environment variables.

Please also see the :ref:`Ray Tune environment variables <tune-env-vars>`.

- **RAY_AIR_FULL_TRACEBACKS**: If set to 1, will print full tracebacks for training functions,
  including internal code paths. Otherwise, abbreviated tracebacks that only show user code
  are printed. Defaults to 0 (disabled).
- **RAY_AIR_NEW_OUTPUT**: If set to 0, this disables
  the `experimental new console output <https://github.com/ray-project/ray/issues/36949>`_.
- **RAY_AIR_RICH_LAYOUT**: If set to 1, this enables
  the `stick table layout <https://github.com/ray-project/ray/issues/36949>`_
  (only available for Ray Tune).

.. _air-multi-tenancy:

Running multiple AIR jobs concurrently on a single cluster
----------------------------------------------------------
Running multiple AIR training or tuning jobs at the same
time on a single cluster is not officially supported.
We don't test this workflow
and recommend the use of multiple smaller clusters
instead.

If you still want to do this, refer to
the
:ref:`Ray Tune multi-tenancy docs <tune-multi-tenancy>`
for potential pitfalls.