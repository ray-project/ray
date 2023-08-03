Environment Variables
=====================

Some behavior of Ray Train can be controlled using environment variables.

Please also see the :ref:`Ray Tune environment variables <tune-env-vars>`.

* **RAY_AIR_FULL_TRACEBACKS**: If set to 1, will print full tracebacks for training functions,
  including internal code paths. Otherwise, abbreviated tracebacks that only show user code
  are printed. Defaults to 0 (disabled).
* **RAY_AIR_NEW_OUTPUT**: If set to 0, this disables
  the `experimental new console output <https://github.com/ray-project/ray/issues/36949>`_.

