.. _ray-profiling:

Profiling on Ray (Alpha)
========================
There are several ways to perform profilling on Ray as desribed `here <https://docs.ray.io/en/master/ray-observability/user-guides/profiling.html>`_. For better experience, Ray is natively supporting profilers to be run for task, actor or job in distributed cases.


GPU Profiling with Nsight Systems
---------------------------------

Installation
~~~~~~~~~~~~

First, install the Nsight System CLI by following the `Nsight User Guide <https://docs.nvidia.com/nsight-systems/InstallationGuide/index.html>`_.

Confirm Nsight is installed correctly:

.. code-block:: bash

  $ nsys --version

  # NVIDIA Nsight Systems version 2022.4.1.21-0db2c85

Run Nsight on Ray
~~~~~~~~~~~~~~~~~~~

To enable GPU profiling, you can specify the config in `runtime_env` as follows:

.. testcode::

  import ray

  ray.init()

  @ray.remote(runtime_env={ "nsight": "default" })
  def ray_task():
    ...
  
  # run nsight with config: "nsys profile [default options] ..."
  ray.get(ray_task.remote())

The `"default"` config can be found in `nsight.py <https://github.com/ray-project/ray/blob/master/python/ray/_private/runtime_env/nsight.py#L20>`_.

Custom Options
**************

You can also add `custom options <https://docs.nvidia.com/nsight-systems/UserGuide/index.html#cli-profile-command-switch-options>` for Nsight profiler by specfying a dictionary of option values.

.. testcode::

  import ray

  ray.init()

  @ray.remote(runtime_env={ "nsight": {
    "o": "task_1", # save report as task_1.nsys-rep
    "t": "cuda,cudnn,cublas",
    "cuda-memory-usage": "true",
    "cuda-graph-trace": "graph",
  }})
  def ray_task():
    ...
  
  # run nsight with config: 
  # "nsys profile  -o task_1 -t cuda,cudnn,cublas --cuda-memory-usage=True --cuda-graph-trace=graph ..."
  ray.get(ray_task.remote())

.. note::
    The default report filename (`-o, --output`) is `worker_process_{pid}.nsys-rep`.

Profiling Result
----------------

Profiling results can be found under the `/tmp/ray/session_*/logs/{profiler_name}` directory. Alternatively, users can download the profiling reports from the :ref:`Ray Dashboard <dash-logs-view>`.

.. note::
    The Nsight profiler output (-o, --output) option allows you to set the path to a filename. Ray uses the logs directory as the base and appends the output option to it. As a best practice, only specify the filename in output option.