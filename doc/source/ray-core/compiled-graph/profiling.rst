Profiling
=========

Ray Compiled Graph provides profiling functionalities to better understand the performance
of individual tasks, systems overhead, and performance bottlenecks.

Nsight system profiler
----------------------

Compiled Graphs build on top of Ray's profiling capabilities, and leverage Nsight
system profiling. 

To run Nsight Profiling on Compiled Graphs, specify the runtime_env for the involved actors
as described in `Run Nsight on Ray <https://docs.ray.io/en/latest/ray-observability/user-guides/profiling.html#run-nsight-on-ray>`__.

After execution, Compiled Graph generates the profiling results under the `/tmp/ray/session_*/logs/{profiler_name}`
directory.

For fine-grained performance analysis of method calls and system overhead, set the environment variable
``RAY_CGRAPH_ENABLE_NVTX_PROFILING=1`` when running the script. For example, for a Compiled Graph script
in ``example.py``, run the following command:

.. testcode::

    RAY_CGRAPH_ENABLE_NVTX_PROFILING=1 python3 example.py


This leverages the `NVTX library <https://nvtx.readthedocs.io/en/latest/index.html#>`_ to automatically
annotate all methods called in the execution loops of compiled graph.

To visualize the profiling results, follow the same instructions as described in 
`Nsight Profiling Result <https://docs.ray.io/en/latest/ray-observability/user-guides/profiling.html#profiling-result>`__.
