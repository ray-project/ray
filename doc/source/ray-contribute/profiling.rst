.. _ray-core-internal-profiling:

Profiling (internal)
====================

This document details, for Ray developers, how to analyze Ray performance.

Getting a stack trace of Ray C++ processes
------------------------------------------

You can use the following GDB command to view the current stack trace of any
running Ray process (e.g., raylet). This can be useful for debugging 100% CPU
utilization or infinite loops (simply run the command a few times to see what
the process is stuck on).

.. code-block:: shell

 sudo gdb -batch -ex "thread apply all bt" -p <pid>

Note that you can find the pid of the raylet with ``pgrep raylet``.

Installation
------------

These instructions are for Ubuntu only. Attempts to get ``pprof`` to correctly
symbolize on Mac OS have failed.

.. code-block:: bash

  sudo apt-get install google-perftools libgoogle-perftools-dev

Launching the to-profile binary
-------------------------------

If you want to launch Ray in profiling mode, define the following variables:

.. code-block:: bash

  export PERFTOOLS_PATH=/usr/lib/x86_64-linux-gnu/libprofiler.so
  export PERFTOOLS_LOGFILE=/tmp/pprof.out


The file ``/tmp/pprof.out`` will be empty until you let the binary run the
target workload for a while and then ``kill`` it via ``ray stop`` or by
letting the driver exit.

Memory Profiling
----------------
If you want to run memory profiling on Ray core components, you can use Jemalloc (https://github.com/jemalloc/jemalloc).
Ray supports environment variables to override LD_PRELOAD on core components.

You can find the component name from `ray_constants.py`. For example, if you'd like to profile gcs_server, 
search `PROCESS_TYPE_GCS_SERVER` in `ray_constants.py`. You can see the value is `gcs_server`.

Users are supposed to provide 3 env vars for memory profiling.

- RAY_JEMALLOC_LIB_PATH: The path to the jemalloc shared library `.so`.
- RAY_JEMALLOC_CONF: The MALLOC_CONF of jemalloc (comma separated).
- RAY_JEMALLOC_PROFILE: Comma separated Ray components to run Jemalloc `.so`. e.g., ("raylet,gcs_server"). Note that the components should match the process type in `ray_constants.py`. (It means "RAYLET,GCS_SERVER" won't work).

.. code-block:: bash

  # Install jemalloc
  wget https://github.com/jemalloc/jemalloc/releases/download/5.2.1/jemalloc-5.2.1.tar.bz2 
  tar -xf jemalloc-5.2.1.tar.bz2 
  cd jemalloc-5.2.1 
  ./configure --enable-prof --enable-prof-libunwind 
  make

  # set jemalloc configs through MALLOC_CONF env variable
  # read http://jemalloc.net/jemalloc.3.html#opt.lg_prof_interval
  # for all jemalloc configs
  # Ray start will profile the GCS server component.
  RAY_JEMALLOC_CONF=prof:true,lg_prof_interval:33,lg_prof_sample:17,prof_final:true,prof_leak:true \
  RAY_JEMALLOC_LIB_PATH=~/jemalloc-5.2.1/lib/libjemalloc.so \
  RAY_JEMALLOC_PROFILE=gcs_server \
  ray start --head

  # You should be able to see the following logs.
  2021-10-20 19:45:08,175	INFO services.py:622 -- Jemalloc profiling will be used for gcs_server. env vars: {'LD_PRELOAD': '/Users/sangbincho/jemalloc-5.2.1/lib/libjemalloc.so', 'MALLOC_CONF': 'prof:true,lg_prof_interval:33,lg_prof_sample:17,prof_final:true,prof_leak:true'}

Visualizing the CPU profile
---------------------------

The output of ``pprof`` can be visualized in many ways. Here we output it as a
zoomable ``.svg`` image displaying the call graph annotated with hot paths.

.. code-block:: bash

  # Use the appropriate path.
  RAYLET=ray/python/ray/core/src/ray/raylet/raylet

  google-pprof -svg $RAYLET /tmp/pprof.out > /tmp/pprof.svg
  # Then open the .svg file with Chrome.

  # If you realize the call graph is too large, use -focus=<some function> to zoom
  # into subtrees.
  google-pprof -focus=epoll_wait -svg $RAYLET /tmp/pprof.out > /tmp/pprof.svg

Here's a snapshot of an example svg output, taken from the official
documentation:

.. image:: http://goog-perftools.sourceforge.net/doc/pprof-test-big.gif

Running Microbenchmarks
-----------------------

To run a set of single-node Ray microbenchmarks, use:

.. code-block:: bash

  ray microbenchmark

You can find the microbenchmark results for Ray releases in the `GitHub release logs <https://github.com/ray-project/ray/tree/master/release/release_logs>`__.

References
----------

- The `pprof documentation <http://goog-perftools.sourceforge.net/doc/cpu_profiler.html>`_.
- A `Go version of pprof <https://github.com/google/pprof>`_.
- The `gperftools <https://github.com/gperftools/gperftools>`_, including libprofiler, tcmalloc, and other goodies.
