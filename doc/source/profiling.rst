Profiling for Ray Developers
============================

This document details, for Ray developers, how to use ``pprof`` to profile Ray
binaries.

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

  export RAYLET_PERFTOOLS_PATH=/usr/lib/x86_64-linux-gnu/libprofiler.so
  export RAYLET_PERFTOOLS_LOGFILE=/tmp/pprof.out


The file ``/tmp/pprof.out`` will be empty until you let the binary run the
target workload for a while and then ``kill`` it via ``ray stop`` or by
letting the driver exit.

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

The following are the results for the 0.7.6 release on a m4.16xl instance running
Ubuntu 18.04 and Python 3.6:

.. code-block:: text

  single client get calls per second 28595.02 +- 580.33
  single client put calls per second 6313.62 +- 66.88
  single client put gigabytes per second 11.6 +- 6.86
  multi client put calls per second 16800.89 +- 381.69
  multi client put gigabytes per second 23.33 +- 0.96
  single client tasks sync per second 1963.72 +- 48.48
  single client tasks async per second 5181.29 +- 30.0
  multi client tasks async per second 5566.7 +- 280.72
  1:1 actor calls sync per second 1595.47 +- 38.32
  1:1 actor calls async per second 2496.26 +- 37.62
  1:1 direct actor calls async per second 7233.63 +- 205.75
  n:n actor calls async per second 5357.63 +- 116.9
  n:n direct actor calls async per second 90703.32 +- 805.56
  n:n direct actor calls with arg async per second 13300.47 +- 532.66

References
----------

- The `pprof documentation <http://goog-perftools.sourceforge.net/doc/cpu_profiler.html>`_.
- A `Go version of pprof <https://github.com/google/pprof>`_.
- The `gperftools <https://github.com/gperftools/gperftools>`_, including libprofiler, tcmalloc, and other goodies.
