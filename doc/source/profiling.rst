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

The following are the results for the 0.7.5 release on a Python 3 / a m4.16xl instance:

.. code-block:: text

  single core get calls per second 12169.8 +- 386.41
  single core put calls per second 3117.45 +- 94.17
  single core put gigabytes per second 11.32 +- 3.4
  multi core put calls per second 16221.06 +- 895.13
  multi core put gigabytes per second 24.14 +- 0.29
  single core tasks sync per second 887.77 +- 3.69
  single core tasks async per second 4524.45 +- 196.39
  multi core tasks async per second 6963.49 +- 161.31
  single core actor calls sync per second 762.4 +- 56.47
  single core actor calls async per second 1030.44 +- 45.42
  multi core actor calls async per second 6065.92 +- 175.05

References
----------

- The `pprof documentation <http://goog-perftools.sourceforge.net/doc/cpu_profiler.html>`_.
- A `Go version of pprof <https://github.com/google/pprof>`_.
- The `gperftools <https://github.com/gperftools/gperftools>`_, including libprofiler, tcmalloc, and other goodies.
