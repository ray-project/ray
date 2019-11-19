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

  single core get calls per second 13387.15 +- 9.53
  single core put calls per second 4569.31 +- 53.59
  single core put gigabytes per second 12.64 +- 6.07
  multi core put calls per second 15667.53 +- 110.85
  multi core put gigabytes per second 22.85 +- 1.15
  single core tasks sync per second 1822.1 +- 51.61
  single core tasks async per second 6603.71 +- 39.5
  multi core tasks async per second 8161.46 +- 456.28
  single core actor calls sync per second 1374.22 +- 81.32
  single core actor calls async per second 1786.57 +- 138.77
  multi core actor calls async per second 6418.93 +- 128.0

References
----------

- The `pprof documentation <http://goog-perftools.sourceforge.net/doc/cpu_profiler.html>`_.
- A `Go version of pprof <https://github.com/google/pprof>`_.
- The `gperftools <https://github.com/gperftools/gperftools>`_, including libprofiler, tcmalloc, and other goodies.
