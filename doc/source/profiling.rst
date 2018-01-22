Profiling Ray
============================

This document details, for Ray developers, how to use ``pprof`` to profile Ray binaries.

Installation
-----------
Previous attempts to get ``pprof`` to correctly symbolize on Mac OS have all
failed; this document describes the workflow on Ubuntu machines.

.. code-block:: bash

   sudo apt-get install google-perftools libgoogle-perftools-dev

Changes to compilation and linking
-----------
Let's say we want to profile ``plasma_manager``.  Change the link instruction in ``src/plasma/CMakeLists.txt`` from

.. code-block:: cmake

   target_link_libraries(plasma_manager common ${PLASMA_STATIC_LIB} ray_static ${ARROW_STATIC_LIB} -lpthread)

to additionally include ``-lprofiler``:

.. code-block:: cmake

   target_link_libraries(plasma_manager common ${PLASMA_STATIC_LIB} ray_static ${ARROW_STATIC_LIB} -lpthread -lprofiler)

Additionally, add ``-g -ggdb`` to ``CMAKE_C_FLAGS`` and ``CMAKE_CXX_FLAGS`` to enable
the debug symbols.  (Keeping ``-O3`` seems okay.)

Recompile.

Launching the to-profile binary
-----------
In various places, instead of launching the target binary via ``plasma_manager``, do:

.. code-block:: bash

   $ LD_PRELOAD=/usr/lib/libprofiler.so CPUPROFILE=/tmp/pprof.out plasma_manager <args>

The ``/tmp/pprof.out`` will be empty, until you let the binary run the target
workload for a while, then ``kill`` it.

Visualizing the CPU profile
-----------
``pprof`` has many options of visualizing/displaying the CPU profile; here we
output it as a zoomable ``.svg`` image displaying the call graph annotated with
hot paths.

.. code-block:: bash

    $ google-pprof -svg plasma_manager /tmp/pprof.out >/tmp/pprof.svg
    # Then open the .svg file with Chrome.

    # If you realize the call graph is too large, use -focus=<regex> to zoom
    # into subtrees.
    $ google-pprof -focus=<func> -svg plasma_manager /tmp/pprof.out >/tmp/pprof.svg

Here's a snapshot of an example svg output, taken from the official documentation:

.. image:: http://goog-perftools.sourceforge.net/doc/pprof-test-big.gif

References
-----------
`pprof documentation <http://goog-perftools.sourceforge.net/doc/cpu_profiler.html>`_.

`Go version of pprof <https://github.com/google/pprof>`_.

`gperftools <https://github.com/gperftools/gperftools>`_, including libprofiler, tcmalloc, and other goodies.
