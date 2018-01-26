Profiling Ray
=============

This document details, for Ray developers, how to use ``pprof`` to profile Ray
binaries.

Installation
------------

These instructions are for Ubuntu only. Attempts to get ``pprof`` to correctly
symbolize on Mac OS have failed.

.. code-block:: bash

  sudo apt-get install google-perftools libgoogle-perftools-dev

Changes to compilation and linking
----------------------------------

Let's say we want to profile the ``plasma_manager``.  Change the link
instruction in ``src/plasma/CMakeLists.txt`` from

.. code-block:: cmake

  target_link_libraries(plasma_manager common ${PLASMA_STATIC_LIB} ray_static ${ARROW_STATIC_LIB} -lpthread)

to additionally include ``-lprofiler``:

.. code-block:: cmake

  target_link_libraries(plasma_manager common ${PLASMA_STATIC_LIB} ray_static ${ARROW_STATIC_LIB} -lpthread -lprofiler)

Additionally, add ``-g -ggdb`` to ``CMAKE_C_FLAGS`` and ``CMAKE_CXX_FLAGS`` to
enable the debug symbols.  (Keeping ``-O3`` seems okay.)

Recompile.

Launching the to-profile binary
-------------------------------

In various places, instead of launching the target binary via
``plasma_manager <args>``, it must be launched with

.. code-block:: bash

  LD_PRELOAD=/usr/lib/libprofiler.so CPUPROFILE=/tmp/pprof.out plasma_manager <args>

In practice, this means modifying ``python/ray/plasma/plasma.py`` so that the
manager is launched with a command that passes a ``modified_env`` into
``Popen``.

.. code-block:: python

  modified_env = os.environ.copy()
  modified_env["LD_PRELOAD"] = "/usr/lib/libprofiler.so"
  modified_env["CPUPROFILE"] = "/tmp/pprof.out"

  process = subprocess.Popen(command,
                             stdout=stdout_file,
                             stderr=stderr_file,
                             env=modified_env)

The file ``/tmp/pprof.out`` will be empty until you let the binary run the
target workload for a while and then ``kill`` it.

Visualizing the CPU profile
---------------------------

The output of ``pprof`` can be visualized in many ways. Here we output it as a
zoomable ``.svg`` image displaying the call graph annotated with hot paths.

.. code-block:: bash

  # Use the appropriate path.
  PLASMA_MANAGER=ray/python/ray/core/src/plasma/plasma_manager

  google-pprof -svg $PLASMA_MANAGER /tmp/pprof.out > /tmp/pprof.svg
  # Then open the .svg file with Chrome.

  # If you realize the call graph is too large, use -focus=<some function> to zoom
  # into subtrees.
  google-pprof -focus=epoll_wait -svg $PLASMA_MANAGER /tmp/pprof.out > /tmp/pprof.svg

Here's a snapshot of an example svg output, taken from the official
documentation:

.. image:: http://goog-perftools.sourceforge.net/doc/pprof-test-big.gif

References
----------

- The `pprof documentation <http://goog-perftools.sourceforge.net/doc/cpu_profiler.html>`_.
- A `Go version of pprof <https://github.com/google/pprof>`_.
- The `gperftools <https://github.com/gperftools/gperftools>`_, including libprofiler, tcmalloc, and other goodies.
