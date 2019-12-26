Multiprocessing API on Ray
==========================

.. warning::

  Support for the multiprocessing API on Ray is an experimental feature,
  so it may be changed at any time without warning. If you encounter any
  bugs/shortcomings that aren't listed in the `Known Limitations`_ section
  below, please file an `issue on GitHub`_. Contributions are always welcome!

Ray offers a subset of the `Python multiprocessing API`_ that makes it easy
for you to scale existing applications that use multiprocessing to a cluster.

Features
--------

Currently, only the ``multiprocessing.Pool`` API is supported.

Usage
-----

Just replace
``from multiprocessing import Pool`` with
``from ray.experimental.multiprocessing import Pool`` for a drop-in replacement.

If there is no Ray cluster running (i.e., started with ``ray.init``), a new one
will be instantiated when the first ``Pool`` is created. If a local Ray cluster is
already running, a new one won't be created.

If you want to connect to an existing Ray cluster than spans multiple machines,
just set the environment variable ``RAY_ADDRESS=<address>``. If running on the head
node, you can use ``RAY_ADDRESS=auto``.

Known Limitations
-----------------

Don't support ``callback`` and ``error_callback`` arguments.
Initializer cannot be used to modify the global namespace (e.g., import packages or set global variables).
Can't pickle generator objects.
imap and imap_unordered submit the full iterable immediately instead of lazily.
