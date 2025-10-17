Object Spilling
===============
.. _object-spilling:

Ray spills objects to a directory in the local filesystem once the object store is full. By default, Ray 
spills objects to the temporary directory (for example, ``/tmp/ray/session_2025-03-28_00-05-20_204810_2814690``). 

Spilling to a custom directory
-------------------------------

You can specify a custom directory for spilling objects by setting the 
``object_spilling_directory`` parameter in the ``ray.init`` function or the 
``--object-spilling-directory`` command line option in the ``ray start`` command.

.. tab-set::

    .. tab-item:: Python

        .. doctest::

            ray.init(object_spilling_directory="/path/to/spill/dir")

    .. tab-item:: CLI

        .. doctest::

            ray start --object-spilling-directory=/path/to/spill/dir

For advanced usage and customizations, reach out to the `Ray team <https://www.ray.io/community>`_.

Stats
-----

When spilling is happening, the following INFO level messages are printed to the Raylet logs. For example, ``/tmp/ray/session_latest/logs/raylet.out``::

  local_object_manager.cc:166: Spilled 50 MiB, 1 objects, write throughput 230 MiB/s
  local_object_manager.cc:334: Restored 50 MiB, 1 objects, read throughput 505 MiB/s

You can also view cluster-wide spill stats by using the ``ray memory`` command::

  --- Aggregate object store stats across all nodes ---
  Plasma memory usage 50 MiB, 1 objects, 50.0% full
  Spilled 200 MiB, 4 objects, avg write throughput 570 MiB/s
  Restored 150 MiB, 3 objects, avg read throughput 1361 MiB/s

If you only want to display cluster-wide spill stats, use ``ray memory --stats-only``.
