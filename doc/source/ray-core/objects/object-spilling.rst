Object Spilling
===============
.. _object-spilling:

Ray 1.3+ spills objects to external storage once the object store is full. By default, objects are spilled to Ray's temporary directory in the local filesystem.

Single node
-----------

Ray uses object spilling by default. Without any setting, objects are spilled to `[temp_folder]/spill`. `temp_folder` is `/tmp` for Linux and MacOS by default.

To configure the directory where objects are placed, use:

.. code-block:: python

    ray.init(
        _system_config={
            "object_spilling_config": json.dumps(
                {"type": "filesystem", "params": {"directory_path": "/tmp/spill"}},
            )
        },
    )

You can also specify multiple directories for spilling to spread the IO load and disk space
usage across multiple physical devices if needed (e.g., SSD devices):

.. code-block:: python

    ray.init(
        _system_config={
            "max_io_workers": 4,  # More IO workers for parallelism.
            "object_spilling_config": json.dumps(
                {
                  "type": "filesystem",
                  "params": {
                    # Multiple directories can be specified to distribute
                    # IO across multiple mounted physical devices.
                    "directory_path": [
                      "/tmp/spill",
                      "/tmp/spill_1",
                      "/tmp/spill_2",
                    ]
                  },
                }
            )
        },
    )

.. note::
  
  To optimize the performance, it is recommended to use SSD instead of HDD when using object spilling for memory intensive workloads.

If you are using an HDD, it is recommended that you specify a large buffer size (> 1MB) to reduce IO requests during spilling.

.. code-block:: python

    ray.init(
        _system_config={
            "object_spilling_config": json.dumps(
                {
                  "type": "filesystem", 
                  "params": {
                    "directory_path": "/tmp/spill",
                    "buffer_size": 1_000_000,
                  }
                },
            )
        },
    )

To enable object spilling to remote storage (any URI supported by `smart_open <https://pypi.org/project/smart-open/>`__):

.. code-block:: python

    ray.init(
        _system_config={
            "max_io_workers": 4,  # More IO workers for remote storage.
            "min_spilling_size": 100 * 1024 * 1024,  # Spill at least 100MB at a time.
            "object_spilling_config": json.dumps(
                {
                  "type": "smart_open", 
                  "params": {
                    "uri": "s3://bucket/path"
                  },
                  "buffer_size": 100 * 1024 * 1024,  # Use a 100MB buffer for writes
                },
            )
        },
    )

It is recommended that you specify a large buffer size (> 1MB) to reduce IO requests during spilling.

Spilling to multiple remote storages is also supported.

.. code-block:: python

    ray.init(
        _system_config={
            "max_io_workers": 4,  # More IO workers for remote storage.
            "min_spilling_size": 100 * 1024 * 1024,  # Spill at least 100MB at a time.
            "object_spilling_config": json.dumps(
                {
                  "type": "smart_open", 
                  "params": {
                    "uri": ["s3://bucket/path1", "s3://bucket/path2, "s3://bucket/path3"],
                  },
                  "buffer_size": 100 * 1024 * 1024, # Use a 100MB buffer for writes
                },
            )
        },
    )

Remote storage support is still experimental.

Cluster mode
------------
To enable object spilling in multi node clusters:

.. code-block:: bash
  
  # Note that `object_spilling_config`'s value should be json format.
  ray start --head --system-config='{"object_spilling_config":"{\"type\":\"filesystem\",\"params\":{\"directory_path\":\"/tmp/spill\"}}"}'

Stats
-----

When spilling is happening, the following INFO level messages will be printed to the raylet logs (e.g., ``/tmp/ray/session_latest/logs/raylet.out``)::

  local_object_manager.cc:166: Spilled 50 MiB, 1 objects, write throughput 230 MiB/s
  local_object_manager.cc:334: Restored 50 MiB, 1 objects, read throughput 505 MiB/s

You can also view cluster-wide spill stats by using the ``ray memory`` command::

  --- Aggregate object store stats across all nodes ---
  Plasma memory usage 50 MiB, 1 objects, 50.0% full
  Spilled 200 MiB, 4 objects, avg write throughput 570 MiB/s
  Restored 150 MiB, 3 objects, avg read throughput 1361 MiB/s

If you only want to display cluster-wide spill stats, use ``ray memory --stats-only``.
