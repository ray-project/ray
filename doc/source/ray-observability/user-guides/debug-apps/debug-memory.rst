.. _ray-core-mem-profiling:

Debugging Memory Issues
-----------------------

To memory profile Ray tasks or actors, use `memray <https://bloomberg.github.io/memray/>`_.
Note that you can also use other memory profiling tools if it supports a similar API.

First, install ``memray``.

.. code-block:: bash

  pip install memray

``memray`` supports a Python context manager to enable memory profiling. You can write the ``memray`` profiling file wherever you want.
But in this example, we will write them to `/tmp/ray/session_latest/logs` because Ray dashboard allows you to download files inside the log folder.
This will allow you to download profiling files from other nodes.

.. tab-set::

    .. tab-item:: Actors

      .. literalinclude:: ../../doc_code/memray_profiling.py
          :language: python
          :start-after: __memray_profiling_start__
          :end-before: __memray_profiling_end__

    .. tab-item:: Tasks

      Note that tasks have a shorter lifetime, so there could be lots of memory profiling files.

      .. literalinclude:: ../../doc_code/memray_profiling.py
          :language: python
          :start-after: __memray_profiling_task_start__
          :end-before: __memray_profiling_task_end__

Once the task or actor runs, go to the :ref:`Logs View <dash-logs-view>` of the dashboard. Find and click the log file name.

.. image:: ../../images/memory-profiling-files.png
    :align: center

Click the download button. 

.. image:: ../../images/download-memory-profiling-files.png
    :align: center

Now, you have the memory profiling file. Running

.. code-block:: bash

  memray flamegraph <memory profiling bin file>

And you can see the result of the memory profiling!

