Pattern: Using pipelining to increase throughput
================================================

If you have multiple work items and each requires several steps to complete,
you can use the `pipelining <https://en.wikipedia.org/wiki/Pipeline_(computing)>`__ technique to improve the cluster utilization and increase the throughput of your system.

.. note::

  Pipelining is an important technique to improve the performance and is heavily used by Ray libraries.
  See :ref:`Ray Dataset pipelines <pipelining_datasets>` as an example.

.. figure:: ../images/pipelining.svg

Example use case
----------------

A component of your application needs to do both compute-intensive work and communicate with other processes.
Ideally, you want to overlap computation and communication to saturate the CPU and increase the overall throughput.

Code example
------------

.. literalinclude:: ../doc_code/pattern_pipelining.py

In the example above, a worker actor pulls work off of a queue and then does some computation on it.
Without pipelining, we call :ref:`ray.get() <ray-get-ref>` immediately after requesting a work item, so we block while that RPC is in flight, causing idle CPU time.
With pipelining, we instead preemptively request the next work item before processing the current one, so we can use the CPU while the RPC is in flight which increases the CPU utilization.
