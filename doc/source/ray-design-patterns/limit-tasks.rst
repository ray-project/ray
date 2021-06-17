Pattern: Using ray.wait to limit the number of in-flight tasks
==============================================================


Example use case
----------------

When you submit a ray task or actor call, Ray will make sure the data is available to the worker. However, if you submit too many tasks rapidly, the worker might be overloaded and run out of memory. You should use ray.wait to block until a certain number of tasks are ready.

Ray Serve uses this pattern to limit the number of in flight queries for each worker.

.. figure:: limit-tasks.svg

    Limit number of parallel tasks


Code example
------------

**Without backpressure:**

.. code-block:: python

    @ray.remote
    class Actor:
        def heavy_compute(self, large_array):
            # taking a long time...

    actor = Actor.remote()
    result_refs = []
    for i in range(1_000_000):
        large_array = np.zeros(1_000_000)
        result_refs.append(actor.heavy_compute.remote(large_array))
    results = ray.get(result_refs)

**With backpressure:**

.. code-block:: python

    result_refs = []
    for i in range(1_000_000):
        large_array = np.zeros(1_000_000)

        # Allow 1000 in flight calls
        # For example, if i = 5000, this call blocks until that
        # 4000 of the object_refs in result_refs are ready
        # and available.
          if len(result_refs) > 1000:
            num_ready = i-1000
            ray.wait(result_refs, num_returns=num_ready)

        result_refs.append(actor.heavy_compute.remote(large_array))
