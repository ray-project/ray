Dask on Ray
===========

Ray offers an experimental scheduler backend for Dask.
With this plugin, you can use familiar Dask APIs such as Dask DataFrames, and the computation will be executed by the Ray system.

Here's an example:

.. code-block:: python

    import ray
    from ray.experimental.dask import ray_dask_get
    import dask.delayed
    from time import sleep

    # Start Ray.
    # Tip: If you're connecting to a cluster, use ray.init(address="auto").
    ray.init()


    def inc(x):
        sleep(1)
        return x + 1

    def add(x, y):
        sleep(1)
        return x + y

    x = dask.delayed(inc)(1)
    y = dask.delayed(inc)(2)
    z = dask.delayed(add)(x, y)
    # The Dask scheduler submits the recorded task graph to Ray.
    z.compute(scheduler=ray_dask_get)

Why use this feature?
- If you'd like to use Dask and Ray libraries in the same application.
- To take advantage of Ray-specific features such as the :ref:`cluster launcher <ref-automatic-cluster>` and :ref:`shared-memory store <memory>`.

Note that Dask-on-Ray is an ongoing project and is not expected to achieve the same performance as using Ray directly.
