Dask on Ray
===========

Ray offers a scheduler backend for Dask. With this plugin, you can use familiar Dask APIs such as Dask DataFrames, and the computation will be executed by the Ray system.

The Ray plugin can be used with any Dask `.compute() <https://docs.dask.org/en/latest/api.html#dask.compute>`__ call.
Note that for execution on a Ray cluster, you should *not* use the `Dask.distributed <https://distributed.dask.org/en/latest/quickstart.html>`__ client.
Just follow the instructions for :ref:`using Ray on a cluster <using-ray-on-a-cluster>` to modify the ``ray.init()`` call.
Here's an example:

.. code-block:: python

    import ray
    from ray.util.dask import ray_dask_get
    import dask.delayed
    from time import sleep

    # Start Ray.
    # Tip: If you're connecting to an existing cluster, use ray.init(address="auto").
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

    1. If you'd like to use Dask and Ray libraries in the same application.
    2. To take advantage of Ray-specific features such as the :ref:`cluster launcher <ref-automatic-cluster>` and :ref:`shared-memory store <memory>`.

Note that Dask-on-Ray is an ongoing project and is not expected to achieve the same performance as using Ray directly.
