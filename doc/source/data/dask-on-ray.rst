.. _dask-on-ray:

Using Dask on Ray
=================


`Dask <https://dask.org/>`__ is a Python parallel computing library geared towards scaling analytics and
scientific computing workloads. It provides `big data collections
<https://docs.dask.org/en/latest/user-interfaces.html>`__ that mimic the APIs of
the familiar `NumPy <https://numpy.org/>`__ and `Pandas <https://pandas.pydata.org/>`__ libraries,
allowing those abstractions to represent
larger-than-memory data and/or allowing operations on that data to be run on a multi-machine cluster,
while also providing automatic data parallelism, smart scheduling,
and optimized operations. Operations on these collections create a task graph, which is
executed by a scheduler.

Ray provides a scheduler for Dask (`dask_on_ray`) which allows you to build data
analyses using Dask's collections and execute
the underlying tasks on a Ray cluster.

`dask_on_ray` uses Dask's scheduler API, which allows you to
specify any callable as the scheduler that you would like Dask to use to execute your
workload. Using the Dask-on-Ray scheduler, the entire Dask ecosystem can be executed on top of Ray.

.. note::
   We always ensure that the latest Dask versions are compatible with Ray nightly.
   The table below shows the latest Dask versions that are tested with Ray versions.

  .. list-table:: Latest Dask versions for each Ray version.
     :header-rows: 1

     * - Ray Version
       - Dask Version
     * - ``2.1.0``
       - ``2022.2.0``
     * - ``2.0.0``
       - ``2022.2.0``
     * - ``1.13.0``
       - ``2022.2.0``
     * - ``1.12.0``
       - ``2022.2.0``
     * - ``1.11.0``
       - ``2022.1.0``
     * - ``1.10.0``
       - ``2021.12.0``
     * - ``1.9.2``
       - ``2021.11.0``
     * - ``1.9.1``
       - ``2021.11.0``
     * - ``1.9.0``
       - ``2021.11.0``
     * - ``1.8.0``
       - ``2021.9.1``
     * - ``1.7.0``
       - ``2021.9.1``
     * - ``1.6.0``
       - ``2021.8.1``
     * - ``1.5.0``
       - ``2021.7.0``
     * - ``1.4.1``
       - ``2021.6.1``
     * - ``1.4.0``
       - ``2021.5.0``

Scheduler
---------

.. _dask-on-ray-scheduler:

The Dask-on-Ray scheduler can execute any valid Dask graph, and can be used with
any Dask `.compute() <https://docs.dask.org/en/latest/api.html#dask.compute>`__
call.
Here's an example:

.. literalinclude:: ../../../python/ray/util/dask/examples/dask_ray_scheduler_example.py
    :language: python

.. note::
  For execution on a Ray cluster, you should *not* use the
  `Dask.distributed <https://distributed.dask.org/en/latest/quickstart.html>`__
  client; simply use plain Dask and its collections, and pass ``ray_dask_get``
  to ``.compute()`` calls, set the scheduler in one of the other ways detailed `here <https://docs.dask.org/en/latest/scheduling.html#configuration>`__, or use our ``enable_dask_on_ray`` configuration helper. Follow the instructions for
  :ref:`using Ray on a cluster <cluster-index>` to modify the
  ``ray.init()`` call.

Why use Dask on Ray?

1. To take advantage of Ray-specific features such as the
      :ref:`launching cloud clusters <cluster-index>` and
      :ref:`shared-memory store <memory>`.
2. If you'd like to use Dask and Ray libraries in the same application without having two different clusters.
3. If you'd like to create data analyses using the familiar NumPy and Pandas APIs provided by Dask and execute them on a fast, fault-tolerant distributed task execution system geared towards production, like Ray.

Dask-on-Ray is an ongoing project and is not expected to achieve the same performance as using Ray directly. All `Dask abstractions <https://docs.dask.org/en/latest/user-interfaces.html>`__ should run seamlessly on top of Ray using this scheduler, so if you find that one of these abstractions doesn't run on Ray, please `open an issue <https://github.com/ray-project/ray/issues/new/choose>`__.

Best Practice for Large Scale workloads
---------------------------------------
For Ray 1.3, the default scheduling policy is to pack tasks to the same node as much as possible.
It is more desirable to spread tasks if you run a large scale / memory intensive Dask on Ray workloads.

In this case, there are two recommended setup.
- Reducing the config flag `scheduler_spread_threshold` to tell the scheduler to prefer spreading tasks across the cluster instead of packing.
- Setting the head node's `num-cpus` to 0 so that tasks are not scheduled on a head node.

.. code-block:: bash

  # Head node. Set `num_cpus=0` to avoid tasks are being scheduled on a head node.
  RAY_scheduler_spread_threshold=0.0 ray start --head --num-cpus=0

  # Worker node.
  RAY_scheduler_spread_threshold=0.0 ray start --address=[head-node-address]

Out-of-Core Data Processing
---------------------------

.. _dask-on-ray-out-of-core:

Processing datasets larger than cluster memory is supported via Ray's :ref:`object spilling <object-spilling>`: if
the in-memory object store is full, objects will be spilled to external storage (local disk by
default). This feature is available but off by default in Ray 1.2, and is on by default
in Ray 1.3+. Please see your Ray version's object spilling documentation for steps to enable and/or configure
object spilling.

Persist
-------

.. _dask-on-ray-persist:

Dask-on-Ray patches `dask.persist()
<https://docs.dask.org/en/latest/api.html#dask.persist>`__  in order to match `Dask
Distributed's persist semantics
<https://distributed.dask.org/en/latest/manage-computation.html#client-persist>`__; namely, calling `dask.persist()` with a Dask-on-Ray
scheduler will submit the tasks to the Ray cluster and return Ray futures inlined in the
Dask collection. This is nice if you wish to compute some base collection (such as
a Dask array), followed by multiple different downstream computations (such as
aggregations): those downstream computations will be faster since that base collection
computation was kicked off early and referenced by all downstream computations, often
via shared memory.

.. literalinclude:: ../../../python/ray/util/dask/examples/dask_ray_persist_example.py
    :language: python


Annotations, Resources, and Task Options
----------------------------------------

.. _dask-on-ray-annotations:


Dask-on-Ray supports specifying resources or any other Ray task option via `Dask's
annotation API <https://docs.dask.org/en/stable/api.html#dask.annotate>`__. This
annotation context manager can be used to attach resource requests (or any other Ray task
option) to specific Dask operations, with the annotations funneling down to the
underlying Ray tasks. Resource requests and other Ray task options can also be specified
globally via the ``.compute(ray_remote_args={...})`` API, which will
serve as a default for all Ray tasks launched via the Dask workload. Annotations on
individual Dask operations will override this global default.

.. literalinclude:: ../../../python/ray/util/dask/examples/dask_ray_annotate_example.py
    :language: python

Note that you may need to disable graph optimizations since it can break annotations,
see `this Dask issue <https://github.com/dask/dask/issues/7036>`__.

Custom optimization for Dask DataFrame shuffling
------------------------------------------------

.. _dask-on-ray-shuffle-optimization:

Dask-on-Ray provides a Dask DataFrame optimizer that leverages Ray's ability to
execute multiple-return tasks in order to speed up shuffling by as much as 4x on Ray.
Simply set the `dataframe_optimize` configuration option to our optimizer function, similar to how you specify the Dask-on-Ray scheduler:

.. literalinclude:: ../../../python/ray/util/dask/examples/dask_ray_shuffle_optimization.py
    :language: python

Callbacks
---------

.. _dask-on-ray-callbacks:

Dask's `custom callback abstraction <https://docs.dask.org/en/latest/diagnostics-local.html#custom-callbacks>`__
is extended with Ray-specific callbacks, allowing the user to hook into the
Ray task submission and execution lifecycles.
With these hooks, implementing Dask-level scheduler and task introspection,
such as progress reporting, diagnostics, caching, etc., is simple.

Here's an example that measures and logs the execution time of each task using
the ``ray_pretask`` and ``ray_posttask`` hooks:

.. code-block:: python

   from ray.util.dask import RayDaskCallback
   from timeit import default_timer as timer


   class MyTimerCallback(RayDaskCallback):
      def _ray_pretask(self, key, object_refs):
         # Executed at the start of the Ray task.
         start_time = timer()
         return start_time

      def _ray_posttask(self, key, result, pre_state):
         # Executed at the end of the Ray task.
         execution_time = timer() - pre_state
         print(f"Execution time for task {key}: {execution_time}s")


   with MyTimerCallback():
      # Any .compute() calls within this context will get MyTimerCallback()
      # as a Dask-Ray callback.
      z.compute(scheduler=ray_dask_get)

The following Ray-specific callbacks are provided:

   1. :code:`ray_presubmit(task, key, deps)`: Run before submitting a Ray
      task. If this callback returns a non-`None` value, a Ray task will _not_
      be created and this value will be used as the would-be task's result
      value.
   2. :code:`ray_postsubmit(task, key, deps, object_ref)`: Run after submitting
      a Ray task.
   3. :code:`ray_pretask(key, object_refs)`: Run before executing a Dask task
      within a Ray task. This executes after the task has been submitted,
      within a Ray worker. The return value of this task will be passed to the
      ray_posttask callback, if provided.
   4. :code:`ray_posttask(key, result, pre_state)`: Run after executing a Dask
      task within a Ray task. This executes within a Ray worker. This callback
      receives the return value of the ray_pretask callback, if provided.
   5. :code:`ray_postsubmit_all(object_refs, dsk)`: Run after all Ray tasks
      have been submitted.
   6. :code:`ray_finish(result)`: Run after all Ray tasks have finished
      executing and the final result has been returned.

See the docstring for
:meth:`RayDaskCallback.__init__() <ray.util.dask.callbacks.RayDaskCallback>.__init__`
for further details about these callbacks, their arguments, and their return
values.

When creating your own callbacks, you can use
:class:`RayDaskCallback <ray.util.dask.callbacks.RayDaskCallback>`
directly, passing the callback functions as constructor arguments:

.. code-block:: python

   def my_presubmit_cb(task, key, deps):
      print(f"About to submit task {key}!")

   with RayDaskCallback(ray_presubmit=my_presubmit_cb):
      z.compute(scheduler=ray_dask_get)

or you can subclass it, implementing the callback methods that you need:

.. code-block:: python

   class MyPresubmitCallback(RayDaskCallback):
      def _ray_presubmit(self, task, key, deps):
         print(f"About to submit task {key}!")

   with MyPresubmitCallback():
      z.compute(scheduler=ray_dask_get)

You can also specify multiple callbacks:

.. code-block:: python

   # The hooks for both MyTimerCallback and MyPresubmitCallback will be
   # called.
   with MyTimerCallback(), MyPresubmitCallback():
      z.compute(scheduler=ray_dask_get)

Combining Dask callbacks with an actor yields simple patterns for stateful data
aggregation, such as capturing task execution statistics and caching results.
Here is an example that does both, caching the result of a task if its
execution time exceeds some user-defined threshold:

.. code-block:: python

   @ray.remote
   class SimpleCacheActor:
      def __init__(self):
         self.cache = {}

      def get(self, key):
         # Raises KeyError if key isn't in cache.
         return self.cache[key]

      def put(self, key, value):
         self.cache[key] = value


   class SimpleCacheCallback(RayDaskCallback):
      def __init__(self, cache_actor_handle, put_threshold=10):
         self.cache_actor = cache_actor_handle
         self.put_threshold = put_threshold

      def _ray_presubmit(self, task, key, deps):
         try:
            return ray.get(self.cache_actor.get.remote(str(key)))
         except KeyError:
            return None

      def _ray_pretask(self, key, object_refs):
         start_time = timer()
         return start_time

      def _ray_posttask(self, key, result, pre_state):
         execution_time = timer() - pre_state
         if execution_time > self.put_threshold:
            self.cache_actor.put.remote(str(key), result)


   cache_actor = SimpleCacheActor.remote()
   cache_callback = SimpleCacheCallback(cache_actor, put_threshold=2)
   with cache_callback:
      z.compute(scheduler=ray_dask_get)

.. note::
  The existing Dask scheduler callbacks (``start``, ``start_state``,
  ``pretask``, ``posttask``, ``finish``) are also available, which can be used to
  introspect the Dask task to Ray task conversion process, but note that the ``pretask``
  and ``posttask`` hooks are executed before and after the Ray task is *submitted*, not
  executed, and that ``finish`` is executed after all Ray tasks have been
  *submitted*, not executed.

This callback API is currently unstable and subject to change.
