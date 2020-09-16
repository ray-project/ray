***********
Dask on Ray
***********

Ray offers a scheduler integration for Dask, allowing you to build data
analyses using the familiar Dask collections (dataframes, arrays) and execute
the underlying computations on a Ray cluster. Using this Dask scheduler, the
entire Dask ecosystem can be executed on top of Ray.

.. note::

  Note that Ray does not currently support object spilling, and hence cannot
  process datasets larger than cluster memory. This is a planned feature.

=========
Scheduler
=========

The Dask-Ray scheduler can execute any valid Dask graph, and can be used with
any Dask `.compute() <https://docs.dask.org/en/latest/api.html#dask.compute>`__
call.
Here's an example:

.. code-block:: python

   import ray
   from ray.util.dask import ray_dask_get
   import dask.delayed
   import time

   # Start Ray.
   # Tip: If you're connecting to an existing cluster, use ray.init(address="auto").
   ray.init()


   @dask.delayed
   def inc(x):
       time.sleep(1)
       return x + 1

   @dask.delayed
   def add(x, y):
       time.sleep(3)
       return x + y

   x = inc(1)
   y = inc(2)
   z = add(x, y)
   # The Dask scheduler submits the underlying task graph to Ray.
   z.compute(scheduler=ray_dask_get)

Why use Dask on Ray?

   1. If you'd like to create data analyses using the familiar NumPy and Pandas
      APIs provided by Dask and execute them on a production-ready distributed
      task execution system like Ray.
   2. If you'd like to use Dask and Ray libraries in the same application
      without having two different task execution backends.
   3. To take advantage of Ray-specific features such as the
      :ref:`cluster launcher <ref-automatic-cluster>` and
      :ref:`shared-memory store <memory>`.

Note that for execution on a Ray cluster, you should *not* use the
`Dask.distributed <https://distributed.dask.org/en/latest/quickstart.html>`__
client; simply use plain Dask and its collections, and pass ``ray_dask_get``
to ``.compute()`` calls. Follow the instructions for
:ref:`using Ray on a cluster <using-ray-on-a-cluster>` to modify the
``ray.init()`` call.

Dask-on-Ray is an ongoing project and is not expected to achieve the same performance as using Ray directly.

=========
Callbacks
=========

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

Note that the existing Dask scheduler callbacks (``start``, ``start_state``,
``pretask``, ``posttask``, ``finish``) are also available, which can be used to
introspect the Dask task to Ray task conversion process, but that ``pretask``
and ``posttask`` are executed before and after the Ray task is *submitted*, not
executed, and that ``finish`` is executed after all Ray tasks have been
*submitted*, not executed.

This callback API is currently unstable and subject to change.
