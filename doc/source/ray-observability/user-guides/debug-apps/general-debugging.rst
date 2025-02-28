.. _observability-general-debugging:

Common Cluster Issues
=======================

Distributed applications are more powerful yet complicated than non-distributed ones. Some of Ray's behavior might catch
users off guard, though there may be sound arguments for these design choices.

This page lists common issues users may run into. In particular, there are some key differences to be aware of when running Ray in a cluster
compared to running Ray locally.

Environment variables aren't passed from the Driver process to Worker processes
---------------------------------------------------------------------------------

**Issue:** If you set an environment variable at the command line (where you run your Driver), it isn't passed to all the Workers running in the Cluster
*if the Cluster was started previously*.

**Example:** If you have a file ``baz.py`` in the directory where you run Ray, and you run the following command:

.. literalinclude:: /ray-observability/doc_code/gotchas.py
   :language: python
   :start-after: __env_var_start__
   :end-before: __env_var_end__

**Expected behavior:** Most people expect (as if it were a single process on a single machine) that the environment variables are the same in all Workers. It isn't.

**Fix:** Use Runtime Environments to pass environment variables explicitly. If you call ``ray.init(runtime_env=...)``, then the Workers have the environment variable set.

.. literalinclude:: /ray-observability/doc_code/gotchas.py
   :language: python
   :start-after: __env_var_fix_start__
   :end-before: __env_var_fix_end__


Filenames work sometimes and not at other times
-----------------------------------------------

Issue: If you reference a file by name in a Task or Actor,
it sometimes works and sometimes fails. This happens because if the Task or Actor runs on the Head Node— it works—but if it runs on another machine, it doesn't.

Example: Let's say you do the following command:

.. code-block:: bash

   % touch /tmp/foo.txt

And I have this code:

.. testcode::

  import os
  import ray

  @ray.remote
  def check_file():
      foo_exists = os.path.exists("/tmp/foo.txt")
      return foo_exists

  futures = []
  for _ in range(1000):
      futures.append(check_file.remote())

  print(ray.get(futures))

Then you get a mix of True and False. If ``check_file()`` runs on the Head Node, or when running locally, it works; but if it runs on a Worker Node, it returns False.

**Expected behavior:** Most people expect this to either fail or succeed consistently. It's the same code, after all.

**Fix:**

— Use only shared paths for such applications. For example, if you are using a network file system, you can use that, or the files can reside on S3.
— Do not rely on file path consistency.


Placement Groups aren't composable
-----------------------------------

**Issue:** If a task called from something running in a Placement Group uses Ray Tasks, resources aren't allocated and it hangs.

**Example:** You are using Ray Tune, which creates Placement Groups, and you want to apply it to an objective function that uses Ray Tasks. For example:

.. testcode::

  import ray
  from ray import tune

  def create_task_that_uses_resources():
      @ray.remote(num_cpus=10, scheduling_strategy=PlacementGroupSchedulingStrategy(placement_group=None))
      def sample_task():
          print("Hello")
          return

      return ray.get([sample_task.remote() for i in range(10)])

  def objective(config):
      create_task_that_uses_resources()

  tuner = tune.Tuner(objective, param_space={"a": 1})
  tuner.fit()

This errors with the message:

.. testoutput::
  :options: +MOCK

    ValueError: Cannot schedule create_task_that_uses_resources.<locals>.sample_task with the placement group
    because the resource request {'CPU': 10} cannot fit into any bundles for the placement group, [{'CPU': 1.0}].

**Expected behavior:** The above executes.

**Fix:** In the ``@ray.remote`` declaration of Tasks called by ``create_task_that_uses_resources()``, include a
``scheduling_strategy=PlacementGroupSchedulingStrategy(placement_group=None)``.

.. code-block:: diff

  def create_task_that_uses_resources():
  +     @ray.remote(num_cpus=10, scheduling_strategy=PlacementGroupSchedulingStrategy(placement_group=None))
  -     @ray.remote(num_cpus=10)

Outdated Function Definitions
-----------------------------

Because of Python subtleties, if you redefine a remote function, you may not always get the expected behavior. In this case, it may be that Ray isn't running the newest version of the function.

Suppose you define a remote function ``f`` and then redefine it. Ray should use the newest version.

.. testcode::

  import ray

  @ray.remote
  def f():
      return 1

  @ray.remote
  def f():
      return 2

  print(ray.get(f.remote()))  # This should be 2.

.. testoutput::

  2

However, the following are cases where modifying the remote function does not update Ray to the new version (at least without stopping and restarting Ray):

— **The function is imported from an external file":** In this case, 
if you import the file (e.g., ``file.py``) and then 
change the definition of ``f`` in that file, a re-import won't update 
``f`` because the second import is treated as a no-op.


  A solution is to use ``from importlib import reload; reload(file)`` instead of a second
  ``import file``. Reloading causes the new definition of ``f`` to be
  re-executed and distributed to the other machines.

— **The function relies on a helper function from an external file:** 
In this case, ``f`` can be defined within your Ray application but relies
 on a helper function ``h`` defined in an external file ``file.py``. Changing the 
 definition of ``h`` in ``file.py`` and redefining ``f`` will not 
 update Ray to use the new version of ``h``.

  This happens because when ``f`` is first defined, its definition is shipped to 
  all Worker processes and unpickled. During unpickling, ``file.py`` is 
  imported in the Workers. Then when ``f`` is redefined, its definition is 
  shipped and unpickled again in all Workers. However, this subsequent import 
  is ignored because ``file.py`` has been previously imported in the Workers.

  The easiest solution is to restart the Ray cluster when you update the function definition.

  Another solution is to redefine ``f`` to reload ``file.py`` before calling ``h``. For example, if inside ``file.py`` you have:

  .. testcode::

      def h():
          return 1

  And you define the remote function ``f`` as:

  .. testcode::

      @ray.remote
      def f():
          return file.h()

  You can redefine ``f`` as follows:

  .. testcode::

      @ray.remote
      def f():
          from importlib import reload
          reload(file)
          return file.h()

  This forces the reload to occur on the Workers. Note that in Python 3, you need to do ``from importlib import reload``.


Capture task and actor call sites
---------------------------------

Ray can optionally capture and display the stack trace where your code invokes tasks, creates actors, or calls actor methods. 
This feature helps debug and understand the execution flow of your application.

To enable call site capture, set the environment variable ``RAY_record_task_actor_creation_sites=true``. When enabled:

— Ray captures the stack trace when creating tasks, actors, or calling actor methods.
— The captured stack trace is visible in:
   — the Ray Dashboard UI under the task details and actor details pages,
   — the output of the CLI command ``ray list task --detail``,
   — state API responses.

Note that stack trace capture is disabled by default to avoid performance overhead. Enable it only when needed for debugging.

Example:

.. testcode::

    import ray

    # Enable stack trace capture
    ray.init(runtime_env={"env_vars": {"RAY_record_task_actor_creation_sites": "true"}})

    @ray.remote
    def my_task():
        return 42

    # Capture the stack trace upon task invocation.
    future = my_task.remote()
    result = ray.get(future)

    @ray.remote
    class Counter:
        def __init__(self):
            self.value = 0

        def increment(self):
            self.value += 1
            return self.value

    # Capture the stack trace upon actor creation.
    counter = Counter.remote()

    # Capture the stack trace upon method invocation.
    counter.increment.remote()

This document discusses some common problems that people run into when using Ray as well as some known problems. If you encounter other problems, `let us know`_.

.. _`let us know`: https://github.com/ray-project/ray/issues
