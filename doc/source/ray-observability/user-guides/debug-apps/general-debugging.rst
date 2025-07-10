.. _observability-general-debugging:

Common Issues
=======================

Distributed applications offer great power but also increased complexity. 
Some of Ray's behaviors may initially surprise users, but these design choices serve important purposes in distributed computing environments.

This document outlines common issues encountered when running Ray in a cluster, highlighting key differences compared to running Ray locally.

Environment variables aren't passed from the Driver process to Worker processes
---------------------------------------------------------------------------------

**Issue:** When you set an environment variable on your Driver, it isn't propagated to the Worker processes.

**Example:** Suppose you have a file ``baz.py`` in the directory where you run Ray, and you execute the following command:

.. literalinclude:: /ray-observability/doc_code/gotchas.py
   :language: python
   :start-after: __env_var_start__
   :end-before: __env_var_end__


**Expected behavior:** Users may expect that setting environment variables on the Driver sends them to all Worker processes as if running on a single machine, but it doesn't.

**Fix:** Enable Runtime Environments to explicitly pass environment variables. When you call ``ray.init(runtime_env=...)``, it sends the specified environment variables to the Workers.
Alternatively, you can set the environment variables as part of your cluster setup configuration.

.. literalinclude:: /ray-observability/doc_code/gotchas.py
   :language: python
   :start-after: __env_var_fix_start__
   :end-before: __env_var_fix_end__


Filenames work sometimes and not at other times
-----------------------------------------------

**Issue:** Referencing a file by its name in a Task or Actor may sometimes succeed and sometimes fail. 
This inconsistency arises because the Task or Actor finds the file when running on the Head Node, but the file might not exist on other machines.

**Example:** Consider the following scenario:

.. code-block:: bash

   % touch /tmp/foo.txt

And this code:

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

In this case, you might receive a mixture of True and False. If ``check_file()`` runs on the Head Node or locally, it finds the file; however, on a Worker Node, it doesn't.

**Expected behavior:** Users generally expect file references to either work consistently or to reliably fail, rather than behaving inconsistently.

**Fix:**

— Use only shared file paths for such applications. For example, a network file system or S3 storage can provide the required consistency.
— Avoid relying on local files to be consistent across machines.


Placement Groups aren't composable
-----------------------------------

**Issue:** If you schedule a new task from the tasks or actors running within a Placement Group, the system might fail to allocate resources properly, causing the operation to hang.

**Example:** Imagine you are using Ray Tune (which creates Placement Groups) and want to apply it to an objective function that in turn uses Ray Tasks. For example:

.. testcode::

  import ray
  from ray import tune
  from ray.util.placement_group import PlacementGroupSchedulingStrategy

  def create_task_that_uses_resources():
      @ray.remote(num_cpus=10)
      def sample_task():
          print("Hello")
          return

      return ray.get([sample_task.remote() for i in range(10)])

  def objective(config):
      create_task_that_uses_resources()

  tuner = tune.Tuner(objective, param_space={"a": 1})
  tuner.fit()

This code errors with the message:

.. code-block::

    ValueError: Cannot schedule create_task_that_uses_resources.<locals>.sample_task with the placement group
    because the resource request {'CPU': 10} cannot fit into any bundles for the placement group, [{'CPU': 1.0}].

**Expected behavior:** The code executes successfully without resource allocation issues.

**Fix:** Ensure that in the ``@ray.remote`` declaration of tasks called within ``create_task_that_uses_resources()``, you include the parameter
``scheduling_strategy=PlacementGroupSchedulingStrategy(placement_group=None)``.

.. code-block:: diff

  def create_task_that_uses_resources():
  +     @ray.remote(num_cpus=10, scheduling_strategy=PlacementGroupSchedulingStrategy(placement_group=None))
  -     @ray.remote(num_cpus=10)


Outdated Function Definitions
-----------------------------

Because of Python's subtleties, redefining a remote function may not always update Ray to use the latest version. 
For example, suppose you define a remote function ``f`` and then redefine it; Ray should use the new definition:

.. testcode::

  import ray

  @ray.remote
  def f():
      return 1

  @ray.remote
  def f():
      return 2

  print(ray.get(f.remote()))  # This should print 2.

.. testoutput::

  2

However, there are cases where modifying a remote function doesn't take effect without restarting the cluster:

— **Imported function issue:** If ``f`` is defined in an external file (e.g., ``file.py``), and you modify its definition, re-importing the file may be ignored because Python treats the subsequent import as a no-op. A solution is to use ``from importlib import reload; reload(file)`` instead of a second import.

— **Helper function dependency:** If ``f`` depends on a helper function ``h`` defined in an external file, changes to ``h`` may not propagate. The easiest solution is to restart the Ray cluster. Alternatively, you can redefine ``f`` to reload ``file.py`` before invoking ``h``:

.. testcode::

  @ray.remote
  def f():
      from importlib import reload
      reload(file)
      return file.h()

This forces the external module to reload on the Workers. Note that in Python 3, you must use ``from importlib import reload``.


Capture task and actor call sites
---------------------------------

Ray captures and displays a stack trace when you invoke a task, create an actor, or call an actor method.

To enable call site capture, set the environment variable ``RAY_record_task_actor_creation_sites=true``. When enabled:

— Ray captures a stack trace when creating tasks, actors, or invoking actor methods.
— The captured stack trace is available in the Ray Dashboard (under task and actor details), output of the state CLI command ``ray list task --detail``, and state API responses.

Note that Ray turns off stack trace capture by default due to potential performance impacts. Enable it only when you need it for debugging.

Example:

.. NOTE(edoakes): test is skipped because it reinitializes Ray.
.. testcode::
    :skipif: True

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

This document outlines common problems encountered when using Ray along with potential solutions. If you encounter additional issues, please report them.

.. _`let us know`: https://github.com/ray-project/ray/issues
