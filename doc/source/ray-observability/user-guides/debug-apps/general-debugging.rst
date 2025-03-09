.. _observability-general-debugging:

General Debugging
=======================

Distributed applications are more powerful yet complicated than non-distributed ones. Some of Ray's behavior might catch
users off guard while there may be sound arguments for these design choices.

This page lists some common issues users may run into. In particular, users think of Ray as running on their local machine, and
while this is sometimes true, this leads to a lot of issues.

Environment variables are not passed from the Driver process to Worker processes
---------------------------------------------------------------------------------

**Issue**: If you set an environment variable at the command line (where you run your Driver), it is not passed to all the Workers running in the Cluster
if the Cluster was started previously.

**Example**: If you have a file ``baz.py`` in the directory you are running Ray in, and you run the following command:

.. literalinclude:: /ray-observability/doc_code/gotchas.py
  :language: python
  :start-after: __env_var_start__
  :end-before: __env_var_end__

**Expected behavior**: Most people would expect (as if it was a single process on a single machine) that the environment variables would be the same in all Workers. It won’t be.

**Fix**: Use Runtime Environments to pass environment variables explicitly.
If you call ``ray.init(runtime_env=...)``,
then the Workers will have the environment variable set.


.. literalinclude:: /ray-observability/doc_code/gotchas.py
  :language: python
  :start-after: __env_var_fix_start__
  :end-before: __env_var_fix_end__


Filenames work sometimes and not at other times
-----------------------------------------------

**Issue**: If you reference a file by name in a Task or Actor,
it will sometimes work and sometimes fail. This is
because if the Task or Actor runs on the Head Node
of the Cluster, it will work, but if the Task or A8ctor
runs on another machine it won't.

**Example**: Let's say we do the following command:

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


then you will get a mix of True and False. If
``check_file()`` runs on the Head Node, or we're running
locally it works. But if it runs on a Worker Node, it returns ``False``.

**Expected behavior**: Most people would expect this to either fail or succeed consistently.
It's the same code after all.

**Fix**

- Use only shared paths for such applications -- e.g. if you are using a network file system you can use that, or the files can be on S3.
- Do not rely on file path consistency.



Placement Groups are not composable
-----------------------------------

**Issue**: If you have a task that is called from something that runs in a Placement
Group, the resources are never allocated and it hangs.

**Example**: You are using Ray Tune which creates Placement Groups, and you want to
apply it to an objective function, but that objective function makes use
of Ray Tasks itself, e.g.

.. testcode::

  import ray
  from ray import tune

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

This will error with message:

.. testoutput::
  :options: +MOCK

    ValueError: Cannot schedule create_task_that_uses_resources.<locals>.sample_task with the placement group
    because the resource request {'CPU': 10} cannot fit into any bundles for the placement group, [{'CPU': 1.0}].

**Expected behavior**: The above executes.

**Fix**: In the ``@ray.remote`` declaration of Tasks
called by ``create_task_that_uses_resources()`` , include a
``scheduling_strategy=PlacementGroupSchedulingStrategy(placement_group=None)``.

.. code-block:: diff

  def create_task_that_uses_resources():
  +     @ray.remote(num_cpus=10, scheduling_strategy=PlacementGroupSchedulingStrategy(placement_group=None))
  -     @ray.remote(num_cpus=10)

Outdated Function Definitions
-----------------------------

Due to subtleties of Python, if you redefine a remote function, you may not
always get the expected behavior. In this case, it may be that Ray is not
running the newest version of the function.

Suppose you define a remote function ``f`` and then redefine it. Ray should use
the newest version.

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

However, the following are cases where modifying the remote function will
not update Ray to the new version (at least without stopping and restarting
Ray).

- **The function is imported from an external file:** In this case,
  ``f`` is defined in some external file ``file.py``. If you ``import file``,
  change the definition of ``f`` in ``file.py``, then re-``import file``,
  the function ``f`` will not be updated.

  This is because the second import gets ignored as a no-op, so ``f`` is
  still defined by the first import.

  A solution to this problem is to use ``reload(file)`` instead of a second
  ``import file``. Reloading causes the new definition of ``f`` to be
  re-executed, and exports it to the other machines. Note that in Python 3, you
  need to do ``from importlib import reload``.

- **The function relies on a helper function from an external file:**
  In this case, ``f`` can be defined within your Ray application, but relies
  on a helper function ``h`` defined in some external file ``file.py``. If the
  definition of ``h`` gets changed in ``file.py``, redefining ``f`` will not
  update Ray to use the new version of ``h``.

  This is because when ``f`` first gets defined, its definition is shipped to
  all of the Worker processes, and is unpickled. During unpickling, ``file.py`` gets
  imported in the Workers. Then when ``f`` gets redefined, its definition is
  again shipped and unpickled in all of the Workers. But since ``file.py``
  has been imported in the Workers already, it is treated as a second import
  and is ignored as a no-op.

  Unfortunately, reloading on the Driver does not update ``h``, as the reload
  needs to happen on the worker.

  A solution to this problem is to redefine ``f`` to reload ``file.py`` before
  it calls ``h``. For example, if inside ``file.py`` you have

  .. testcode::

    def h():
        return 1

  And you define remote function ``f`` as

  .. testcode::

    @ray.remote
    def f():
        return file.h()

  You can redefine ``f`` as follows.

  .. testcode::

    @ray.remote
    def f():
        reload(file)
        return file.h()

  This forces the reload to happen on the Workers as needed. Note that in
  Python 3, you need to do ``from importlib import reload``.

This document discusses some common problems that people run into when using Ray
as well as some known problems. If you encounter other problems, `let us know`_.

.. _`let us know`: https://github.com/ray-project/ray/issues

Capture task and actor call sites
---------------------------------

Ray can optionally capture and display the stacktrace of where your code invokes tasks, creates actors or invokes actor tasks. This feature can help with debugging and understanding the execution flow of your application.

To enable call site capture, set the environment variable ``RAY_record_task_actor_creation_sites=true``. When enabled:

- Ray captures the stacktrace when creating tasks, actors or calling actor methods
- The call site stacktrace is visible in:
  - Ray Dashboard UI under the task details and actor details pages
  - ``ray list task --detail`` CLI command output
  - State API responses

Note that stacktrace capture is disabled by default to avoid any performance overhead. Only enable it when needed for debugging purposes.

Example:

.. testcode::

    import ray

    # Enable stacktrace capture
    ray.init(runtime_env={"env_vars": {"RAY_record_task_actor_creation_sites": "true"}})

    @ray.remote
    def my_task():
        return 42

    # Capture the stacktrace upon task invocation.
    future = my_task.remote()
    result = ray.get(future)

    @ray.remote
    class Counter:
        def __init__(self):
            self.value = 0

        def increment(self):
            self.value += 1
            return self.value

    # Capture stacktrace upon actor creation.
    counter = Counter.remote()

    # Capture stacktrace upon method invocation.
    counter.increment.remote()


The stacktrace shows the exact line numbers and call stack where the task was invoked, actor was created and methods were invoked.

This feature is currently only supported for Python and C++ tasks.
