.. _gotchas:

Ray Gotchas
===========

Ray sometimes has some aspects of its behavior that might catch
users off guard. There may be sound arguments for these design choices.

In particular, users think of Ray as running on their local machine, and
while this is mostly true, this doesn't work.

Environment variables are not passed from the driver to workers
---------------------------------------------------------------

**Issue**: If you set an environment variable at the command line, it is not passed to all the workers running in the cluster
if the cluster was started previously.

**Example**: If you have a file ``baz.py`` in the directory you are running Ray in, and you run the following command:

.. literalinclude:: ../../ray-core/doc_code/gotchas.py
  :language: python
  :start-after: __env_var_start__
  :end-before: __env_var_end__

**Expected behavior**: Most people would expect (as if it was a single process on a single machine) that the environment variables would be the same in all workers. It won’t be.

**Fix**: Use runtime environments to pass environment variables explicity.
If you call ``ray.init(runtime_env=...)``,
then the workers will have the environment variable set.


.. literalinclude:: ../../ray-core/doc_code/gotchas.py
  :language: python
  :start-after: __env_var_fix_start__
  :end-before: __env_var_fix_end__


Filenames work sometimes and not at other times
-----------------------------------------------

**Issue**: If you reference a file by name in a task or actor,
it will sometimes work and sometimes fail. This is
because if the task or actor runs on the head node
of the cluster, it will work, but if the task or actor
runs on another machine it won't.

**Example**: Let's say we do the following command:

.. code-block:: bash

	% touch /tmp/foo.txt

And I have this code:

.. code-block:: python

  import os

  ray.init()
  @ray.remote
  def check_file():
    foo_exists = os.path.exists("/tmp/foo.txt")
    print(f"Foo exists? {foo_exists}")

  futures = []
  for _ in range(1000):
    futures.append(check_file.remote())

  ray.get(futures)


then you will get a mix of True and False. If
``check_file()`` runs on the head node, or we're running
locally it works. But if it runs on a worker node, it returns ``False``.

**Expected behavior**: Most people would expect this to either fail or succeed consistently.
It's the same code after all.

**Fix**

- Use only shared paths for such applications -- e.g. if you are using a network file system you can use that, or the files can be on s3.
- Do not rely on file path consistency.



Placement groups are not composable
-----------------------------------

**Issue**: If you have a task that is called from something that runs in a placement
group, the resources are never allocated and it hangs.

**Example**: You are using Ray Tune which creates placement groups, and you want to
apply it to an objective function, but that objective function makes use
of Ray Tasks itself, e.g.

.. code-block:: python

  from ray import air, tune

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
ValueError: Cannot schedule create_task_that_uses_resources.<locals>.sample_task with the placement group
because the resource request {'CPU': 10} cannot fit into any bundles for the placement group, [{'CPU': 1.0}].

**Expected behavior**: The above executes.

**Fix**: In the ``@ray.remote`` declaration of tasks
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

.. code-block:: python

  @ray.remote
  def f():
      return 1

  @ray.remote
  def f():
      return 2

  ray.get(f.remote())  # This should be 2.

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
  all of the workers, and is unpickled. During unpickling, ``file.py`` gets
  imported in the workers. Then when ``f`` gets redefined, its definition is
  again shipped and unpickled in all of the workers. But since ``file.py``
  has been imported in the workers already, it is treated as a second import
  and is ignored as a no-op.

  Unfortunately, reloading on the driver does not update ``h``, as the reload
  needs to happen on the worker.

  A solution to this problem is to redefine ``f`` to reload ``file.py`` before
  it calls ``h``. For example, if inside ``file.py`` you have

  .. code-block:: python

    def h():
        return 1

  And you define remote function ``f`` as

  .. code-block:: python

    @ray.remote
    def f():
        return file.h()

  You can redefine ``f`` as follows.

  .. code-block:: python

    @ray.remote
    def f():
        reload(file)
        return file.h()

  This forces the reload to happen on the workers as needed. Note that in
  Python 3, you need to do ``from importlib import reload``.

This document discusses some common problems that people run into when using Ray
as well as some known problems. If you encounter other problems, please
`let us know`_.

.. _`let us know`: https://github.com/ray-project/ray/issues
