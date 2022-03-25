Debugging and Profiling
=======================

Observing Ray Work
------------------

You can run ``ray stack`` to dump the stack traces of all Ray workers on
the current node. This requires ``py-spy`` to be installed. See the `Troubleshooting page <troubleshooting.html>`_ for more details.

Visualizing Tasks in the Ray Timeline
-------------------------------------

The most important tool is the timeline visualization tool. To visualize tasks
in the Ray timeline, you can dump the timeline as a JSON file by running ``ray
timeline`` from the command line or by using the following command.

To use the timeline, Ray profiling must be enabled by setting the
``RAY_PROFILING=1`` environment variable prior to starting Ray on every machine.

.. code-block:: python

  ray.timeline(filename="/tmp/timeline.json")

Then open `chrome://tracing`_ in the Chrome web browser, and load
``timeline.json``.

.. _`chrome://tracing`: chrome://tracing


Profiling Using Python's CProfile
---------------------------------

A second way to profile the performance of your Ray application is to
use Python's native cProfile `profiling module`_. Rather than tracking
line-by-line of your application code, cProfile can give the total runtime
of each loop function, as well as list the number of calls made and
execution time of all function calls made within the profiled code.

.. _`profiling module`: https://docs.python.org/3/library/profile.html#module-cProfile

Unlike ``line_profiler`` above, this detailed list of profiled function calls
**includes** internal function calls and function calls made within Ray!

However, similar to ``line_profiler``, cProfile can be enabled with minimal
changes to your application code (given that each section of the code you want
to profile is defined as its own function). To use cProfile, add an import
statement, then replace calls to the loop functions as follows:

.. code-block:: python

  import cProfile  # Added import statement

  def ex1():
      list1 = []
      for i in range(5):
          list1.append(ray.get(func.remote()))

  def main():
      ray.init()
      cProfile.run('ex1()')  # Modified call to ex1
      cProfile.run('ex2()')
      cProfile.run('ex3()')

  if __name__ == "__main__":
      main()

Now, when executing your Python script, a cProfile list of profiled function
calls will be outputted to terminal for each call made to ``cProfile.run()``.
At the very top of cProfile's output gives the total execution time for
``'ex1()'``:

.. code-block:: bash

  601 function calls (595 primitive calls) in 2.509 seconds

Following is a snippet of profiled function calls for ``'ex1()'``. Most of
these calls are quick and take around 0.000 seconds, so the functions of
interest are the ones with non-zero execution times:

.. code-block:: bash

  ncalls  tottime  percall  cumtime  percall filename:lineno(function)
  ...
      1    0.000    0.000    2.509    2.509 your_script_here.py:31(ex1)
      5    0.000    0.000    0.001    0.000 remote_function.py:103(remote)
      5    0.000    0.000    0.001    0.000 remote_function.py:107(_remote)
  ...
     10    0.000    0.000    0.000    0.000 worker.py:2459(__init__)
      5    0.000    0.000    2.508    0.502 worker.py:2535(get)
      5    0.000    0.000    0.000    0.000 worker.py:2695(get_global_worker)
     10    0.000    0.000    2.507    0.251 worker.py:374(retrieve_and_deserialize)
      5    0.000    0.000    2.508    0.502 worker.py:424(get_object)
      5    0.000    0.000    0.000    0.000 worker.py:514(submit_task)
  ...

The 5 separate calls to Ray's ``get``, taking the full 0.502 seconds each call,
can be noticed at ``worker.py:2535(get)``. Meanwhile, the act of calling the
remote function itself at ``remote_function.py:103(remote)`` only takes 0.001
seconds over 5 calls, and thus is not the source of the slow performance of
``ex1()``.


Profiling Ray Actors with cProfile
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Considering that the detailed output of cProfile can be quite different depending
on what Ray functionalities we use, let us see what cProfile's output might look
like if our example involved Actors (for an introduction to Ray actors, see our
`Actor documentation here`_).

.. _`Actor documentation here`: http://docs.ray.io/en/master/actors.html

Now, instead of looping over five calls to a remote function like in ``ex1``,
let's create a new example and loop over five calls to a remote function
**inside an actor**. Our actor's remote function again just sleeps for 0.5
seconds:

.. code-block:: python

  # Our actor
  @ray.remote
  class Sleeper(object):
      def __init__(self):
          self.sleepValue = 0.5

      # Equivalent to func(), but defined within an actor
      def actor_func(self):
          time.sleep(self.sleepValue)

Recalling the suboptimality of ``ex1``, let's first see what happens if we
attempt to perform all five ``actor_func()`` calls within a single actor:

.. code-block:: python

  def ex4():
      # This is suboptimal in Ray, and should only be used for the sake of this example
      actor_example = Sleeper.remote()

      five_results = []
      for i in range(5):
          five_results.append(actor_example.actor_func.remote())

      # Wait until the end to call ray.get()
      ray.get(five_results)

We enable cProfile on this example as follows:

.. code-block:: python

  def main():
      ray.init()
      cProfile.run('ex4()')

  if __name__ == "__main__":
      main()

Running our new Actor example, cProfile's abbreviated output is as follows:

.. code-block:: bash

  12519 function calls (11956 primitive calls) in 2.525 seconds

  ncalls  tottime  percall  cumtime  percall filename:lineno(function)
  ...
  1    0.000    0.000    0.015    0.015 actor.py:546(remote)
  1    0.000    0.000    0.015    0.015 actor.py:560(_remote)
  1    0.000    0.000    0.000    0.000 actor.py:697(__init__)
  ...
  1    0.000    0.000    2.525    2.525 your_script_here.py:63(ex4)
  ...
  9    0.000    0.000    0.000    0.000 worker.py:2459(__init__)
  1    0.000    0.000    2.509    2.509 worker.py:2535(get)
  9    0.000    0.000    0.000    0.000 worker.py:2695(get_global_worker)
  4    0.000    0.000    2.508    0.627 worker.py:374(retrieve_and_deserialize)
  1    0.000    0.000    2.509    2.509 worker.py:424(get_object)
  8    0.000    0.000    0.001    0.000 worker.py:514(submit_task)
  ...

It turns out that the entire example still took 2.5 seconds to execute, or the
time for five calls to ``actor_func()`` to run in serial. We remember in ``ex1``
that this behavior was because we did not wait until after submitting all five
remote function tasks to call ``ray.get()``, but we can verify on cProfile's
output line ``worker.py:2535(get)`` that ``ray.get()`` was only called once at
the end, for 2.509 seconds. What happened?

It turns out Ray cannot parallelize this example, because we have only
initialized a single ``Sleeper`` actor. Because each actor is a single,
stateful worker, our entire code is submitted and ran on a single worker the
whole time.

To better parallelize the actors in ``ex4``, we can take advantage
that each call to ``actor_func()`` is independent, and instead
create five ``Sleeper`` actors. That way, we are creating five workers
that can run in parallel, instead of creating a single worker that
can only handle one call to ``actor_func()`` at a time.

.. code-block:: python

  def ex4():
      # Modified to create five separate Sleepers
      five_actors = [Sleeper.remote() for i in range(5)]

      # Each call to actor_func now goes to a different Sleeper
      five_results = []
      for actor_example in five_actors:
          five_results.append(actor_example.actor_func.remote())

      ray.get(five_results)

Our example in total now takes only 1.5 seconds to run:

.. code-block:: bash

  1378 function calls (1363 primitive calls) in 1.567 seconds

  ncalls  tottime  percall  cumtime  percall filename:lineno(function)
  ...
  5    0.000    0.000    0.002    0.000 actor.py:546(remote)
  5    0.000    0.000    0.002    0.000 actor.py:560(_remote)
  5    0.000    0.000    0.000    0.000 actor.py:697(__init__)
  ...
  1    0.000    0.000    1.566    1.566 your_script_here.py:71(ex4)
  ...
  21    0.000    0.000    0.000    0.000 worker.py:2459(__init__)
  1    0.000    0.000    1.564    1.564 worker.py:2535(get)
  25    0.000    0.000    0.000    0.000 worker.py:2695(get_global_worker)
  3    0.000    0.000    1.564    0.521 worker.py:374(retrieve_and_deserialize)
  1    0.000    0.000    1.564    1.564 worker.py:424(get_object)
  20    0.001    0.000    0.001    0.000 worker.py:514(submit_task)
  ...

This document discusses some common problems that people run into when using Ray
as well as some known problems. If you encounter other problems, please
`let us know`_.

.. _`let us know`: https://github.com/ray-project/ray/issues

Understanding `ObjectLostErrors`
--------------------------------
Ray throws an ``ObjectLostError`` to the application when an object cannot be
retrieved due to application or system error. This can occur during a
``ray.get()`` call or when fetching a task's arguments, and can happen for a
number of reasons. Here is a guide to understanding the root cause for
different error types:

- ``ObjectLostError``: The object was successfully created, but then all copies
  were lost due to node failure.
- ``OwnerDiedError``: The owner of an object, i.e., the Python worker that
  first created the ``ObjectRef`` via ``.remote()`` or ``ray.put()``, has died.
  The owner stores critical object metadata and an object cannot be retrieved
  if this process is lost.
- ``ObjectReconstructionFailedError``: Should only be thrown when `lineage
  reconstruction`_ is enabled. This error is thrown if an object, or another
  object that this object depends on, cannot be reconstructed because the
  maximum number of task retries has been exceeded. By default, a non-actor
  task can be retried up to 3 times and an actor task cannot be retried.
  This can be overridden with the ``max_retries`` parameter for remote
  functions and the ``max_task_retries`` parameter for actors.
- ``ReferenceCountingAssertionError``: The object has already been deleted,
  so it cannot be retrieved. Ray implements automatic memory management through
  distributed reference counting, so this error should not happen in general.
  However, there is a `known edge case`_ that can produce this error.

.. _`lineage reconstruction`: https://docs.ray.io/en/master/ray-core/actors/fault-tolerance.html
.. _`known edge case`: https://github.com/ray-project/ray/issues/18456

Crashes
-------

If Ray crashed, you may wonder what happened. Currently, this can occur for some
of the following reasons.

- **Stressful workloads:** Workloads that create many many tasks in a short
  amount of time can sometimes interfere with the heartbeat mechanism that we
  use to check that processes are still alive. On the head node in the cluster,
  you can check the files ``/tmp/ray/session_*/logs/monitor*``. They will
  indicate which processes Ray has marked as dead (due to a lack of heartbeats).
  However, it is currently possible for a process to get marked as dead without
  actually having died.

- **Starting many actors:** Workloads that start a large number of actors all at
  once may exhibit problems when the processes (or libraries that they use)
  contend for resources. Similarly, a script that starts many actors over the
  lifetime of the application will eventually cause the system to run out of
  file descriptors. This is addressable, but currently we do not garbage collect
  actor processes until the script finishes.

- **Running out of file descriptors:** As a workaround, you may be able to
  increase the maximum number of file descriptors with a command like
  ``ulimit -n 65536``. If that fails, double check that the hard limit is
  sufficiently large by running ``ulimit -Hn``. If it is too small, you can
  increase the hard limit as follows (these instructions work on EC2).

    * Increase the hard ulimit for open file descriptors system-wide by running
      the following.

      .. code-block:: bash

        sudo bash -c "echo $USER hard nofile 65536 >> /etc/security/limits.conf"

    * Logout and log back in.

No Speedup
----------

You just ran an application using Ray, but it wasn't as fast as you expected it
to be. Or worse, perhaps it was slower than the serial version of the
application! The most common reasons are the following.

- **Number of cores:** How many cores is Ray using? When you start Ray, it will
  determine the number of CPUs on each machine with ``psutil.cpu_count()``. Ray
  usually will not schedule more tasks in parallel than the number of CPUs. So
  if the number of CPUs is 4, the most you should expect is a 4x speedup.

- **Physical versus logical CPUs:** Do the machines you're running on have fewer
  **physical** cores than **logical** cores? You can check the number of logical
  cores with ``psutil.cpu_count()`` and the number of physical cores with
  ``psutil.cpu_count(logical=False)``. This is common on a lot of machines and
  especially on EC2. For many workloads (especially numerical workloads), you
  often cannot expect a greater speedup than the number of physical CPUs.

- **Small tasks:** Are your tasks very small? Ray introduces some overhead for
  each task (the amount of overhead depends on the arguments that are passed
  in). You will be unlikely to see speedups if your tasks take less than ten
  milliseconds. For many workloads, you can easily increase the sizes of your
  tasks by batching them together.

- **Variable durations:** Do your tasks have variable duration? If you run 10
  tasks with variable duration in parallel, you shouldn't expect an N-fold
  speedup (because you'll end up waiting for the slowest task). In this case,
  consider using ``ray.wait`` to begin processing tasks that finish first.

- **Multi-threaded libraries:** Are all of your tasks attempting to use all of
  the cores on the machine? If so, they are likely to experience contention and
  prevent your application from achieving a speedup. This is very common with
  some versions of ``numpy``, and in that case can usually be setting an
  environment variable like ``MKL_NUM_THREADS`` (or the equivalent depending
  on your installation) to ``1``.

  For many - but not all - libraries, you can diagnose this by opening ``top``
  while your application is running. If one process is using most of the CPUs,
  and the others are using a small amount, this may be the problem. The most
  common exception is PyTorch, which will appear to be using all the cores
  despite needing ``torch.set_num_threads(1)`` to be called to avoid contention.

If you are still experiencing a slowdown, but none of the above problems apply,
we'd really like to know! Please create a `GitHub issue`_ and consider
submitting a minimal code example that demonstrates the problem.

.. _`Github issue`: https://github.com/ray-project/ray/issues

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
