Troubleshooting and FAQs
========================

This document discusses some common problems that people run into when using Ray
as well as some known problems. If you encounter other problems, please
`let us know`_.

.. _`let us know`: https://github.com/ray-project/ray/issues

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


Hanging
-------

.. tip::

    You can run ``ray stack`` to dump the stack traces of all Ray workers on
    the current node. This requires py-spy to be installed.

If a workload is hanging and not progressing, the problem may be one of the
following.

- **Reconstructing an object created with put:** When an object that is needed
  has been evicted or lost, Ray will attempt to rerun the task that created the
  object. However, there are some cases that currently are not handled. For
  example, if the object was created by a call to ``ray.put`` on the driver
  process, then the argument that was passed into ``ray.put`` is no longer
  available and so the call to ``ray.put`` cannot be rerun (without rerunning
  the driver).

- **Reconstructing an object created by actor task:** Ray currently does not
  reconstruct objects created by actor methods.

Serialization Problems
----------------------

Ray's serialization is currently imperfect. If you encounter an object that
Ray does not serialize/deserialize correctly, please let us know. For example,
you may want to bring it up on `this thread`_.

- `Objects with multiple references to the same object`_.

- `Subtypes of lists, dictionaries, or tuples`_.

.. _`this thread`: https://github.com/ray-project/ray/issues/557
.. _`Objects with multiple references to the same object`: https://github.com/ray-project/ray/issues/319
.. _`Subtypes of lists, dictionaries, or tuples`: https://github.com/ray-project/ray/issues/512

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
