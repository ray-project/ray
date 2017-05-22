Troubleshooting
===============

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
  prevent your application from achieving a speedup. You can diagnose this by
  opening ``top`` while your application is running. If one process is using
  most of the CPUs, and the others are using a small amount, this may be the
  problem. This is very common with some versions of ``numpy``, and in that case
  can usually be setting an environment variable like ``MKL_NUM_THREADS`` (or
  the equivalent depending on your installation) to ``1``.

Crashes
-------

If Ray crashed, you may wonder what happened. Currently, this can occur for some
of the following reasons.

- **Stressful workloads:** Workloads that create many many tasks in a short
  amount of time can sometimes interfere with the heartbeat mechanism that we
  use to check that processes are still alive. On the head node in the cluster,
  you can check the files ``/tmp/raylogs/monitor-******.out`` and
  ``/tmp/raylogs/monitor-******.err``. They will indicate which processes Ray
  has marked as dead (due to a lack of heartbeats). However, it is currently
  possible for a process to get marked as dead without actually having died.

- **Starting many actors:** Workloads that start a large number of actors all at
  once may exhibit problems when the processes (or libraries that they use)
  contend for resources. Similarly, a script that starts many actors over the
  lifetime of the application will eventually cause the system to run out of
  file descriptors.

Hanging
-------

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

If you encounter objects where Ray's serialization is currently imperfect. If
you encounter an object that Ray does not serialize/deserialize correctly,
please let us know. For example, you may want to bring it up on `this thread`_.

- `Objects with multiple references to the same object`_.

- `Subtypes of lists, dictionaries, or tuples`_.

.. _`this thread`: https://github.com/ray-project/ray/issues/557
.. _`Objects with multiple references to the same object`: https://github.com/ray-project/ray/issues/319
.. _`Subtypes of lists, dictionaries, or tuples`: https://github.com/ray-project/ray/issues/512
