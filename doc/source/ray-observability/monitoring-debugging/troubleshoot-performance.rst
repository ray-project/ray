Troubleshooting Performance
===========================

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
  prevent your application from achieving a speedup.
  This is common with some versions of ``numpy``. To avoid contention, set an
  environment variable like ``MKL_NUM_THREADS`` (or the equivalent depending on
  your installation) to ``1``.

  For many - but not all - libraries, you can diagnose this by opening ``top``
  while your application is running. If one process is using most of the CPUs,
  and the others are using a small amount, this may be the problem. The most
  common exception is PyTorch, which will appear to be using all the cores
  despite needing ``torch.set_num_threads(1)`` to be called to avoid contention.

If you are still experiencing a slowdown, but none of the above problems apply,
we'd really like to know! Please create a `GitHub issue`_ and consider
submitting a minimal code example that demonstrates the problem.

.. _`Github issue`: https://github.com/ray-project/ray/issues

This document discusses some common problems that people run into when using Ray
as well as some known problems. If you encounter other problems, please
`let us know`_.

.. _`let us know`: https://github.com/ray-project/ray/issues
