.. _observability-debug-failures:

Debugging Failures
==================

What Kind of Failures Exist in Ray?
-----------------------------------

Ray consists of two major APIs. ``.remote()`` to create a Task or Actor, and :func:`ray.get <ray.get>` to get the result.
Debugging Ray means identifying and fixing failures from remote processes that run functions and classes (Tasks and Actors) created by the ``.remote`` API.

Ray APIs are future APIs (indeed, it is :ref:`possible to convert Ray object references to standard Python future APIs <async-ref-to-futures>`),
and the error handling model is the same. When any remote Tasks or Actors fail, the returned object ref contains an exception.
When you call ``get`` API to the object ref, it raises an exception.

.. testcode::

  import ray
  @ray.remote
  def f():
      raise ValueError("it's an application error")

  # Raises a ValueError.
  try:
    ray.get(f.remote())
  except ValueError as e:
    print(e)

.. testoutput::

  ...
  ValueError: it's an application error

In Ray, there are three types of failures. See exception APIs for more details.

- **Application failures**: This means the remote task/actor fails by the user code. In this case, ``get`` API will raise the :func:`RayTaskError <ray.exceptions.RayTaskError>` which includes the exception raised from the remote process.
- **Intentional system failures**: This means Ray is failed, but the failure is intended. For example, when you call cancellation APIs like ``ray.cancel`` (for task) or ``ray.kill`` (for actors), the system fails remote tasks and actors, but it is intentional.
- **Unintended system failures**: This means the remote tasks and actors failed due to unexpected system failures such as processes crashing (for example, by out-of-memory error) or nodes failing.

  1. `Linux Out of Memory killer <https://www.kernel.org/doc/gorman/html/understand/understand016.html>`_ or :ref:`Ray Memory Monitor <ray-oom-monitor>` kills processes with high memory usages to avoid out-of-memory.
  2. The machine shuts down (e.g., spot instance termination) or a :term:`raylet <raylet>` crashed (e.g., by an unexpected failure).
  3. System is highly overloaded or stressed (either machine or system components like Raylet or :term:`GCS <GCS / Global Control Service>`), which makes the system unstable and fail.

Debugging Application Failures
------------------------------

Ray distributes users' code to multiple processes across many machines. Application failures mean bugs in users' code.
Ray provides a debugging experience that's similar to debugging a single-process Python program.

print
~~~~~

``print`` debugging is one of the most common ways to debug Python programs.
:ref:`Ray's Task and Actor logs are printed to the Ray Driver <ray-worker-logs>` by default,
which allows you to simply use the ``print`` function to debug the application failures.

Debugger
~~~~~~~~

Many Python developers use a debugger to debug Python programs, and `Python pdb <https://docs.python.org/3/library/pdb.html>`_) is one of the popular choices.
Ray has native integration to ``pdb``. You can simply add ``breakpoint()`` to Actors and Tasks code to enable ``pdb``. View :ref:`Ray Debugger <ray-debugger>` for more details.


Running out of file descriptors (``Too may open files``)
--------------------------------------------------------

In a Ray cluster, arbitrary two system components can communicate with each other and make 1 or more connections.
For example, some workers may need to communicate with GCS to schedule Actors (worker <-> GCS connection).
Your Driver can invoke Actor methods (worker <-> worker connection).

Ray can support 1000s of raylets and 10000s of worker processes. When a Ray cluster gets larger,
each component can have an increasing number of network connections, which requires file descriptors.

Linux typically limits the default file descriptors per process to 1024. When there are
more than 1024 connections to the component, it can raise error messages below.

.. code-block:: bash

  Too may open files

It is especially common for the head node GCS process because it is a centralized
component that many other components in Ray communicate with. When you see this error message,
we recommend you adjust the max file descriptors limit per process via the ``ulimit`` command.

We recommend you apply ``ulimit -n 65536`` to your host configuration. However, you can also selectively apply it for
Ray components (view below example). Normally, each worker has 2~3 connections to GCS. Each raylet has 1~2 connections to GCS.
65536 file descriptors can handle 10000~15000 of workers and 1000~2000 of nodes.
If you have more workers, you should consider using a higher number than 65536.

.. code-block:: bash

  # Start head node components with higher ulimit.
  ulimit -n 65536 ray start --head

  # Start worker node components with higher ulimit.
  ulimit -n 65536 ray start --address <head_node>

  # Start a Ray driver with higher ulimit.
  ulimit -n 65536 <python script>

If that fails, double-check that the hard limit is sufficiently large by running ``ulimit -Hn``.
If it is too small, you can increase the hard limit as follows (these instructions work on EC2).

* Increase the hard ulimit for open file descriptors system-wide by running
  the following.

  .. code-block:: bash

    sudo bash -c "echo $USER hard nofile 65536 >> /etc/security/limits.conf"

* Logout and log back in.


Failures due to memory issues
--------------------------------
View :ref:`debugging memory issues <ray-core-mem-profiling>` for more details.


This document discusses some common problems that people run into when using Ray
as well as some known problems. If you encounter other problems, `let us know`_.

.. _`let us know`: https://github.com/ray-project/ray/issues