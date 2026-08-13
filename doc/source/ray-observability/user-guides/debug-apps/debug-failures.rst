.. meta::
   :description: Diagnose Ray application failures, including PyArrow HDFS JVM SIGSEGV crashes, file-descriptor exhaustion, and memory-related crashes.

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


Running out of file descriptors (``Too many open files``)
---------------------------------------------------------

In a Ray cluster, arbitrary two system components can communicate with each other and make 1 or more connections.
For example, some workers may need to communicate with GCS to schedule Actors (worker <-> GCS connection).
Your Driver can invoke Actor methods (worker <-> worker connection).

Ray can support 1000s of raylets and 10000s of worker processes. When a Ray cluster gets larger,
each component can have an increasing number of network connections, which requires file descriptors.

Linux typically limits the default file descriptors per process to 1024. When there are
more than 1024 connections to the component, it can raise error messages below.

.. code-block:: bash

  Too many open files

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


.. _troubleshoot-pyarrow-hdfs-jvm-crashes:

JVM crashes when using PyArrow with HDFS
-----------------------------------------

When Ray and PyArrow HDFS run in the same Python process on Linux, the process might
terminate with ``SIGSEGV`` or ``SIGABRT`` and create an ``hs_err_pid*.log`` file.
The crash can occur after :func:`ray.init`, even when the same
``pyarrow.fs.HadoopFileSystem`` operation succeeds before Ray initializes. A newer JDK
might make the failure less frequent, but upgrading the JDK alone doesn't resolve the
underlying signal-handler conflict.

PyArrow HDFS loads ``libhdfs``, which creates a HotSpot JVM inside the Python process.
A ``SIGSEGV`` in a JVM process doesn't always represent a fatal memory error. HotSpot
deliberately uses hardware faults and operating-system signals for VM operations such
as implicit null checks. Its signal handler inspects the signal context and either
handles an expected, recoverable JVM fault or starts crash reporting for a genuine
fatal error. These recoverable signals are normally invisible to the application.

Ray's CoreWorker also installs an Abseil failure-signal handler for signals including
``SIGSEGV``. Abseil doesn't understand HotSpot's JIT-generated code or VM-specific
signal contexts, so it can't determine whether a particular signal is recoverable by
the JVM. Installing both handlers doesn't necessarily cause an immediate crash. The
conflict is established when the handlers are installed, but the crash occurs later
when HotSpot produces a recoverable internal fault and no longer gets the first
opportunity to classify and handle it. JVM execution paths, JIT compilation, thread
scheduling, and memory layout can therefore make the failure appear intermittent.

Use HotSpot signal chaining
~~~~~~~~~~~~~~~~~~~~~~~~~~~

On Linux with a HotSpot or OpenJDK distribution that includes ``libjsig.so``, use
HotSpot's signal-chaining mechanism as the preferred mitigation. Locate the library in
the same JDK selected by ``JAVA_HOME``, and preload it before starting Python:

.. code-block:: bash

    LIBJSIG="$(find "$JAVA_HOME" -type f -name libjsig.so -print -quit)"
    test -n "$LIBJSIG" || {
      echo "libjsig.so not found under JAVA_HOME=$JAVA_HOME" >&2
      exit 1
    }

    export LD_PRELOAD="$LIBJSIG${LD_PRELOAD:+:$LD_PRELOAD}"
    python your_program.py

``libjsig.so`` intercepts subsequent ``signal()``, ``sigset()``, and ``sigaction()``
calls and chains newly installed handlers behind the HotSpot handler. The ordering is
important because only HotSpot can classify a JVM-generated ``SIGSEGV`` as recoverable.
HotSpot consumes its internal faults, while signals it doesn't recognize continue to
Ray's handler. This approach keeps Ray's native crash diagnostics enabled.

You must configure ``LD_PRELOAD`` before the Python process starts. Setting it through
``os.environ`` in a running Python process is too late. The library path varies by JDK;
common locations include ``$JAVA_HOME/lib/libjsig.so`` and
``$JAVA_HOME/lib/server/libjsig.so``. Other JVM implementations and minimized runtime
images might not include it.

In a KubeRay deployment, use an image that contains ``libjsig.so`` at a stable path and
set ``LD_PRELOAD`` on every head or worker container that can access HDFS. For example:

.. code-block:: yaml

    spec:
      headGroupSpec:
        template:
          spec:
            containers:
              - name: ray-head
                env:
                  - name: LD_PRELOAD
                    value: /usr/local/lib/libjsig.so
      workerGroupSpecs:
        - groupName: workers
          template:
            spec:
              containers:
                - name: ray-worker
                  env:
                    - name: LD_PRELOAD
                      value: /usr/local/lib/libjsig.so

Verify the path when building the image instead of configuring a nonexistent preload
library. Preserve any other libraries that your environment already lists in
``LD_PRELOAD``.

Disable Ray's failure-signal handler
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

If ``libjsig.so`` isn't available or the environment doesn't permit ``LD_PRELOAD``,
disable Ray's failure-signal handler before importing Ray:

.. code-block:: bash

    RAY_DISABLE_FAILURE_SIGNAL_HANDLER=1 python your_program.py

This removes one side of the conflict, but it has a diagnostic cost. If the CoreWorker
later experiences a genuine ``SIGSEGV``, ``SIGABRT``, or similar native failure, Ray
might no longer print the C++ failure stack normally produced by Abseil. Setting
``PYTHONFAULTHANDLER=1`` can provide Python-level diagnostics, but it doesn't restore
the disabled C++ failure stack.

Don't combine this fallback with ``libjsig.so``. When Ray's handler is disabled, there
is little value in chaining it. For an A/B stress reproducer and test results, see
`GitHub issue #36415 <https://github.com/ray-project/ray/issues/36415>`_.


Failures due to memory issues
--------------------------------
View :ref:`debugging memory issues <ray-core-mem-profiling>` for more details.


This document discusses some common problems that people run into when using Ray
as well as some known problems. If you encounter other problems, `let us know`_.

.. _`let us know`: https://github.com/ray-project/ray/issues
