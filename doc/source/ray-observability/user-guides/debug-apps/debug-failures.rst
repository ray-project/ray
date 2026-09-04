.. meta::
   :description: Diagnose Ray application failures: the kinds of failures that occur, print and debugger workflows, file-descriptor exhaustion, and memory-related crashes.

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
might make the failure less frequent, but upgrading alone doesn't guarantee that you
avoid the underlying signal-handler conflict.

PyArrow HDFS loads ``libhdfs``, which creates a HotSpot JVM inside the Python process.
A ``SIGSEGV`` in a JVM process doesn't always represent a fatal memory error. HotSpot
deliberately uses hardware faults and operating-system signals for VM operations such
as implicit null checks. Its signal handler inspects the signal context and either
handles an expected, recoverable JVM fault or starts crash reporting for a genuine
fatal error. These recoverable signals are normally invisible to the application.

Ray's CoreWorker also installs an Abseil failure-signal handler for signals including
``SIGSEGV``. Linux maintains one current signal disposition for each signal; it doesn't
automatically invoke multiple handlers in registration order. Unless the libraries
explicitly implement chaining, a later registration can replace an earlier handler.
Abseil doesn't understand HotSpot's JIT-generated code or VM-specific signal contexts,
so it can't determine whether a particular signal is recoverable by the JVM.

Installing both handlers doesn't necessarily cause an immediate crash. Installation
establishes the conflict, but the crash comes later, when HotSpot produces a
recoverable internal fault and no longer gets the first opportunity to classify and
handle it. JVM execution paths, JIT compilation, thread scheduling, and memory layout
can therefore make the failure appear intermittent.

Use HotSpot signal chaining
~~~~~~~~~~~~~~~~~~~~~~~~~~~

On Linux with a HotSpot or OpenJDK distribution that includes ``libjsig.so``, use
HotSpot's signal chaining. Locate the library in the same JDK that ``JAVA_HOME``
selects, then preload it before you start Python:

.. code-block:: bash

    LIBJSIG=""
    if [ -n "${JAVA_HOME:-}" ]; then
      LIBJSIG="$(find -L "$JAVA_HOME" -type f -name libjsig.so 2>/dev/null | sed -n '1p')"
    fi
    test -n "$LIBJSIG" || {
      echo "libjsig.so not found under JAVA_HOME=${JAVA_HOME:-<unset>}" >&2
      exit 1
    }

    export LD_PRELOAD="$LIBJSIG${LD_PRELOAD:+:$LD_PRELOAD}"
    python your_program.py

``LD_PRELOAD`` makes the dynamic loader load ``libjsig.so`` before the other native
libraries; the environment variable itself doesn't change signal semantics.
``libjsig.so`` then intercepts subsequent ``signal()``, ``sigset()``, and
``sigaction()`` calls and turns what would otherwise be handler replacement into an
explicit chain behind the HotSpot handler. The ordering is important because only
HotSpot can classify a JVM-generated ``SIGSEGV`` as recoverable. HotSpot consumes and
recovers from its internal faults without forwarding them. Signals it doesn't
recognize continue to Ray's handler. This mechanism lets Ray's failure-signal handler
remain enabled.

.. note::

    JDK 16 and later might warn that using ``signal()`` and ``sigset()`` for signal
    chaining is deprecated. This warning applies to those two registration functions,
    not to ``libjsig.so`` or ``LD_PRELOAD``. Ray's Abseil failure-signal handler uses
    the supported ``sigaction()`` function on Linux. The warning alone therefore
    doesn't indicate that ``sigaction()`` chaining failed. For details, see the
    `JDK 21 signal-chaining documentation
    <https://docs.oracle.com/en/java/javase/21/vm/signal-chaining.html>`_.

You must configure ``LD_PRELOAD`` before the Python process starts. Setting it through
``os.environ`` in a running Python process is too late. The library path varies by JDK.
Common locations include ``$JAVA_HOME/lib/libjsig.so`` and
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

Verify the path in the image you build. The path above is an example, not a standard
location. If the library is missing, signal chaining isn't enabled and the dynamic
loader might report an error. Preserve any other libraries that your environment
already lists in ``LD_PRELOAD``.

Last resort: disable Ray's failure-signal handler
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

If ``libjsig.so`` isn't available, the environment doesn't permit ``LD_PRELOAD``, and
the conflict persists, you can disable Ray's failure-signal handler as a last resort.
Set the variable before you import Ray:

.. code-block:: bash

    RAY_DISABLE_FAILURE_SIGNAL_HANDLER=1 python your_program.py

This removes one side of the conflict, but it has a diagnostic cost. If the CoreWorker
later experiences a genuine ``SIGSEGV``, ``SIGABRT``, or similar native failure, Ray
might no longer print the C++ failure stack normally produced by Abseil. Ray's Python
fault handler can still provide Python-level diagnostics, but it doesn't restore the
disabled C++ failure stack.

Don't disable Ray's handler during normal operation. For more background and
discussion, see
`GitHub issue #36415 <https://github.com/ray-project/ray/issues/36415>`_.


Failures due to memory issues
--------------------------------
View :ref:`debugging memory issues <ray-core-mem-profiling>` for more details.


This document discusses some common problems that people run into when using Ray
as well as some known problems. If you encounter other problems, `let us know`_.

.. _`let us know`: https://github.com/ray-project/ray/issues
