.. meta::
   :description: Scale multiprocessing.Pool programs to a Ray cluster with ray.util.multiprocessing.Pool as a drop-in replacement.

.. _ray-multiprocessing:

Distributed multiprocessing.Pool
================================

.. _`issue on GitHub`: https://github.com/ray-project/ray/issues

Ray supports running distributed Python programs with the `multiprocessing.Pool API`_
using `Ray Actors <actors.html>`__ instead of local processes. This makes it easy
to scale existing applications that use ``multiprocessing.Pool`` from a single node
to a cluster.

.. _`multiprocessing.Pool API`: https://docs.python.org/3/library/multiprocessing.html#module-multiprocessing.pool

Quickstart
----------

To get started, first `install Ray <installation.html>`__, then use
``ray.util.multiprocessing.Pool`` in place of ``multiprocessing.Pool``.
This will start a local Ray cluster the first time you create a ``Pool`` and
distribute your tasks across it. See the `Run on a Cluster`_ section below for
instructions to run on a multi-node Ray cluster instead.

.. code-block:: python

  from ray.util.multiprocessing import Pool

  def f(index):
      return index

  pool = Pool()
  for result in pool.map(f, range(100)):
      print(result)

The full ``multiprocessing.Pool`` API is currently supported. Please see the
`multiprocessing documentation`_ for details.

.. warning::
  The ``context`` argument in the ``Pool`` constructor is ignored when using Ray.

.. _`multiprocessing documentation`: https://docs.python.org/3/library/multiprocessing.html#module-multiprocessing.pool

Experimental elastic actor capacity
-----------------------------------

By default, ``Pool`` eagerly creates a fixed number of actors. Supplying
``min_size``, ``max_size``, or ``idle_timeout_s`` enables elastic capacity:

.. code-block:: python

  pool = Pool(
      min_size=0,
      max_size=64,
      idle_timeout_s=60,
      ray_remote_args={"num_cpus": 1},
  )

The pool creates actors as submitted batches make existing actors busy and
retires actors that have no outstanding batches after ``idle_timeout_s``.
``min_size`` is the idle capacity floor and ``max_size`` is the actor-slot
ceiling. A slot undergoing retirement remains occupied until Ray confirms the
actor exit.

Actor creation is asynchronous. Batches are submitted directly to Ray actor
mailboxes, including while a new actor is waiting for resources or running its
initializer. This creates cluster-autoscaler demand without a local dispatcher
or readiness polling. Actor resources do not change implicitly; use
``ray_remote_args`` when actors should request CPUs, GPUs, or custom resources.
Elastic slot ownership relies on a serial, non-restarting actor mailbox, so
elastic pools reject non-default ``max_concurrency``, ``max_restarts``, and
``max_task_retries`` actor options.

``maxtasksperchild`` also applies to elastic pools. An elastic actor retires
after accepting that many batches. Its slot remains occupied until Ray confirms
the actor exit, then ``min_size`` or later demand creates a replacement.

Guarantees and failure boundaries
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Elastic capacity has the following guarantees:

- The number of actor slots never exceeds ``max_size``. A retiring slot is not
  reused until Ray reports that its actor has exited.
- ``close()`` rejects new submissions but preserves calls that Ray has already
  accepted. ``terminate()`` may abort accepted calls. Call ``join()`` after
  either method to wait for actor cleanup.
- A user-function exception fails its result without poisoning a live actor.
  A confirmed actor death releases its slot so later submissions can replace
  it. If actor ownership is ambiguous, the pool fails closed instead of
  risking two actors in one slot. This includes an ambiguous completion of the
  actor termination call: the slot remains occupied unless Ray confirms that
  the actor exited.
- Autoscaling changes actor capacity only. Ray actor mailboxes remain the task
  queue and Ray object references remain the result protocol.

These guarantees deliberately have the following boundaries:

- A sequence of Ray submissions, such as the chunks of one ``map_async()``
  call, is not a distributed transaction. If the Ray control plane fails
  synchronously between submissions, earlier chunks may already be accepted.
- A pool does not reconnect actor handles after a Ray session is replaced or
  recover transparently from an unavailable control plane. Management failures
  are reported to the caller.
- If an actor needed for ``min_size`` dies during startup, the pool fails closed
  instead of automatically retrying a permanently failing initializer without
  a bound. This makes the capacity-floor failure visible and prevents actor
  creation churn.
- ``join()`` has no independent deadline. It continues waiting if Ray never
  settles an actor termination reference, rather than reusing capacity whose
  physical actor may still exist.
- Actor capacity is bounded, but queued calls, result objects, and Joblib's
  input cache remain proportional to accepted work. The pool does not impose
  backpressure or a fixed memory limit independent of the workload.
- Applications are responsible for completing the normal pool lifecycle with
  ``close()`` or ``terminate()`` followed by ``join()``.

Run on a Cluster
----------------

This section assumes that you have a running Ray cluster. To start a Ray cluster,
see the :ref:`cluster setup <cluster-index>` instructions.

To connect a ``Pool`` to a running Ray cluster, you can specify the address of the
head node in one of two ways:

- By setting the ``RAY_ADDRESS`` environment variable.
- By passing the ``ray_address`` keyword argument to the ``Pool`` constructor.

.. code-block:: python

  from ray.util.multiprocessing import Pool

  # Starts a new local Ray cluster.
  pool = Pool()

  # Connects to a running Ray cluster, with the current node as the head node.
  # Alternatively, set the environment variable RAY_ADDRESS="auto".
  pool = Pool(ray_address="auto")

  # Connects to a running Ray cluster, with a remote node as the head node.
  # Alternatively, set the environment variable RAY_ADDRESS="<ip_address>:<port>".
  pool = Pool(ray_address="<ip_address>:<port>")

You can also start Ray manually by calling ``ray.init()`` (with any of its supported
configuration options) before creating a ``Pool``.
