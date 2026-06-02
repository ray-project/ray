.. _streaming-generator:

Streaming Generator Internals
=============================

This document explains how streaming generator tasks work in Ray 2.55 and outlines the main implementation differences from normal Ray tasks.
For the normal task lifecycle, see :ref:`task-lifecycle`.


Overview
--------

A streaming generator task is a Python generator function executed as a Ray task.
Unlike a normal Ray task, it doesn't return all results in the final ``PushTask`` reply.
Instead, after each ``yield``, the remote executor converts the yielded value into a Ray object and reports it to the caller before the overall generator task finishes.

The following example creates a streaming generator task:

.. testcode::

  import ray

  @ray.remote
  def numbers():
      for i in range(3):
          yield i

  gen = numbers.remote()

  for ref in gen:
      print(ray.get(ref))

.. testoutput::

  0
  1
  2

The ``gen`` variable is an ``ObjectRefGenerator``.
Iterating over it returns one ``ObjectRef`` per yielded value.
But note that ``ObjectRefGenerator`` is not serializable and can't be passed to other remote tasks.


Defining a Streaming Generator Function
---------------------------------------

A streaming generator function is defined using the :func:`ray.remote` decorator, the same as a normal remote function.

But inside the decorator, Ray checks whether the decorated Python function is a generator function by calling `inspect.isgeneratorfunction(function) <https://github.com/ray-project/ray/blob/ray-2.55.0/python/ray/remote_function.py#L149>`__.

Note that a user can explicitly set ``num_returns`` to an integer for a generator function.
However, it is not the scope of this document because, in that case, Ray treats the invocation as a fixed-return task, iterates over the generator while buffering task outputs, and returns the fixed set of ``ObjectRef`` objects from ``.remote()``.

Besides ``num_returns``, a user can set ``_generator_backpressure_num_objects`` to control the backpressure behavior of a streaming generator task.


Task Submission and Return ObjectIDs
------------------------------------

Invoking ``generator.remote()`` submits one streaming generator task and `returns one ObjectRefGenerator <https://github.com/ray-project/ray/blob/ray-2.55.0/python/ray/remote_function.py#L509-L514>`__ to the Python caller.
The submission path is the same as normal tasks until ``CoreWorker`` builds the ``TaskSpecification``:

1. The caller exports the pickled function definition to GCS if needed.
2. The caller flattens and serializes the task arguments.
3. The caller calls into C++ ``CoreWorker`` to submit the task.
4. ``CoreWorker`` builds a ``TaskSpecification``.

For streaming generator tasks, ``CoreWorker`` `converts the streaming return sentinel <https://github.com/ray-project/ray/blob/ray-2.55.0/src/ray/core_worker/core_worker.cc#L1896-L1902>`__ and the ``TaskSpecification`` records:

1. ``streaming_generator=true``: Marks the task as a streaming generator task.
2. ``num_returns=1``: Creates one normal return ObjectID. This is the ``generator ObjectRef``.
3. ``returns_dynamic=true``: Indicates that the yielded values are dynamic returns, not fixed returns listed by ``num_returns``.
4. ``generator_backpressure_num_objects``: Stores the backpressure threshold. ``-1`` means backpressure is disabled.

The ``generator ObjectRef`` isn't a yielded value. It is only resolved when the generator task ends.
Ray also uses its ObjectID as the ``generator_id`` in ``ReportGeneratorItemReturns``.
Yielded values get `deterministic object IDs derived from the task ID and the yield index <https://github.com/ray-project/ray/blob/ray-2.55.0/src/ray/common/task/task_spec.cc#L223-L229>`__::

    Generator task:
      TaskID = T

    Normal task return namespace:
      ObjectID(T, index=1)              generator ObjectRef, not a yielded value
                                        used as generator_id and task completion/failure ref

    Streaming generator namespace:
      1st yield  -> ObjectID(T, index=2)
      2nd yield  -> ObjectID(T, index=3)
      3rd yield  -> ObjectID(T, index=4)
      ...
      end of stream -> ObjectID(T, index=2 + num_yields)

Task return object IDs are one-based, so index ``0`` isn't a valid task return index.
Index ``1`` belongs to the ``generator ObjectRef``.
Streamed items start at index ``2`` to avoid sharing an object ID with the generator ObjectRef.

Before ``.remote()`` returns to Python, the caller-side core worker `creates an in-memory mapping <https://github.com/ray-project/ray/blob/ray-2.55.0/src/ray/core_worker/task_manager.cc#L321-L330>`__ from the ObjectID of the generator ObjectRef to an ``ObjectRefStream``.
This mapping records reported yield indexes, exposes only the next in-order streamed ref to Python, and tracks where iteration should resume.
The Python API wraps the generator ObjectRef in ``ObjectRefGenerator`` and uses it to read from the caller-side stream.


Scheduling
----------

Scheduling a streaming generator task follows the same worker lease path as a normal Ray task.
See :ref:`task-lifecycle` for how ``NormalTaskSubmitter`` resolves dependencies, requests a worker lease, and sends ``PushTask`` to the leased worker.

The streaming generator specific difference is that the caller now has the ``ObjectRefGenerator`` before the task finishes.
The caller can start iterating immediately, but iteration waits until the executor reports the next streamed return.


Streaming Generator Task Execution
----------------------------------

Execution starts the same as a normal task:

1. The leased worker receives the ``PushTask`` RPC.
2. The worker deserializes the arguments.
3. The worker fetches and unpickles the remote function.
4. The worker calls the user function.

For a normal task, calling the function produces the final return values.
The executor worker serializes the return values.
Small returns are sent back inline in the ``PushTask`` reply, while larger returns are put in the executor node's plasma object store and referenced from the reply.

For a streaming generator task, calling the function produces a Python generator or async generator object.
The executor then drives the generator itself:

1. For a sync generator, the executor worker `repeatedly calls send(stats) <https://github.com/ray-project/ray/blob/ray-2.55.0/python/ray/_raylet.pyx#L1301-L1340>`__.
2. For an async generator, the executor worker `runs the asend(stats) loop <https://github.com/ray-project/ray/blob/ray-2.55.0/python/ray/_raylet.pyx#L1355-L1444>`__ with the worker event loop used for async task execution.

Each ``send(stats)`` or ``asend(stats)`` resumes the Python generator until the next ``yield``.
After a value is yielded, the generator is paused.
Before the executor calls ``send(stats)`` or ``asend(stats)`` again, it serializes the yielded value, stores it inline or in plasma, sends ``ReportGeneratorItemReturns`` to the caller, and waits for backpressure if needed.
The loop ends when the generator raises ``StopIteration`` or ``StopAsyncIteration``.

Note that Ray streaming generators are output-only streams.
Callers can only iterate over the returned ``ObjectRefGenerator`` to receive yielded refs.
Callers can't send values back into the remote generator, which is allowed for normal Python generators, because the remote executor owns the Python generator's ``send`` or ``asend`` channel.
It passes ``None`` for the first ``stats`` value, and then passes a `StreamingGeneratorStats <https://github.com/ray-project/ray/blob/ray-2.55.0/python/ray/_raylet.pyx#L1166-L1168>`__ object after each yielded value is reported.
The stats object records executor-side metadata for the previous yielded value, such as object creation and storage time.


Reporting Yielded Values
------------------------

For each yielded value, the executor runs `report_streaming_generator_output <https://github.com/ray-project/ray/blob/ray-2.55.0/python/ray/_raylet.pyx#L1170-L1233>`__, which performs the following steps:

1. `Create the streamed return object <https://github.com/ray-project/ray/blob/ray-2.55.0/python/ray/_raylet.pyx#L1459-L1493>`__ using the deterministic ObjectID for the yield index.
2. Store the yielded value using the same direct-return versus plasma-return rule as normal task returns. Direct return values are serialized into the report RPC. Plasma return values are stored in plasma and referenced from the report.
3. Send `ReportGeneratorItemReturns <https://github.com/ray-project/ray/blob/ray-2.55.0/src/ray/core_worker/core_worker.cc#L3155-L3217>`__ from the executor to the caller.

When the caller `handles ReportGeneratorItemReturns <https://github.com/ray-project/ray/blob/ray-2.55.0/src/ray/core_worker/task_manager.cc#L779-L878>`__, it performs the following steps:

1. Insert the returned object into the caller-side ``ObjectRefStream`` at the yield index.
2. Handle the reported return object using the same direct-return versus plasma-return logic as normal task returns.
3. `Make the reported ObjectRef ready <https://github.com/ray-project/ray/blob/ray-2.55.0/src/ray/core_worker/task_manager.cc#L821-L847>`__. If a caller is `waiting in next(gen) <https://github.com/ray-project/ray/blob/ray-2.55.0/python/ray/_private/object_ref_generator.py#L188-L237>`__ for this in-order yield index, that wait can now finish.

The following diagram shows the reporting path for the first yielded value::

    Caller / owner process                             Executor worker
    ----------------------                             ---------------

    gen = task.remote()
      |
      |  generator_ref = ObjectRef(T, 1)
      |  (the generator ObjectRef, not a yielded value)
      |
      |  Create ObjectRefGenerator
      |  and ObjectRefStream(generator_ref)
      |
      |                                                Run generator task
      |                                                output = 1st yield
      |                                                        |
      |                                                Compute return ObjectID
      |                                                ObjectID(T, index=2)
      |                                                        |
      |                                                Store yielded value
      |                                                inline or in plasma
      |                                                        |
      |<---------------- ReportGeneratorItemReturns -----------|
      |                 returned_object.object_id = ObjectID(T, 2)
      |                   # task return object index
      |                 worker_addr = executor address
      |                 item_index = 0
      |                   # yield index
      |                 generator_id = ObjectID(T, 1)
      |                   # ObjectID inside generator_ref
      |                 attempt_number = 0
      |
      |  Insert ObjectRef(T, 2)
      |  into ObjectRefStream at yield index 0
      |
      |  next(gen) returns ObjectRef(T, 2)
      |
      |---------------- ReportGeneratorItemReturnsReply ------>|
      |                 total_num_object_consumed = 1
      |                 (also releases backpressure if the
      |                 caller delayed the reply)

The executor repeats this protocol for every yielded value.
The `final PushTask reply <https://github.com/ray-project/ray/blob/ray-2.55.0/src/ray/core_worker/task_execution/task_receiver.cc#L54-L60>`__ only completes the generator task.
It contains streamed return ObjectIDs, not all streamed values.

The `ReportGeneratorItemReturnsRequest <https://github.com/ray-project/ray/blob/ray-2.55.0/src/ray/protobuf/core_worker.proto#L434-L450>`__ RPC ordering isn't guaranteed.
The executor sends report RPCs asynchronously.
It `waits for a report response <https://github.com/ray-project/ray/blob/ray-2.55.0/src/ray/core_worker/generator_waiter.cc#L32-L57>`__ only when generator backpressure is enabled and the number of generated but unconsumed objects reaches the configured threshold.
When backpressure is disabled, or when the threshold still allows more unconsumed objects, multiple reports can be in flight.
The caller uses ``attempt_number`` to reject stale reports from older task attempts after a retry has started.


Backpressure
------------

Streaming generators can produce objects faster than the caller consumes them.
Ray supports a private task option named ``_generator_backpressure_num_objects`` to limit generated but unconsumed streamed objects.

The option has the following behavior:

1. ``-1`` disables generator backpressure.
2. A positive value sets the maximum number of generated but unconsumed objects.
3. ``1`` behaves most like a local Python generator: Ray produces the next object only after the caller consumes the previous ``ObjectRef``.
4. ``0`` isn't valid.

Note that Ray doesn't support ``_generator_backpressure_num_objects`` for async generators.

After the executor sends ``ReportGeneratorItemReturns``, it calls `WaitUntilObjectConsumed() <https://github.com/ray-project/ray/blob/ray-2.55.0/src/ray/core_worker/generator_waiter.cc#L32-L57>`__.
This call blocks the executor thread while:

.. code-block:: text

  generated_objects - consumed_objects >= _generator_backpressure_num_objects

If the stream is under the threshold, the caller replies to ``ReportGeneratorItemReturns`` when it handles the report.
If the stream is at or above the threshold, the caller holds the report reply.
The caller resumes the executor by consuming object refs from the ``ObjectRefGenerator`` by calling ``next(gen)`` or iterating over the generator.
When the unconsumed count drops below the threshold, the caller replies to all held backpressured report RPCs with the cumulative number of consumed objects.
Each report reply updates the executor-side consumed count and can wake an executor blocked in ``WaitUntilObjectConsumed()``.


Stopping Consumption
--------------------

If the caller stops iterating but keeps the ``ObjectRefGenerator`` alive, Ray keeps the caller-side stream alive.
With backpressure enabled, this can leave the executor paused in ``WaitUntilObjectConsumed()``.

When the ``ObjectRefGenerator`` goes out of scope, its `destructor <https://github.com/ray-project/ray/blob/ray-2.55.0/python/ray/_private/object_ref_generator.py#L289-L295>`__ asks the caller-side core worker to delete the ``ObjectRefStream`` for the ObjectID of the generator ObjectRef.
`Deleting the stream <https://github.com/ray-project/ray/blob/ray-2.55.0/src/ray/core_worker/task_manager.cc#L690-L727>`__ releases unconsumed streamed refs from the caller-side stream and replies to pending backpressured report RPCs with ``NotFound``.
This unblocks an executor that was waiting for the caller to consume more refs.
If the executor reports more yielded values after the stream is deleted, the caller rejects those future ``ReportGeneratorItemReturns`` RPCs with ``NotFound``.
Note that deleting the ``ObjectRefStream`` doesn't cancel the remote generator task. The executor continues running user code until the generator finishes or the task is cancelled separately.


Getting Streamed Return Values
------------------------------

The caller receives an ``ObjectRefGenerator`` from ``.remote()``:

.. code-block:: python

  gen = numbers.remote()
  ref = next(gen)
  value = ray.get(ref)

``next(gen)`` doesn't return the yielded Python value.
It returns an ``ObjectRef`` for the next yielded value.

Internally, ``ObjectRefGenerator`` `asks the caller-side core worker <https://github.com/ray-project/ray/blob/ray-2.55.0/python/ray/_private/object_ref_generator.py#L188-L282>`__ to peek at the next expected object ID in the stream.
If the item isn't ready, the caller waits until the executor reports that object reference through ``ReportGeneratorItemReturns``.
When the item is ready, the caller consumes the corresponding stream entry and returns the corresponding ``ObjectRef`` to Python.
If the caller previously held report replies because of backpressure, this consume operation can release those held replies once the unconsumed count drops below the threshold.

The caller-side core worker can compute the ObjectID for the next yield index before the executor reports it.
However, it doesn't return that ref until it is ready for the following reasons:

1. The generator may finish or fail before producing that index. In those cases, iteration should raise ``StopIteration`` or surface the task error through the generator ObjectRef instead of returning a normal-looking streamed ref that was never reported.
2. Returning refs without waiting would let the caller drain an unbounded number of refs from repeated calls to ``next(gen)``. It would also break backpressure accounting because ``next(gen)`` is the signal that the caller consumed a stream item.


Finishing the Generator
-----------------------

When the Python generator raises ``StopIteration`` or ``StopAsyncIteration``, the executor finishes the streaming task.
The completion flow is:

1. The executor `waits for all in-flight ReportGeneratorItemReturns RPCs <https://github.com/ray-project/ray/blob/ray-2.55.0/python/ray/_raylet.pyx#L1345-L1352>`__ to finish before sending the final ``PushTask`` reply. This prevents the task from appearing complete before the caller receives one of the yielded objects.
2. The executor sends the final ``PushTask`` reply. This reply includes the generator ObjectRef and the `list of streamed return ObjectIDs <https://github.com/ray-project/ray/blob/ray-2.55.0/src/ray/core_worker/task_execution/task_receiver.cc#L54-L60>`__ produced by the task.
3. The caller handles the final ``PushTask`` reply, records how many streaming return objects the task produced, and marks the stream as ended.
4. The caller `writes an internal end-of-stream marker <https://github.com/ray-project/ray/blob/ray-2.55.0/src/ray/core_worker/task_manager.cc#L1078-L1085>`__ into the caller-side ``ObjectRefStream``.
5. Later, when ``ObjectRefGenerator`` reaches the marker, it checks the generator ObjectRef.

At that point:

1. If the generator task succeeded, iteration raises ``StopIteration`` or ``StopAsyncIteration``.
2. If the generator task failed and the failure hasn't been surfaced yet, ``ObjectRefGenerator`` returns the generator ObjectRef once. ``ray.get`` on that ref raises the task error.
3. After surfacing that error ref, later iteration stops.


Failures, Retries, and Reconstruction
-------------------------------------

If the generator raises an application exception, Ray reports an error object at the current stream index.
The caller receives an ``ObjectRef`` for that item, and ``ray.get`` on that ref raises the task exception.
Ray also uses the generator ObjectRef, available through ``gen.completed()``, to represent completion or failure of the whole generator task.

Ray can retry a streaming generator task through the same task retry machinery used for normal tasks. This assumes that the generator task is idempotent and deterministic.
A retry can be triggered when the running task attempt fails, such as dependency resolution failure, worker or node death, eligible out-of-memory failure, or a retryable application exception when ``retry_exceptions`` is enabled.
When retries remain, the task manager marks the current attempt failed, increments the attempt number, and resubmits the same task spec.

Object reconstruction is a separate resubmission path from immediate retry on task attempt failure.
It starts when a streamed return object stored in plasma is lost while its lineage is still reconstructable.
In that case, the object recovery manager calls `TaskManager::ResubmitTask <https://github.com/ray-project/ray/blob/ray-2.55.0/src/ray/core_worker/object_recovery_manager.cc#L140-L164>`__ for the task that produced the lost object.
If the streaming generator task is still running when reconstruction is requested, Ray `queues the resubmission <https://github.com/ray-project/ray/blob/ray-2.55.0/src/ray/core_worker/task_manager.cc#L353-L435>`__ until the current attempt finishes or fails.
If the task is already finished or failed, Ray sets up the task entry for resubmission immediately.
