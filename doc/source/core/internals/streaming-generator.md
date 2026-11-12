---
myst:
  html_meta:
    description: "Internals of Ray streaming generator tasks: return ObjectID handling, scheduling, reporting yielded values, and backpressure."
---

(streaming-generator)=

# Streaming generator

This page explains how streaming generator tasks work in Ray 2.55 and outlines the main implementation differences from normal Ray tasks. Before you read it, read about the normal task lifecycle in {ref}`task-lifecycle`.

## Overview

A streaming generator task is a Python generator function executed as a Ray task. Unlike a normal Ray task, it doesn't return all results in the final `PushTask` reply. Instead, after each `yield`, the remote executor converts the yielded value into a Ray object and reports it to the caller before the overall generator task finishes.

The following example creates a streaming generator task:

```{eval-rst}
.. testcode::

  import ray

  @ray.remote
  def numbers():
      for i in range(3):
          yield i

  gen = numbers.remote()

  for ref in gen:
      print(ray.get(ref))
```

```{eval-rst}
.. testoutput::

  0
  1
  2
```

The `gen` variable is an `ObjectRefGenerator`. Iterating over it returns one `ObjectRef` per yielded value. `ObjectRefGenerator` isn't serializable, and you can't pass it to other remote tasks.

## Defining a streaming generator function

You define a streaming generator function with the {func}`ray.remote` decorator, the same as a normal remote function.

Inside the decorator, Ray checks whether the decorated Python function is a generator function by calling [inspect.isgeneratorfunction(function)](https://github.com/ray-project/ray/blob/ray-2.55.0/python/ray/remote_function.py#L149).

You can explicitly set `num_returns` to an integer for a generator function. This page doesn't cover that case, because Ray then treats the invocation as a fixed-return task, iterates over the generator while buffering task outputs, and returns the fixed set of `ObjectRef` objects from `.remote()`.

## Task submission and return ObjectIDs

Invoking `generator.remote()` submits one streaming generator task and [returns one ObjectRefGenerator](https://github.com/ray-project/ray/blob/ray-2.55.0/python/ray/remote_function.py#L509-L514) to the Python caller. The submission path is the same as for normal tasks until `CoreWorker` builds the `TaskSpecification`:

1. The caller exports the pickled function definition to GCS if needed.
1. The caller flattens and serializes the task arguments.
1. The caller calls into C++ `CoreWorker` to submit the task.
1. `CoreWorker` builds a `TaskSpecification`.

For streaming generator tasks, `CoreWorker` [converts the streaming return sentinel](https://github.com/ray-project/ray/blob/ray-2.55.0/src/ray/core_worker/core_worker.cc#L1896-L1902), and the `TaskSpecification` records the following fields:

1. `streaming_generator=true`: Marks the task as a streaming generator task.
1. `num_returns=1`: Creates one normal return ObjectID. This is the `generator ObjectRef`.
1. `returns_dynamic=true`: Indicates that the yielded values are dynamic returns, not fixed returns listed by `num_returns`.
1. `generator_backpressure_num_objects`: Stores the backpressure threshold. `-1` means backpressure is disabled.

The `generator ObjectRef` isn't a yielded value. Ray resolves it only when the generator task ends. Ray also uses its ObjectID as the `generator_id` in `ReportGeneratorItemReturns`. Yielded values get [deterministic object IDs derived from the task ID and the yield index](https://github.com/ray-project/ray/blob/ray-2.55.0/src/ray/common/task/task_spec.cc#L223-L229):

```
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
```

Task return object IDs are one-based, so index `0` isn't a valid task return index. Index `1` belongs to the `generator ObjectRef`. Streamed items start at index `2` to avoid sharing an object ID with the generator ObjectRef.

Before `.remote()` returns to Python, the caller-side core worker [creates an in-memory mapping](https://github.com/ray-project/ray/blob/ray-2.55.0/src/ray/core_worker/task_manager.cc#L321-L330) from the ObjectID of the generator ObjectRef to an `ObjectRefStream`. This mapping records reported yield indexes, exposes only the next in-order streamed ref to Python, and tracks where iteration should resume. The Python API wraps the generator ObjectRef in `ObjectRefGenerator` and uses it to read from the caller-side stream.

## Scheduling

Scheduling a streaming generator task follows the same worker lease path as a normal Ray task. See {ref}`task-lifecycle` for how `NormalTaskSubmitter` resolves dependencies, requests a worker lease, and sends `PushTask` to the leased worker.

The difference for streaming generators is that the caller has the `ObjectRefGenerator` before the task finishes. The caller can start iterating immediately, but iteration waits until the executor reports the next streamed return.

## Streaming generator task execution

Execution starts the same way as for a normal task:

1. The leased worker receives the `PushTask` RPC.
1. The worker deserializes the arguments.
1. The worker fetches and unpickles the remote function.
1. The worker calls the user function.

For a normal task, calling the function produces the final return values, which the executor worker serializes. The executor sends small returns back inline in the `PushTask` reply. It puts larger returns in the executor node's Plasma store and references them from the reply.

For a streaming generator task, calling the function produces a Python generator or async generator object. The executor then drives the generator itself:

1. For a sync generator, the executor worker [repeatedly calls send(stats)](https://github.com/ray-project/ray/blob/ray-2.55.0/python/ray/_raylet.pyx#L1301-L1340).
1. For an async generator, the executor worker [runs the asend(stats) loop](https://github.com/ray-project/ray/blob/ray-2.55.0/python/ray/_raylet.pyx#L1355-L1444) with the worker event loop used for async task execution.

Each `send(stats)` or `asend(stats)` resumes the Python generator until the next `yield`. After the generator yields a value, it pauses. Before the executor calls `send(stats)` or `asend(stats)` again, [it sends `ReportGeneratorItemReturns` to the caller](#reporting-yielded-values) and [waits for backpressure if needed](#backpressure). The loop ends when the generator raises `StopIteration` or `StopAsyncIteration`.

Ray streaming generators are output-only streams. Callers can only iterate over the returned `ObjectRefGenerator` to receive yielded refs. Unlike with a normal Python generator, callers can't send values back into the remote generator, because the remote executor owns the Python generator's `send` or `asend` channel. The executor passes `None` for the first `stats` value, and then passes a [StreamingGeneratorStats](https://github.com/ray-project/ray/blob/ray-2.55.0/python/ray/_raylet.pyx#L1166-L1168) object after it reports each yielded value. The stats object records executor-side metadata for the previous yielded value, such as object creation time.

## Reporting yielded values

For each yielded value, the executor runs [report_streaming_generator_output](https://github.com/ray-project/ray/blob/ray-2.55.0/python/ray/_raylet.pyx#L1170-L1233), which performs the following steps:

1. [Create the streamed return object](https://github.com/ray-project/ray/blob/ray-2.55.0/python/ray/_raylet.pyx#L1459-L1493) using the deterministic ObjectID for the yield index.
1. Store the yielded value using the same direct-return versus plasma-return rule as normal task returns. The executor serializes direct return values into the report RPC. It stores plasma return values in the Plasma store and references them from the report.
1. Send [ReportGeneratorItemReturns](https://github.com/ray-project/ray/blob/ray-2.55.0/src/ray/core_worker/core_worker.cc#L3155-L3217) from the executor to the caller.

When the caller [handles ReportGeneratorItemReturns](https://github.com/ray-project/ray/blob/ray-2.55.0/src/ray/core_worker/task_manager.cc#L779-L878), it performs the following steps:

1. Insert the returned object into the caller-side `ObjectRefStream` at the yield index.
1. Handle the reported return object as a direct return or a plasma return, using the same logic as normal task returns.
1. [Make the reported ObjectRef ready](https://github.com/ray-project/ray/blob/ray-2.55.0/src/ray/core_worker/task_manager.cc#L821-L847). If a caller is [waiting in next(gen)](https://github.com/ray-project/ray/blob/ray-2.55.0/python/ray/_private/object_ref_generator.py#L188-L237) for this in-order yield index, that wait can finish.

The following diagram shows the reporting path for yielded values:

```
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
  |------------------------- PushTask ---------------------->|
  |                 task_spec.streaming_generator = true
  |                 task_spec.num_returns = 1
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
  |
  |                         ...
  |
  |<---------------- ReportGeneratorItemReturns -----------|
  |                 returned_object.object_id = ObjectID(T, 3)
  |                 item_index = 1
  |                 generator_id = ObjectID(T, 1)
  |                 attempt_number = 0
  |
  |  Insert ObjectRef(T, 3)
  |  into ObjectRefStream at yield index 1
  |
  |  next(gen) returns ObjectRef(T, 3)
  |
  |---------------- ReportGeneratorItemReturnsReply ------>|
  |                 total_num_object_consumed = 2
  |
  |                         ...
  |
  |<----------------------- PushTaskReply -------------------|
  |                 return_objects[0] = generator_ref
  |                 streaming_generator_return_ids = [...]
```

The executor repeats this protocol for every yielded value. Finally, the `PushTask` reply marks the generator task as complete. The caller handles this reply in [`TaskManager::CompletePendingTask`](https://github.com/ray-project/ray/blob/ray-2.55.0/src/ray/core_worker/task_manager.cc#L908-L1085). The reply contains [`streaming_generator_return_ids`](https://github.com/ray-project/ray/blob/ray-2.55.0/src/ray/protobuf/core_worker.proto#L172-L174), which list the streamed return ObjectIDs and whether each return is in the Plasma store. It doesn't contain the yielded values themselves, because the executor already reported those values through `ReportGeneratorItemReturns`. The caller uses the number of entries in `streaming_generator_return_ids` to write the end-of-stream marker. For example, if the generator yielded three values, the caller writes the marker at yield index `3`. This marker tells `ObjectRefGenerator` that the executor never reports the next index, so iteration should stop instead of waiting forever.

The yield order is fixed, but the order in which [ReportGeneratorItemReturnsRequest](https://github.com/ray-project/ray/blob/ray-2.55.0/src/ray/protobuf/core_worker.proto#L434-L450) RPCs arrive at the caller isn't guaranteed. The executor sends report RPCs asynchronously. It [waits for a report response](https://github.com/ray-project/ray/blob/ray-2.55.0/src/ray/core_worker/generator_waiter.cc#L32-L57) only when generator backpressure is enabled and the number of generated but unconsumed objects reaches the configured threshold. When backpressure is disabled, or when the threshold still allows more unconsumed objects, multiple reports can be in flight. The caller uses `attempt_number` to reject stale reports from older task attempts after a retry starts.

## Backpressure

Streaming generators can produce objects faster than the caller consumes them. Ray supports a private task option named `_generator_backpressure_num_objects` to limit the number of generated but unconsumed streamed objects.

The option has the following behavior:

1. `-1` disables generator backpressure.
1. A positive value sets the maximum number of generated but unconsumed objects.
1. `1` behaves most like a local Python generator. Ray produces the next object only after the caller consumes the previous `ObjectRef`.
1. `0` isn't valid.

Ray doesn't support `_generator_backpressure_num_objects` for async generators.

After the executor sends `ReportGeneratorItemReturns`, it calls [WaitUntilObjectConsumed()](https://github.com/ray-project/ray/blob/ray-2.55.0/src/ray/core_worker/generator_waiter.cc#L32-L57) to block the executor thread while the following condition holds:

```text
generated_objects - consumed_objects >= _generator_backpressure_num_objects
```

If the stream is under the threshold, the caller replies to `ReportGeneratorItemReturns` when it handles the report. If the stream is at or above the threshold, the caller holds the report reply. The caller resumes the executor by consuming object refs from the `ObjectRefGenerator`, either by calling `next(gen)` or by iterating over the generator. When the unconsumed count drops below the threshold, the caller replies to all held backpressured report RPCs with the cumulative number of consumed objects. Each report reply updates the executor-side consumed count and can wake an executor blocked in `WaitUntilObjectConsumed()`.

## Stopping consumption

If the caller stops iterating but keeps the `ObjectRefGenerator` alive, Ray keeps the caller-side stream alive. With backpressure enabled, this can leave the executor paused in `WaitUntilObjectConsumed()`.

When the `ObjectRefGenerator` goes out of scope, its [destructor](https://github.com/ray-project/ray/blob/ray-2.55.0/python/ray/_private/object_ref_generator.py#L289-L295) asks the caller-side core worker to delete the `ObjectRefStream` for the ObjectID of the generator ObjectRef. [Deleting the stream](https://github.com/ray-project/ray/blob/ray-2.55.0/src/ray/core_worker/task_manager.cc#L690-L727) releases unconsumed streamed refs from the caller-side stream and replies to pending backpressured report RPCs with `NotFound`. This unblocks an executor that was waiting for the caller to consume more refs. If the executor reports more yielded values after the stream is deleted, the caller rejects those future `ReportGeneratorItemReturns` RPCs with `NotFound`. The executor treats the failed report reply as if [all generated refs were consumed](https://github.com/ray-project/ray/blob/ray-2.55.0/src/ray/core_worker/core_worker.cc#L3190-L3211) for backpressure accounting, so it doesn't stay blocked on the deleted stream. Deleting the `ObjectRefStream` doesn't cancel the remote generator task. The executor continues running user code until the generator finishes or the task is cancelled separately.

## Getting streamed return values

The caller receives an `ObjectRefGenerator` from `.remote()`:

```python
gen = numbers.remote()
ref = next(gen)
value = ray.get(ref)
```

`next(gen)` doesn't return the yielded Python value. It returns an `ObjectRef` for the next yielded value.

Internally, `ObjectRefGenerator` [asks the caller-side core worker](https://github.com/ray-project/ray/blob/ray-2.55.0/python/ray/_private/object_ref_generator.py#L188-L282) to peek at the next expected object ID in the stream. If the item isn't ready, the caller waits until the executor reports that object ref through `ReportGeneratorItemReturns`. When the item is ready, the caller consumes the corresponding stream entry and returns the corresponding `ObjectRef` to Python. If the caller previously held report replies because of backpressure, this consume operation can release those held replies when the unconsumed count drops below the threshold.

The caller-side core worker can compute the ObjectID for the next yield index before the executor reports it. However, it doesn't return that ref until the ref is ready, for the following reasons:

1. The generator might finish or fail before producing that index. In those cases, iteration should raise `StopIteration` or surface the task error through the generator ObjectRef instead of returning a normal-looking streamed ref that was never reported.
1. Returning refs without waiting would let the caller drain an unbounded number of refs from repeated calls to `next(gen)`. It would also break backpressure accounting because `next(gen)` is the signal that the caller consumed a stream item.

## Finishing the generator

When the Python generator raises `StopIteration` or `StopAsyncIteration` on `send(stats)` or `asend(stats)`, the executor finishes the streaming task. The completion flow has the following steps:

1. The executor [waits for all in-flight ReportGeneratorItemReturns RPCs](https://github.com/ray-project/ray/blob/ray-2.55.0/python/ray/_raylet.pyx#L1345-L1352) to finish before sending the final `PushTask` reply. This prevents the task from appearing complete before the caller receives one of the yielded objects.
1. The executor sends the `PushTask` reply. This reply includes the generator ObjectRef and the [list of streamed return ObjectIDs](https://github.com/ray-project/ray/blob/ray-2.55.0/src/ray/core_worker/task_execution/task_receiver.cc#L54-L60) produced by the task.
1. The caller handles the `PushTask` reply, records how many streaming return objects the task produced, and marks the stream as ended.
1. The caller [writes an internal end-of-stream marker](https://github.com/ray-project/ray/blob/ray-2.55.0/src/ray/core_worker/task_manager.cc#L1078-L1085) into the caller-side `ObjectRefStream`.
1. Later, when `ObjectRefGenerator` reaches the marker, it checks the generator ObjectRef.

At that point, iteration behaves as follows:

1. If the generator task succeeded, iteration raises `StopIteration` or `StopAsyncIteration`.
1. If the generator task failed and the failure hasn't been surfaced yet, `ObjectRefGenerator` returns the generator ObjectRef once. `ray.get` on that ref raises the task error.
1. After surfacing that error ref, later iteration stops.

## Failures, retries, and reconstruction

If the generator raises an application exception, Ray reports an error object at the current stream index. The caller receives an `ObjectRef` for that item, and `ray.get` on that ref raises the task exception. Ray also uses the generator ObjectRef, available through `gen.completed()`, to represent completion or failure of the whole generator task.

Ray can retry a streaming generator task from its beginning through the same task retry machinery used for normal tasks. This assumes that the generator task is idempotent and deterministic. Ray can trigger a retry when the running task attempt fails, for example because of a dependency resolution failure, worker or node death, an eligible out-of-memory failure, or a retryable application exception when `retry_exceptions` is enabled. When retries remain, the task manager marks the current attempt failed, increments the attempt number, and resubmits the same task spec.

Object reconstruction is a separate resubmission path from immediate retry on task attempt failure. It starts when a streamed return object stored in the Plasma store is lost while its lineage is still reconstructable. In that case, the object recovery manager calls [TaskManager::ResubmitTask](https://github.com/ray-project/ray/blob/ray-2.55.0/src/ray/core_worker/object_recovery_manager.cc#L140-L164) for the task that produced the lost object. If the streaming generator task is still running when reconstruction is requested, Ray [queues the resubmission](https://github.com/ray-project/ray/blob/ray-2.55.0/src/ray/core_worker/task_manager.cc#L353-L435) until the current attempt finishes or fails. If the task is already finished or failed, Ray sets up the task entry for resubmission immediately.
