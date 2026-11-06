---
myst:
  html_meta:
    description: "Task fault tolerance in Ray: catch application-level errors, configure retries for failed tasks, and cancel misbehaving tasks."
---

(fault-tolerance-tasks)=
(task-fault-tolerance)=

# Task fault tolerance

Tasks can fail because of application-level errors, such as Python-level exceptions, or system-level failures, such as a machine failure. This page describes the mechanisms you can use to recover from these errors.

## Catching application-level failures

Ray surfaces application-level failures as Python-level exceptions. When a task on a remote worker or actor fails because of a Python-level exception, Ray wraps the original exception in a `RayTaskError` and stores it as the task's return value. Ray throws this wrapped exception to any worker that tries to get the result, either by calling `ray.get` or by executing another task that depends on the object. If your exception type can be subclassed, the raised exception is an instance of both `RayTaskError` and your exception type, so you can catch either of them. Otherwise, the wrapped exception is only a `RayTaskError`, and you can access your original exception through the `cause` field of the `RayTaskError`.

```{literalinclude} ../doc_code/task_exceptions.py
:language: python
:start-after: __task_exceptions_begin__
:end-before: __task_exceptions_end__
```

The following example catches your exception type when the type can be subclassed:

```{literalinclude} ../doc_code/task_exceptions.py
:language: python
:start-after: __catch_user_exceptions_begin__
:end-before: __catch_user_exceptions_end__
```

The following example accesses your exception type when the type can't be subclassed:

```{literalinclude} ../doc_code/task_exceptions.py
:language: python
:start-after: __catch_user_final_exceptions_begin__
:end-before: __catch_user_final_exceptions_end__
```

If Ray can't serialize your exception, it converts the exception to a `RayError`.

```{literalinclude} ../doc_code/task_exceptions.py
:language: python
:start-after: __unserializable_exceptions_begin__
:end-before: __unserializable_exceptions_end__
```

Use `ray list tasks` from the {ref}`State API CLI <state-api-overview-ref>` to query task exit details:

```bash
# This API is only available when you download Ray via `pip install "ray[default]"`
ray list tasks
```

```bash
======== List: 2023-05-26 10:32:00.962610 ========
Stats:
------------------------------
Total: 3

Table:
------------------------------
    TASK_ID                                             ATTEMPT_NUMBER  NAME    STATE      JOB_ID  ACTOR_ID    TYPE         FUNC_OR_CLASS_NAME    PARENT_TASK_ID                                    NODE_ID                                                   WORKER_ID                                                 ERROR_TYPE
 0  16310a0f0a45af5cffffffffffffffffffffffff01000000                 0  f       FAILED   01000000              NORMAL_TASK  f                     ffffffffffffffffffffffffffffffffffffffff01000000  767bd47b72efb83f33dda1b661621cce9b969b4ef00788140ecca8ad  b39e3c523629ab6976556bd46be5dbfbf319f0fce79a664122eb39a9  TASK_EXECUTION_EXCEPTION
 1  c2668a65bda616c1ffffffffffffffffffffffff01000000                 0  g       FAILED   01000000              NORMAL_TASK  g                     ffffffffffffffffffffffffffffffffffffffff01000000  767bd47b72efb83f33dda1b661621cce9b969b4ef00788140ecca8ad  b39e3c523629ab6976556bd46be5dbfbf319f0fce79a664122eb39a9  TASK_EXECUTION_EXCEPTION
 2  c8ef45ccd0112571ffffffffffffffffffffffff01000000                 0  f       FAILED   01000000              NORMAL_TASK  f                     ffffffffffffffffffffffffffffffffffffffff01000000  767bd47b72efb83f33dda1b661621cce9b969b4ef00788140ecca8ad  b39e3c523629ab6976556bd46be5dbfbf319f0fce79a664122eb39a9  TASK_EXECUTION_EXCEPTION
```

(task-retries)=

## Retrying failed tasks

If a worker dies unexpectedly while executing a task, either because the process crashed or because the machine failed, Ray reruns the task until either the task succeeds or the maximum number of retries is exceeded. The default number of retries is 3. To override it, specify `max_retries` in the `@ray.remote` decorator. Specify -1 for infinite retries, or 0 to disable retries. To override the default number of retries for all submitted tasks, set the OS environment variable `RAY_TASK_MAX_RETRIES`, for example, by passing it to your driver script or by using {ref}`runtime environments<runtime-environments>`.

To experiment with this behavior, run the following code:

```{literalinclude} ../doc_code/tasks_fault_tolerance.py
:language: python
:start-after: __tasks_fault_tolerance_retries_begin__
:end-before: __tasks_fault_tolerance_retries_end__
```

When a task returns a result in the Ray object store, the resulting object can be lost after the original task has already finished. In these cases, Ray also tries to automatically recover the object by re-executing the tasks that created it. You configure this recovery with the same `max_retries` option. For more information, see {ref}`object fault tolerance <fault-tolerance-objects>`.

By default, Ray doesn't retry tasks when application code throws an exception. To control whether Ray retries application-level errors, and which application-level errors it retries, use the `retry_exceptions` argument, which is `False` by default. To enable retries on application-level errors, set `retry_exceptions=True` to retry on any exception, or pass a list of retryable exceptions. The following example shows both approaches:

```{literalinclude} ../doc_code/tasks_fault_tolerance.py
:language: python
:start-after: __tasks_fault_tolerance_retries_exception_begin__
:end-before: __tasks_fault_tolerance_retries_exception_end__
```


Use `ray list tasks -f task_id=<task_id>` from the {ref}`State API CLI <state-api-overview-ref>` to see failed task attempts and retries:

```bash
# This API is only available when you download Ray via `pip install "ray[default]"`
ray list tasks -f task_id=16310a0f0a45af5cffffffffffffffffffffffff01000000
```

```bash
======== List: 2023-05-26 10:38:08.809127 ========
Stats:
------------------------------
Total: 2

Table:
------------------------------
    TASK_ID                                             ATTEMPT_NUMBER  NAME              STATE       JOB_ID  ACTOR_ID    TYPE         FUNC_OR_CLASS_NAME    PARENT_TASK_ID                                    NODE_ID                                                   WORKER_ID                                                 ERROR_TYPE
 0  16310a0f0a45af5cffffffffffffffffffffffff01000000                 0  potentially_fail  FAILED    01000000              NORMAL_TASK  potentially_fail      ffffffffffffffffffffffffffffffffffffffff01000000  94909e0958e38d10d668aa84ed4143d0bf2c23139ae1a8b8d6ef8d9d  b36d22dbf47235872ad460526deaf35c178c7df06cee5aa9299a9255  WORKER_DIED
 1  16310a0f0a45af5cffffffffffffffffffffffff01000000                 1  potentially_fail  FINISHED  01000000              NORMAL_TASK  potentially_fail      ffffffffffffffffffffffffffffffffffffffff01000000  94909e0958e38d10d668aa84ed4143d0bf2c23139ae1a8b8d6ef8d9d  22df7f2a9c68f3db27498f2f435cc18582de991fbcaf49ce0094ddb0
```


## Cancelling misbehaving tasks

If a task is hanging, cancel it so your program can continue to make progress. To cancel a task, call `ray.cancel` on an object ref that the task returned. By default, if the task is mid-execution, this call sends a `KeyboardInterrupt` to the task's worker. Passing `force=True` to `ray.cancel` force-exits the worker. For more details, see {func}`the API reference <ray.cancel>` for `ray.cancel`.

Ray currently doesn't automatically retry cancelled tasks.

Sometimes application-level code might cause memory leaks on a worker after repeated task executions, for example, because of bugs in third-party libraries. To make progress in these cases, set the `max_calls` option in a task's `@ray.remote` decorator. Once a worker has executed this many invocations of the given remote function, it automatically exits. By default, `max_calls` is infinite for CPU tasks and 1 for GPU tasks.
