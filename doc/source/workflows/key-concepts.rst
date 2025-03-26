Key Concepts
------------

.. warning::

  The experimental Ray Workflows library has been deprecated and will be removed in a
  future version of Ray.

.. note::
  Workflows is a library that provides strong durability for Ray task graphs.
  If you’re brand new to Ray, we recommend starting with the :ref:`core walkthrough <core-walkthrough>` instead.

DAG API
~~~~~~~

Normally, Ray tasks are executed eagerly.
In order to provide durability, Ray Workflows uses the lazy :ref:`Ray DAG API <ray-dag-guide>`
to separate the definition and execution of task DAGs.

Switching from Ray tasks to the DAG API is simple: just replace all calls to ``.remote(...)``
(which return object references), to calls to ``.bind(...)`` (which return DAG nodes).
Ray DAG nodes can otherwise be composed like normal Ray tasks.

However, unlike Ray tasks, you are not allowed to call ``ray.get()`` or ``ray.wait()`` on
DAG nodes. Instead, the DAG needs to be *executed* in order to compute a result.

Composing functions together into a DAG:

.. testcode::
    :hide:

    import tempfile
    import ray

    temp_dir = tempfile.TemporaryDirectory()

    ray.init(storage=f"file://{temp_dir.name}")

.. testcode::

    import ray

    @ray.remote
    def one() -> int:
        return 1

    @ray.remote
    def add(a: int, b: int) -> int:
        return a + b

    dag = add.bind(100, one.bind())


Workflow Execution
~~~~~~~~~~~~~~~~~~

To execute a DAG with workflows, use `workflow.run`:

.. testcode::

    from ray import workflow

    # Run the workflow until it completes and returns the output
    assert workflow.run(dag) == 101

    # Or you can run it asynchronously and fetch the output via 'ray.get'
    output_ref = workflow.run_async(dag)
    assert ray.get(output_ref) == 101


Once started, a workflow's execution is durably logged to storage. On system
failure, the workflow can be resumed on any Ray cluster with access to the
storage.

When executing the workflow DAG, workflow tasks are retried on failure, but once
they finish successfully and the results are persisted by the workflow engine,
they will never be run again.

Getting the result of a workflow:

.. testcode::
    :hide:

    ray.shutdown()

.. testcode::

    # configure the storage with "ray.init" or "ray start --head --storage=<STORAGE_URI>"
    # A default temporary storage is used by by the workflow if starting without
    # Ray init.
    ray.init(storage="/tmp/data")
    assert workflow.run(dag, workflow_id="run_1") == 101
    assert workflow.get_status("run_1") == workflow.WorkflowStatus.SUCCESSFUL
    assert workflow.get_output("run_1") == 101
    # workflow.get_output_async returns an ObjectRef.
    assert ray.get(workflow.get_output_async("run_1")) == 101

Objects
~~~~~~~
Workflows integrates seamlessly with Ray objects, by allowing Ray object
references to be passed into and returned from tasks. Objects are checkpointed
when initially returned from a task. After checkpointing, the object can be
shared among any number of workflow tasks at memory-speed via the Ray object
store.

Using Ray objects in a workflow:

.. testcode::

    import ray
    from typing import List

    @ray.remote
    def hello():
        return "hello"

    @ray.remote
    def words() -> List[ray.ObjectRef]:
        # NOTE: Here it is ".remote()" instead of ".bind()", so
        # it creates an ObjectRef instead of a DAG.
        return [hello.remote(), ray.put("world")]

    @ray.remote
    def concat(words: List[ray.ObjectRef]) -> str:
        return " ".join([ray.get(w) for w in words])

    assert workflow.run(concat.bind(words.bind())) == "hello world"

Dynamic Workflows
~~~~~~~~~~~~~~~~~
Workflows can generate new tasks at runtime. This is achieved by returning a
continuation of a DAG. A continuation is something returned by a function and
executed after it returns. The continuation feature enables nesting, looping,
and recursion within workflows.

The Fibonacci recursive workflow:

.. testcode::

    @ray.remote
    def add(a: int, b: int) -> int:
        return a + b

    @ray.remote
    def fib(n: int) -> int:
        if n <= 1:
            return n
        # return a continuation of a DAG
        return workflow.continuation(add.bind(fib.bind(n - 1), fib.bind(n - 2)))

    assert workflow.run(fib.bind(10)) == 55


Events
~~~~~~
Events are external signals sent to the workflow. Workflows can be efficiently
triggered by timers or external events using the event system.

.. testcode::

    import time

    # Sleep is a special type of event.
    sleep_task = workflow.sleep(1)

    # `wait_for_events` allows for pluggable event listeners.
    event_task = workflow.wait_for_event(workflow.event_listener.TimerListener, time.time() + 2)

    @ray.remote
    def gather(*args):
        return args

    # If a task's arguments include events, the task won't be executed until all
    # of the events have occurred.
    workflow.run(gather.bind(sleep_task, event_task, "hello world"))
