.. _workflows:

Ray Workflows: Durable Ray Task Graphs
======================================================================

.. warning::

  Ray Workflow is available as **alpha** in Ray 2.0+. Expect rough corners and
  for its APIs and storage format to change. Please file feature requests and
  bug reports on GitHub Issues or join the discussion on the
  `Ray Slack <https://forms.gle/9TSdDYUgxYs8SA9e8>`__.

Ray Workflow provides high-performance, *durable* application workflows using
Ray tasks as the underlying execution engine. It's supposed to be used to
orchestrate Ray apps so that the failed Ray jobs can resume from the place
where they failed without recomputing.


Why Ray Workflows?
-----------------

**Flexibility:** Combine the flexibility of Ray's dynamic task graphs with
strong durability guarantees. Branch or loop conditionally based on runtime
data. Use Ray distributed libraries seamlessly within workflow tasks.

**Performance:** Ray Workflows offers sub-second overheads for task launch and
supports workflows with hundreds of thousands of tasks. Take advantage of the
Ray object store to pass distributed datasets between tasks with zero-copy
overhead.

**Dependency management:** Ray Workflows leverages Ray's runtime environment
feature to snapshot the code dependencies of a workflow. This enables management
of workflows as code is upgraded over time.

You might find that Ray Workflows is *lower level* compared to engines such as
`AirFlow <https://www.astronomer.io/blog/airflow-ray-data-science-story>`__
(which can also run on Ray). This is because Ray Workflow focuses more on core
workflow primitives as opposed to tools and integrations.

Concepts
--------
Ray Workflows provides the durable *task* primitives, which are analogous to
Ray's non-durable tasks.

Ray DAG
~~~~~~~

If you’re new to Ray, we recommend starting with the
:ref:`walkthrough <core-walkthrough>`.

Normally, Ray tasks are executed eagerly.
Ray DAG provides a way to build the DAG without execution, and Ray Workflow is
based on Ray DAGs.

It is simple to build a Ray DAG: you just replace all ``.remote(...)`` with
``.bind(...)`` in a Ray application. Ray DAGs can be composed in arbitrarily
like normal Ray tasks.

Unlike Ray tasks, you are not allowed to call ``ray.get()`` or ``ray.wait()`` on
DAGs. And instead, you need an engine to execute the DAGs.

.. code-block:: python
    :caption: Composing functions together into a DAG:

    import ray

    @ray.remote
    def one() -> int:
        return 1

    @ray.remote
    def add(a: int, b: int) -> int:
        return a + b

    dag = add.bind(100, one.bind())


Workflows
~~~~~~~~~

It takes a single line of code to run a workflow DAG:

.. code-block:: python
    :caption: Run a workflow DAG:

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

.. code-block:: python
    :caption: Retrieve the output:

    # configure the storage with "ray.init" or "ray start --head --storage=<STORAGE_URI>"
    # A default temporary storage is used by by the workflow if starting without
    # Ray init.
    ray.init(storage="/tmp/data")
    assert output.run(workflow_id="run_1") == 101
    assert workflow.get_status("run_1") == workflow.WorkflowStatus.SUCCESSFUL
    assert workflow.get_output("run_1") == 101
    # workflow.get_output_async returns an ObjectRef.
    assert ray.get(workflow.get_output_async("run_1")) == 101

Objects
~~~~~~~
Large data objects can be stored in the Ray object store. References to these
objects can be passed into and returned from tasks. Objects are checkpointed
when initially returned from a task. After checkpointing, the object can be
shared among any number of workflow tasks at memory-speed via the Ray object
store.

.. code-block:: python
    :caption: Using Ray objects in a workflow:

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

Dynamic Workflow
~~~~~~~~~~~~~~~~
Workflows can generate new tasks at runtime. This is achieved by returning a
continuation of a DAG. A continuation is something returned by a function and
executed after it returns. The continuation feature enables nesting, looping,
and recursion within workflows.

.. code-block:: python
    :caption: The Fibonacci recursive workflow:

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

.. code-block:: python
    :caption: Using events.

    # Sleep is a special type of event.
    sleep_task = workflow.sleep(100)

    # `wait_for_events` allows for pluggable event listeners.
    event_task = workflow.wait_for_event(MyEventListener)

    @ray.remote
    def gather(*args):
        return args

    # If a task's arguments include events, the task won't be executed until all
    # of the events have occurred.
    workflow.run(gather.bind(sleep_task, event_task, "hello world"))
