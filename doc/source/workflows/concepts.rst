.. _workflows:

Ray Workflows: Fast and General Workflow Engine
===============================================

.. tip::

  Ray Workflows is available as **alpha** in Ray 1.7+. Please file feature requests and bug reports on GitHub Issues.

Ray Workflows is a library built on Ray tasks to support high-performance, *durable* workflows. It is intended to support both business workflows (e.g., thousands of long-running user workflows as in Cadence), and large-scale workflows (e.g., ML and data pipelines).

Why Workflows?
--------------

Ray Workflows builds on the dynamicity of Ray tasks to support a broad range of use cases with high performance. It offers:

**Greater flexibility:** Combine the flexibility of Ray's dynamic task graphs with strong durability guarantees. Branch or loop conditionally based on runtime data. Use Ray distributed libraries seamlessly within workflow steps.

**Higher performance:** Workflows offers sub-second overheads for task launch and supports workflows with hundreds of thousands of steps. Take advantage of the Ray object store to pass distributed datasets between steps with zero-copy overhead.

**Dependency management:** Workflows leverages Ray's runtime environment feature to snapshot the code dependencies of a workflow. This enables management of workflows and virtual actors as code is upgraded over time.

You might find that workflows is *lower level* compared to engines such as `AirFlow <https://www.astronomer.io/blog/airflow-ray-data-science-story>`__ (which can also run on Ray). This means it focuses more on core workflow primitives as opposed to management tools and integrations.

Concepts
--------

**Steps**: Functions annotated with the ``@workflow.step`` decorator. Steps are retried on failure, but once a step finishes successfully it will never be run again. Similar to Ray tasks, steps can take other step futures as arguments. Unlike Ray tasks, you are not allowed to call ``ray.get()`` or ``ray.wait()`` on step futures, which enables recoverability.

.. code-block:: python
    :caption: Composing steps together into a workflow.

    from ray import workflow

    @workflow.step
    def one() -> int:
        return 1

    @workflow.step
    def add(a: int, b: int) -> int:
        return a + b

    output: Workflow[int] = add.step(100, one.step())

**Workflows**: Running DAGs of steps created with ``output.run(workflow_id=<id>)`` or ``output.run_async(workflow_id=<id>)``. Once started, a workflow's execution is durably logged to storage. On system failure, workflows can be resumed on any Ray cluster with access to the storage.

.. code-block:: python
    :caption: Creating a new workflow run.

    workflow.init(storage="/tmp/data")
    assert output.run(workflow_id="run_1") == 101
    assert workflow.get_status("run_1") == workflow.WorkflowStatus.SUCCESSFUL
    assert workflow.get_output("run_1") == 101

**Objects**: Large data objects stored in the Ray object store. References to these objects can be passed into and returned from steps. Objects are checkpointed when initially returned from a step. After checkpointing, the object can be shared among any number of workflow steps at memory-speed via the Ray object store.

.. code-block:: python
    :caption: Returning Ray objects from a workflow.

    @ray.remote
    def hello():
        return "hello"

    @workflow.step
    def words() -> List[ray.ObjectRef]:
        return [hello.remote(), ray.put("world")]

    @workflow.step
    def concat(words: List[ray.ObjectRef]) -> str:
        return " ".join([ray.get(w) for w in words])

    workflow.init()
    assert concat.step(words.step()).run() == "hello world"

**Dynamic Workflows**: Workflows that generate new steps at runtime. When a step returns a step future as its output, that DAG of steps is dynamically inserted into the workflow DAG following the original step. This feature enables nesting, looping, and recursion within workflows.

.. code-block:: python
    :caption: The Fibonacci recursive workflow.

    @workflow.step
    def fib(n: int) -> int:
        if n <= 1:
            return n
        return add.step(fib.step(n - 1), fib.step(n - 2))

    assert fib.step(10).run() == 55

**Virtual Actors**: (This feature is under development)
