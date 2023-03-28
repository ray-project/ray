Getting Started
===============

.. note::
  Workflows is a library that provides strong durability for Ray task graphs.
  If you’re brand new to Ray, we recommend starting with the :ref:`core walkthrough <core-walkthrough>` instead.

Your first workflow
-------------------

Let's start by defining a simple workflow DAG, which we'll use for the below example.
Here is a single three-node DAG (note the use of ``.bind(...)`` instead of
``.remote(...)``). The DAG will not be executed until further actions are
taken on it:

.. code-block:: python

    from typing import List
    import ray

    # Define Ray remote functions.
    @ray.remote
    def read_data(num: int):
        return [i for i in range(num)]

    @ray.remote
    def preprocessing(data: List[float]) -> List[float]:
        return [d**2 for d in data]

    @ray.remote
    def aggregate(data: List[float]) -> float:
        return sum(data)

    # Build the DAG:
    # data -> preprocessed_data -> aggregate
    data = read_data.bind(10)
    preprocessed_data = preprocessing.bind(data)
    output = aggregate.bind(preprocessed_data)


We can plot this DAG by using ``ray.dag.vis_utils.plot(output, "output.jpg")``:

.. image:: basic.png
   :width: 500px
   :align: center

Next, let's execute the DAG we defined and inspect the result:

.. code-block:: python

    # <follow the previous code>
    from ray import workflow

    # Execute the workflow and print the result.
    print(workflow.run(output))

    # You can also run the workflow asynchronously and fetch the output via
    # 'ray.get'
    output_ref = workflow.run_async(output)
    print(ray.get(output_ref))


Each node in the original DAG becomes a workflow task. You can think of workflow
tasks as wrappers around Ray tasks that insert *checkpointing logic* to
ensure intermediate results are durably persisted. This enables workflow DAGs to
always resume from the last successful task on failure.

Setting workflow options
------------------------

You can directly set Ray options to a workflow task just like a normal
Ray remote function. To set workflow-specific options, use ``workflow.options``
either as a decorator or as kwargs to ``<task>.options``:

.. code-block:: python

    import ray
    from ray import workflow

    @workflow.options(max_retries=5)
    @ray.remote(num_cpus=2, num_gpus=3)
    def read_data(num: int):
        return [i for i in range(num)]

    read_data_with_options = read_data.options(
        num_cpus=1, num_gpus=1, **workflow.options(checkpoint=True))


Retrieving Workflow Results
---------------------------

To retrieve a workflow result, assign ``workflow_id`` when running a workflow:

.. code-block:: python

    import ray
    from ray import workflow

    try:
        # Cleanup previous workflows
        # An exception will be raised if it doesn't exist.
        workflow.delete("add_example")
    except workflow.WorkflowNotFoundError:
        pass

    @ray.remote
    def add(left: int, right: int) -> int:
        return left + right

    @ray.remote
    def get_val() -> int:
        return 10

    ret = add.bind(get_val.bind(), 20)

    assert workflow.run(ret, workflow_id="add_example") == 30

The workflow results can be retrieved with
``workflow.get_output(workflow_id)``. If a workflow is not given a
``workflow_id``, a random string is set as the ``workflow_id``. To list all
workflow ids, call ``ray.workflow.list_all()``.

.. code-block:: python

    assert workflow.get_output("add_example") == 30
    # "workflow.get_output_async" is an asynchronous version

Sub-Task Results
~~~~~~~~~~~~~~~~

We can retrieve the results for individual workflow tasks too with *task id*. Task ID can be given with ``task_id``:

 1) via ``.options(**workflow.options(task_id="task_name"))``
 2) via decorator ``@workflow.options(task_id="task_name")``

If tasks are not given ``task_id``, the function name of the steps is set as the ``task_id``.
If there are multiple tasks with the same id, a suffix with a counter ``_n`` will be added.

Once a task id is given, the result of the task will be retrievable via ``workflow.get_output(workflow_id, task_id="task_id")``.
If the task with the given ``task_id`` hasn't been executed before the workflow completes, an exception will be thrown. Here are some examples:

.. code-block:: python

    import ray
    from ray import workflow

    workflow_id = "double"
    try:
        # cleanup previous workflows
        workflow.delete(workflow_id)
    except workflow.WorkflowNotFoundError:
        pass

    @ray.remote
    def double(v):
        return 2 * v

    inner_task = double.options(**workflow.options(task_id="inner")).bind(1)
    outer_task = double.options(**workflow.options(task_id="outer")).bind(inner_task)
    result_ref = workflow.run_async(outer_task, workflow_id="double")

    inner = workflow.get_output_async(workflow_id, task_id="inner")
    outer = workflow.get_output_async(workflow_id, task_id="outer")

    assert ray.get(inner) == 2
    assert ray.get(outer) == 4
    assert ray.get(result_ref) == 4

Error handling
--------------

Workflow provides two ways to handle application-level exceptions: (1) automatic retry (as in normal Ray tasks), and (2) the ability to catch and handle exceptions.

- If ``max_retries`` is given, the task will be retried for the given number of times if the workflow task failed.
- If ``retry_exceptions`` is True, then the workflow task retries both task crashes and application-level errors;
  if it is ``False``, then the workflow task only retries task crashes.
- If ``catch_exceptions`` is True, the return value of the function will be converted to ``Tuple[Optional[T], Optional[Exception]]``.
  It can be combined with ``max_retries`` to retry a given number of times before returning the result tuple.

``max_retries`` and ``retry_exceptions`` are also Ray task options,
so they should be used inside the Ray remote decorator. Here is how you could use them:

.. code-block:: python

    # specify in decorator
    @workflow.options(catch_exceptions=True)
    @ray.remote(max_retries=5, retry_exceptions=True)
    def faulty_function():
        pass

    # specify in .options()
    faulty_function.options(max_retries=3, retry_exceptions=False,
                            **workflow.options(catch_exceptions=False))

.. note::  By default ``retry_exceptions`` is ``False``, and ``max_retries`` is ``3``.

Here is one example:

.. code-block:: python

    from typing import Tuple
    import random

    import ray
    from ray import workflow

    @ray.remote
    def faulty_function() -> str:
        if random.random() > 0.5:
            raise RuntimeError("oops")
        return "OK"

    # Tries up to five times before giving up.
    r1 = faulty_function.options(max_retries=5).bind()
    workflow.run(r1)

    @ray.remote
    def handle_errors(result: Tuple[str, Exception]):
        # The exception field will be None on success.
        err = result[1]
        if err:
            return "There was an error: {}".format(err)
        else:
            return "OK"

    # `handle_errors` receives a tuple of (result, exception).
    r2 = faulty_function.options(**workflow.options(catch_exceptions=True)).bind()
    workflow.run(handle_errors.bind(r2))


Durability guarantees
---------------------

Workflow tasks provide *exactly-once* execution semantics. What this means is
that **once the result of a workflow task is logged to durable storage, Ray
guarantees the task will never be re-executed**. A task that receives the output
of another workflow task can be assured that its inputs tasks will never be
re-executed.

Failure model
~~~~~~~~~~~~~
- If the cluster fails, any workflows running on the cluster enter ``RESUMABLE`` state. The workflows can be resumed on another cluster (see the management API section).
- The lifetime of the workflow is not coupled with the driver. If the driver exits, the workflow will continue running in the background of the cluster.

Note that tasks that have side effects still need to be idempotent. This is because the task could always fail before its result is logged.

.. code-block:: python
    :caption: Non-idempotent workflow:

    @ray.remote
    def book_flight_unsafe() -> FlightTicket:
        ticket = service.book_flight()
        # Uh oh, what if we failed here?
        return ticket

    # UNSAFE: we could book multiple flight tickets
    workflow.run(book_flight_unsafe.bind())

.. code-block:: python
    :caption: Idempotent workflow:

    @ray.remote
    def generate_id() -> str:
       # Generate a unique idempotency token.
       return uuid.uuid4().hex

    @ray.remote
    def book_flight_idempotent(request_id: str) -> FlightTicket:
       if service.has_ticket(request_id):
           # Retrieve the previously created ticket.
           return service.get_ticket(request_id)
       return service.book_flight(request_id)

    # SAFE: book_flight is written to be idempotent
    request_id = generate_id.bind()
    workflow.run(book_flight_idempotent.bind(request_id))

Dynamic workflows
-----------------

Workflow tasks can be dynamically created in the runtime. In theory, Ray DAG is
static which means a DAG node can't be returned in a DAG node. For example, the
following code is invalid:

.. code-block:: python

    @ray.remote
    def bar(): ...

    @ray.remote
    def foo():
        return bar.bind() # This is invalid since Ray DAG is static

    ray.get(foo.bind().execute()) # This will error

Workflow introduces a utility function called ``workflow.continuation`` which
makes Ray DAG node can return a DAG in the runtime:

.. code-block:: python

    @ray.remote
    def bar():
        return 10

    @ray.remote
    def foo():
        # This will return a DAG to be executed
        # after this function is finished.
        return workflow.continuation(bar.bind())

    assert ray.get(foo.bind().execute()) == 10
    assert workflow.run(foo.bind()) == 10


The dynamic workflow enables nesting, looping, and recursion within workflows.

The following example shows how to implement the recursive ``factorial`` program
using dynamically workflow: 

.. code-block:: python

    @ray.remote
    def factorial(n: int) -> int:
        if n == 1:
            return 1
        else:
            # Here a DAG is passed to the continuation.
            # The DAG will continue to be executed after this task.
            return workflow.continuation(multiply.bind(n, factorial.bind(n - 1)))

    @ray.remote
    def multiply(a: int, b: int) -> int:
        return a * b

    assert workflow.run(factorial.bind(10)) == 3628800
    # You can also execute the code with Ray DAG engine.
    assert ray.get(factorial.bind(10).execute()) == 3628800


The key behavior to note is that when a task returns a DAG wrapped by
``workflow.continuation`` instead of a concrete value, that wrapped DAG will be
substituted for the task's return. 

To better understand dynamic workflows, let's look at a more realistic example of booking a trip:

.. code-block:: python

    @ray.remote
    def book_flight(...) -> Flight: ...

    @ray.remote
    def book_hotel(...) -> Hotel: ...

    @ray.remote
    def finalize_or_cancel(
        flights: List[Flight],
        hotels: List[Hotel]) -> Receipt: ...

    @ray.remote
    def book_trip(origin: str, dest: str, dates) -> Receipt:
        # Note that the workflow engine will not begin executing
        # child workflows until the parent task returns.
        # This avoids task overlap and ensures recoverability.
        f1 = book_flight.bind(origin, dest, dates[0])
        f2 = book_flight.bind(dest, origin, dates[1])
        hotel = book_hotel.bind(dest, dates)
        return workflow.continuation(finalize_or_cancel.bind([f1, f2], [hotel]))

    receipt: Receipt = workflow.run(book_trip.bind("OAK", "SAN", ["6/12", "7/5"]))

Here the workflow initially just consists of the ``book_trip`` task. Once
executed, ``book_trip`` generates tasks to book flights and hotels in parallel,
which feeds into a task to decide whether to cancel the trip or finalize it. The
DAG can be visualized as follows (note the dynamically generated nested
workflows within ``book_trip``): 

.. image:: trip.png
   :width: 500px
   :align: center

The execution order here will be:
1. Run the ``book_trip`` task.
2. Run the two ``book_flight`` tasks and the ``book_hotel``  task in parallel.
3. Once all three booking tasks finish, ``finalize_or_cancel`` will be executed and its return will be the output of the workflow.

Ray Integration
---------------

Mixing workflow tasks with Ray tasks and actors
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Workflows are compatible with Ray tasks and actors. There are two methods of using them together:

1. Workflows can be launched from within a Ray task or actor. For example, you can launch a long-running workflow from Ray serve in response to a user request. This is no different from launching a workflow from the driver program.
2. Workflow tasks can use Ray tasks or actors within a single task. For example, a task could use Ray Train internally to train a model. No durability guarantees apply to the tasks or actors used within the task; if the task fails, it will be re-executed from scratch.

Passing nested arguments
~~~~~~~~~~~~~~~~~~~~~~~~
Like Ray tasks, when you pass a list of task outputs to a task, the values are
not resolved. But we ensure that all ancestors of a task are fully executed
before the task starts which is different from passing them into a Ray remote
function whether they have been executed or not is not defined.

.. code-block:: python

    @ray.remote
    def add(values: List[ray.ObjectRef[int]]) -> int:
        # although those values are not resolved, they have been
        # *fully executed and checkpointed*. This guarantees exactly-once
        # execution semantics.
        return sum(ray.get(values))

    @ray.remote
    def get_val() -> int:
        return 10

    ret = add.bind([get_val.bind() for _ in range(3)])
    assert workflow.run(ret) == 30

Passing object references between tasks
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Ray object references and data structures composed of them (e.g.,
``ray.Dataset``) can be passed into and returned from workflow tasks. To ensure
recoverability, their contents will be logged to durable storage before
executing. However, an object will not be checkpointed more than once, even if
it is passed to many different tasks.

.. code-block:: python

    @ray.remote
    def do_add(a, b):
        return a + b

    @ray.remote
    def add(a, b):
        return do_add.remote(a, b)

    workflow.run(add.bind(ray.put(10), ray.put(20))) == 30


Ray actor handles are not allowed to be passed between tasks.

Setting custom resources for tasks
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

You can assign resources (e.g., CPUs, GPUs to tasks via the same ``num_cpus``, ``num_gpus``, and ``resources`` arguments that Ray tasks take):

.. code-block:: python

    @ray.remote(num_gpus=1)
    def train_model() -> Model:
        pass  # This task is assigned to a GPU by Ray.

    workflow.run(train_model.bind())
