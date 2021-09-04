Workflow Basics
===============

Get started with a single three-step workflow:

.. code-block:: python

    from ray import workflow
    from typing import List

    @workflow.step
    def read_data(num: int):
        return [i for i in range(num)]
        
    @workflow.step
    def preprocessing(data: List[float]) -> List[float]:
        return [d**2 for d in data]

    @workflow.step
    def aggregate(data: List[float]) -> float:
        return sum(data)

    # Initialize workflow storage.
    workflow.init()

    # Setup the workflow.
    data = read_data.step(10)
    preprocessed_data = preprocessing.step(data)
    output = aggregate.step(preprocessed_data)

    # Execute the workflow and print the result.
    print(output.run())

    # The workflow can also be executed asynchronously.
    # print(ray.get(output.run_async()))

Here we created the workflow:

.. image:: basic.png
   :width: 500px
   :align: center

Workflow steps behave similarly to Ray tasks. They are declared via the ``@workflow.step`` annotation, and can take in either concrete values or the output of another workflow step as an argument.

Unlike Ray tasks, which return an ``ObjectRef[T]`` when called and are executed eagerly, workflow steps return ``Workflow[T]`` and are not executed until ``.run()`` is called on a composed workflow DAG.

Composing workflow steps
------------------------

As seen in the example above, workflow steps can be composed by passing ``Workflow[T]`` outputs to other steps. Just like a Ray task, a step can take multiple inputs:

.. code-block:: python

    @workflow.step
    def add(left: int, right: int) -> int:
        return left + right
        
    @workflow.step
    def get_val() -> int:
        return 10

    ret = add.step(get_val1.step(), 20)
    assert ret.run() == 30

Here we can see though ``get_val1.step()`` returns a ``Workflow[int]``, when passed to the ``add`` step, the ``add`` function will see its resolved value.

Error handling
--------------

Normally, any step that raises an exception will cause the workflow to abort and enter FAILED state.

Workflows provides two ways to handle application-level exceptions: (1) automatic retry, and (2) the ability to catch and handle exceptions.

The following error handling flags can be either set in the step decorator or via ``.options()``:

.. code-block:: python

    @workflow.step
    def faulty_function() -> str:
        if random.random() > 0.5:
            raise RuntimeError("oops")
        return "OK"

    # Tries up to three times before giving up.
    r1 = faulty_function.options(max_retries=3).step()
    r1.run()

    @workflow.step
    def handle_errors(result: Tuple[str, Exception]):
        # The exception field will be None on success.
        err = result[1]
        if err:
            return "There was an error: {}".format(err)
        else:
            return "OK"

    # `handle_errors` receives a tuple of (result, exception).
    r2 = faulty_function.options(catch_exceptions=True).step()
    handle_errors.step(r2).run()

- If `max_retries` is given, the step will be retried for the given number of times if an exception is raised. It will only retry for the application level error. For system errors, it's controlled by ray.
- If `catch_exceptions` is True, the return value of the function will be converted to `Tuple[Optional[T], Optional[Exception]]`. This can be combined with ``max_retries`` to try a given number of times before returning the result tuple.

The parameters can also be passed to the decorator

.. code-block:: python

    @workflow.step(max_retries=3, catch_exceptions=True)
    def faulty_function():
        pass

Durability guarantees
---------------------

Workflow steps provide *exactly-once* execution semantics. What this means is that once the result of a workflow step is logged to durable storage, Ray guarantees the step will never be re-executed. A step that receives the output of another workflow step can be assured that its inputs steps will never be re-executed.

Failure model
~~~~~~~~~~~~~
- If the cluster fails, any workflows running on the cluster enter RESUMABLE state. The workflows can be resumed on another cluster (see the management API section).
- The lifetime of the workflow is not coupled with the driver. If the driver exits, the workflow will continue running in the background of the cluster.

Note that steps that have side-effects still need to be idempotent. This is because the step could always fail prior to its result being logged.

.. code-block:: python
    :caption: Non-idempotent workflow:

    @workflow.step
    def book_flight_unsafe() -> FlightTicket:
        ticket = service.book_flight()
        # Uh oh, what if we failed here?
        return ticket

    # UNSAFE: we could book multiple flight tickets
    book_flight_unsafe.step().run()

.. code-block:: python
    :caption: Idempotent workflow:

    @workflow.step
    def generate_id() -> str:
       # Generate a unique idempotency token.
       return uuid.uuid4().hex

    @workflow.step
    def book_flight_idempotent(request_id: int) -> FlightTicket:
       if service.has_ticket(request_id):
           # Retrieve the previously created ticket.
           return service.get_ticket(request_id)
       return service.book_flight(request_id)

    # SAFE: book_flight is written to be idempotent
    request_id = generate_id.step()
    book_flight_idempotent.step(request_id).run()

Dynamic workflows
-----------------

Additional steps can be dynamically created and inserted into the workflow DAG during execution. The following example shows how to implement the recursive ``factorial`` program using dynamically generated steps:

.. code-block:: python

    @workflow.step
    def factorial(n: int) -> int:
        if n == 1:
            return 1
        else:
            return multiply.step(n, factorial.step(n - 1))

    @workflow.step
    def multiply(a: int, b: int) -> int:
        return a * b

    ret = factorial.step(10).run()
    assert ret.run() == 3628800

The key behavior to note is that when a step returns a ``Workflow`` output instead of a concrete value, that workflow's output will be substituted for the step's return. To better understand dynamic workflows, let's look at a more realistic example of booking a trip:

.. code-block:: python

    @workflow.step
    def book_flight(...) -> Flight: ...

    @workflow.step
    def book_hotel(...) -> Hotel: ...

    @workflow.step
    def finalize_or_cancel(
        flights: List[Flight],
        hotels: List[Hotel]) -> Receipt: ...

    @workflow.step
    def book_trip(origin: str, dest: str, dates) -> 
            "Workflow[Receipt]":
        # Note that the workflow engine will not begin executing
        # child workflows until the parent step returns.
        # This avoids step overlap and ensures recoverability.
        f1: Workflow = book_flight.step(origin, dest, dates[0])
        f2: Workflow = book_flight.step(dest, origin, dates[1])
        hotel: Workflow = book_hotel.step(dest, dates)
        return finalize_or_cancel.step([f1, f2], [hotel])

    fut = book_trip.step("OAK", "SAN", ["6/12", "7/5"])
    fut.run()  # returns Receipt(...)

Here the workflow initially just consists of the ``book_trip`` step. Once executed, ``book_trip`` generates steps to book flights and hotels in parallel, which feeds into a step to decide whether to cancel the trip or finalize it. The DAG can be visualized as follows (note the dynamically generated nested workflows within ``book_trip``):

.. image:: trip.png
   :width: 500px
   :align: center

The execution order here will be:
1. Run the ``book_trip`` step.
2. Run the two ``book_flight`` steps and the ``book_hotel``  step in parallel.
3. Once all three booking steps finish, ``finalize_or_cancel`` will be executed and its return will be the output of the workflow.

Ray Integration
---------------

Mixing steps with Ray tasks and actors
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Workflows are compatible with Ray tasks and actors. There are two methods of using them together:

1. Workflows can be launched from within a Ray task or actor. For example, you can launch a long-running workflow from Ray serve in response to a user request. This is no different from launching a workflow from the driver program.
2. Workflow steps can use Ray tasks or actors within a single step. For example, a step could use RaySGD internally to train a model. No durability guarantees apply to the tasks or actors used within the step; if the step fails, it will be re-executed from scratch.

Passing nested arguments
~~~~~~~~~~~~~~~~~~~~~~~~
Unlike Ray tasks, when you pass a list of ``Workflow`` outputs to a step, the values are fully resolved. This ensures that all a step's ancestors are fully executed prior to the step starting:

.. code-block:: python

    @workflow.step
    def add(values: List[int]) -> int:
        return sum(values)
        
    @workflow.step
    def get_val() -> int:
        return 10

    ret = add.step([get_val.step() for _ in range(3)])
    assert ret.run() == 30

Passing object references between steps
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Ray object references and data structures composed of them (e.g., ``ray.Dataset``) can be passed into and returned from workflow steps. To ensure recoverability, their contents will be logged to durable storage. However, an object will not be checkpointed more than once, even if it is passed to many different steps.

Ray actor handles are not allowed to be passed between steps.

Setting custom resources for steps
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

You can assign resources (e.g., CPUs, GPUs to steps via the same ``num_cpus``, ``num_gpus``, and ``resources`` arguments that Ray tasks take):

.. code-block:: python

    @workflow.step(num_gpus=1)
    def train_model() -> Model:
        pass  # This step is assigned a GPU by Ray.

    train_model.step().run()
