Virtual Actors
==============

Introduction
------------

Workflows also provides a *virtual actors* abstraction, which can be thought of as syntactic sugar on top of a dynamic workflow. Virtual actors are like Ray actors, but backed by durable storage instead of a running process. You can also launch sub-workflows from the methods of each virtual actor (e.g., train models in parallel). Here is a basic example:

.. code-block:: python

    from ray import workflow
    import ray

    @workflow.virtual_actor
    class Counter:
        def __init__(self, init_val):
            self._val = init_val

        def incr(self, val=1):
            self._val += val
            print(self._val)

        @workflow.virtual_actor.readonly
        def value(self):
            return self._val

    # Initialize a Counter actor with id="my_counter".
    counter = Counter.get_or_create("my_counter", 0)

    # Similar to workflow steps, actor methods support:
    # - `run()`, which will return the value
    # - `run_async()`, which will return a ObjectRef
    counter.incr.run(10)
    assert counter.value.run() == 10

    # Non-blocking execution.
    counter.incr.run_async(10)
    counter.incr.run(10)
    assert 30 == ray.get(counter.value.run_async())

In the code above, we define a ``Counter`` virtual actor. When the ``Counter`` is created, its class definition and initial state is logged into storage as a dynamic workflow with ``workflow_id="my_counter"``. When actor methods are called, new steps  are dynamically appended to the workflow and executed, returning the new actor state and result.

``__dict__`` in virtual actors must be able to json serializable, otherwise ``__getstate__`` and ``__setstate__`` must be defined, which will be called on each step to restore and save the actor.

We can retrieve the actor via its ``workflow_id`` in another process, to get the value:

.. code-block:: python

    counter = workflow.get_actor(workflow_id="counter")
    assert 30 == counter.value.run()

Readonly methods are not only lower overhead since they skip action logging, but can be executed concurrently with respect to mutating methods on the actor.

Launching sub-workflows from actor methods
------------------------------------------

Inside virtual actor methods, sub-workflow involving other methods of the virtual actor can be launched. These sub-workflows can also include workflow steps defined outside the actor class, for example:

.. code-block:: python

    @workflow.step
    def double(s):
        return 2 * s

    @workflow.virtual_actor
    class Actor:
        def __init__(self):
            self.val = 1

        def double(self, update):
            step = double.step(self.val)
            if not update:
                # inside the method, a workflow can be launched
                return step
            else:
                # workflow can also be passed to anthoer method
                return self.update.step(step)

        def update(self, v):
            self.val = v
            return self.val


    handler = Actor.get_or_create("actor")
    assert handler.double.run(False) == 2
    assert handler.double.run(False) == 2
    assert handler.double.run(True) == 2
    assert handler.double.run(True) == 4

Actor method ordering
---------------------

Workflow virtual actors provide similar ordering guarantees as Ray actors: the methods will be executed in the same order as they are submitted, provided they are submitted from the same thread. This applies both to ``.run()`` (trivially true) and ``.run_async()```, and is also guaranteed to hold under cluster failures. Hence, you can use actor methods as a short-lived queue of work to process for the actor.

When an actor method launches a sub-workflow, that entire sub-workflow will be run as part of the actor method step. This means all steps of the sub-workflow will be guaranteed to complete before any other queued actor method calls are run. However, note that the sub-workflow is not transactional, that is, read-only methods can read intermediate actor state written by steps of the sub-workflow.

Long-lived sub-workflows
------------------------

We do not recommend running long-lived workflows as sub-workflows of a virtual actor. This is because sub-workflows block future actor methods calls from executing while they are running. Instead, you can launch a *separate* workflow and track its execution using workflow API methods. By generating the workflow id deterministically (ensuring idempotency), no duplicate workflows will be launched even if there is a failure.

.. code-block:: python
    :caption: Long-lived sub-workflow (bad).

    @workflow.virtual_actor
    class ShoppingCart:
        ...
        # BAD: blocks until shipping completes, which could be
        # slow. Until that workflow finishes, no mutating methods
        # can be called on this actor.
        def do_checkout():
            # Run shipping workflow as sub-workflow of this method.
            return ship_items.step(self.items)

.. code-block:: python
    :caption: Launching separate workflows (good).

    @workflow.virtual_actor
    class ShoppingCart:
        ...
        # GOOD: the checkout method is non-blocking, and the shipment
        # status can be monitored via ``self.shipment_workflow_id``.
        def do_checkout():
            # Deterministically generate a workflow id for idempotency.
            self.shipment_workflow_id = "ship_{}".format(self.order_id)
            # Run shipping workflow as a separate async workflow.
            ship_items.step(self.items).run_async(
                workflow_id=self.shipment_workflow_id)

Receiving external events
-------------------------

**Note: This feature is not yet implemented.**
