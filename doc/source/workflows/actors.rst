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

        def __getstate__(self):
            return self._val

        def __setstate__(self, val):
            self._val = val

    workflow.init()

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

Virtual actors must define ``__getstate__`` and ``__setstate__``, which will be called on each step to restore and save the actor.

We can retrieve the actor via its ``workflow_id`` in another process, to get the value:

.. code-block:: python

    counter = workflow.get_actor(workflow_id="counter")
    assert 30 == counter.value.run()

Readonly methods are not only lower overhead since they skip action logging, but can be executed concurrently with respect to mutating methods on the actor.

Launching sub-workflows from actor methods
------------------------------------------

**Note: This feature is not yet implemented.**

Receiving external events
-------------------------

**Note: This feature is not yet implemented.**
