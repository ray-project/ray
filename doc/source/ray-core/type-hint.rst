Type Hints in Ray
==================

Ray provides comprehensive support for Python type hints with both remote functions and actors.
This enables better IDE support, static type checking, and improved code maintainability in distributed Ray applications.

Overview
--------

In most cases, Ray applications can use type hints without any modifications to existing code.
Ray automatically handles type inference for standard remote functions and basic actor usage patterns.

However, certain patterns require specific approaches to ensure proper type annotation,
particularly when working with actor references.

Remote Functions
----------------

Ray remote functions support standard Python type annotations without additional configuration.
The ``@ray.remote`` decorator preserves the original function signature and type information.

.. code-block:: python

    import ray

    @ray.remote
    def add_numbers(x: int, y: int) -> int:
        return x + y

    # Type hints work seamlessly with remote function calls
    result_ref = add_numbers.remote(5, 3)
    result: int = ray.get(result_ref)

For remote class methods, type annotations work naturally within the class definition.
However, to ensure proper type inference, actor methods should use the ``@ray.method`` decorator:

.. code-block:: python

    import ray

    @ray.remote
    class Counter:
        def __init__(self):
            self.value = 0

        @ray.method
        def increment(self, amount: int) -> int:
            self.value += amount
            return self.value

    # Usage maintains type safety
    counter = Counter.remote()
    result_ref = counter.increment.remote(5)
    result: int = ray.get(result_ref)

Actor Type Annotations
----------------------

When passing Ray actors as arguments to remote functions, special consideration is needed for proper type annotation.
The challenge arises because the ``@ray.remote`` decorator transforms classes, making direct type annotation problematic.

Problematic Approach
~~~~~~~~~~~~~~~~~~~~

The following approach appears intuitive but creates type checking issues:

.. code-block:: python

    import ray

    @ray.remote
    class Counter:
        def __init__(self):
            self.value = 0

        @ray.method
        def increment(self, amount: int) -> int:
            self.value += amount
            return self.value

    @ray.remote
    def use_counter(counter: Counter) -> int:  # Type checker will complain
        return ray.get(counter.increment.remote(5))

This approach fails because ``Counter`` after decoration refers to an actor class,
not the original class type, causing IDE and static type checkers to report errors.

Recommended Solution
~~~~~~~~~~~~~~~~~~~~

To properly annotate actor types, separate the class definition from the remote declaration
and use ``ray.actor.ActorProxy`` for type hints:

.. code-block:: python

    import ray
    from ray.actor import ActorProxy

    class Counter:
        def __init__(self):
            self.value = 0

        @ray.method
        def increment(self, amount: int) -> int:
            self.value += amount
            return self.value

    # Create the remote actor class separately
    CounterActor = ray.remote(Counter)

    @ray.remote
    def use_counter(counter: ActorProxy[Counter]) -> int:
        return ray.get(counter.increment.remote(5))

    # Usage example
    counter_actor = CounterActor.remote()
    result_ref = use_counter.remote(counter_actor)
    result: int = ray.get(result_ref)

The ``@ray.method`` decorator is essential for actor methods as it enables Ray to properly
infer method types and maintain type safety across remote calls.
