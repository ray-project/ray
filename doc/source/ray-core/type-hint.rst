Type Hints in Ray
==================

Ray provides comprehensive support for Python type hints with both remote functions and actors.
This enables better IDE support, static type checking, and improved code maintainability in distributed Ray applications.

Overview
--------

In most cases, Ray applications can use type hints without any modifications to existing code.
Ray automatically handles type inference for standard remote functions and basic actor usage patterns.

However, certain advanced patterns require specific approaches to ensure proper type annotation,
particularly when working with actor references and complex distributed workflows.

Remote Functions
----------------

Ray remote functions support standard Python type annotations without additional configuration.
The ``@ray.remote`` decorator preserves the original function signature and type information.

.. code-block:: python

    import ray

    @ray.remote
    def process_data(data: list[int], multiplier: float) -> list[float]:
        return [x * multiplier for x in data]

    # Type hints work seamlessly with remote function calls
    result_ref = process_data.remote([1, 2, 3], 2.5)
    result: list[float] = ray.get(result_ref)

For remote class methods, type annotations work naturally within the class definition:

.. code-block:: python

    import ray

    @ray.remote
    class DataProcessor:
        def __init__(self, config: dict):
            self.config = config

        def process(self, data: list[int]) -> list[int]:
            return [x * self.config.get("multiplier", 1) for x in data]

    # Usage maintains type safety
    processor = DataProcessor.remote({"multiplier": 2})
    result_ref = processor.process.remote([1, 2, 3])
    result: list[int] = ray.get(result_ref)

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
    class DataProcessor:
        def __init__(self):
            self.data = []

        def add_data(self, item: int) -> None:
            self.data.append(item)

        def get_sum(self) -> int:
            return sum(self.data)

    @ray.remote
    def use_processor(processor: DataProcessor) -> int:  # Type checker will complain
        processor.add_data.remote(5)
        return ray.get(processor.get_sum.remote())

This approach fails because ``DataProcessor`` after decoration refers to an actor class,
not the original class type, causing IDE and static type checkers to report errors.

Recommended Solution
~~~~~~~~~~~~~~~~~~~~

To properly annotate actor types, separate the class definition from the remote declaration
and use ``ray.actor.ActorProxy`` for type hints:

.. code-block:: python

    import ray
    from ray.actor import ActorProxy

    class DataProcessor:
        def __init__(self):
            self.data = []

        def add_data(self, item: int) -> None:
            self.data.append(item)

        def get_sum(self) -> int:
            return sum(self.data)

    # Create the remote actor class separately
    DataProcessorActor = ray.remote(DataProcessor)

    @ray.remote
    def use_processor(processor: ActorProxy[DataProcessor]) -> int:
        processor.add_data.remote(5)
        return ray.get(processor.get_sum.remote())

    # Usage example
    processor_actor = DataProcessorActor.remote()
    result_ref = use_processor.remote(processor_actor)
    result: int = ray.get(result_ref)
