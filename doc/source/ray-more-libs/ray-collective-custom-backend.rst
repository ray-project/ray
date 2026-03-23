.. _ray-collective-custom-backend:

Custom Collective Backends
==========================

This guide shows how to create and use custom collective backends with Ray.

Overview
--------

Ray collective operations support custom backends through a registration API. You can implement your own backend by:

1. Creating a class that inherits from ``BaseGroup``
2. Implementing required collective operations
3. Registering your backend with ``register_collective_backend``

Creating a Custom Backend
-------------------------

Step 1: Define Your Backend Class
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Your backend class must inherit from ``BaseGroup`` and implement required methods. See the ``BaseGroup`` API reference for the complete list of required methods.

.. code-block:: python

    from ray.util.collective.collective_group.base_collective_group import BaseGroup

    class MyCustomBackend(BaseGroup):
        def __init__(self, world_size, rank, group_name):
            super().__init__(world_size, rank, group_name)

        @classmethod
        def backend(cls):
            return "MY_BACKEND"

        @classmethod
        def check_backend_availability(cls) -> bool:
            return True

        def allreduce(self, tensor, allreduce_options=None):
            pass

        def broadcast(self, tensor, broadcast_options=None):
            pass

        def barrier(self, barrier_options=None):
            pass

Step 2: Register Your Backend
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: python

    from ray.util.collective.backend_registry import register_collective_backend

    register_collective_backend("MY_BACKEND", MyCustomBackend)

Important: Registration Requirements
------------------------------------

**Your backend must be registered on both the driver and all actors before using collective operations.**

This is because each process (driver and each actor) needs to know about your backend class to instantiate it.

Two Ways to Initialize Collective Groups
----------------------------------------

There are **two distinct approaches** to initialize collective groups. **Choose one approach for your use case - do not mix them for the same group.**

.. note::

    Using both ``create_collective_group`` and ``init_collective_group`` together for the same group is incorrect and will cause errors.

Approach 1: Driver-Managed (Recommended)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Use ``create_collective_group`` on the driver to declare the group. Workers do **not** call ``init_collective_group`` - the group is automatically initialized when workers call collective operations.

.. code-block:: python

    import ray
    import numpy as np
    from ray.util.collective import allreduce, broadcast, create_collective_group
    from ray.util.collective.backend_registry import register_collective_backend
    from ray.util.collective.types import ReduceOp

    # Import your custom backend
    from my_backend import MyCustomBackend

    # Register on driver
    register_collective_backend("MY_BACKEND", MyCustomBackend)

    ray.init()

    @ray.remote
    class Worker:
        def __init__(self, rank):
            self.rank = rank

        def setup(self):
            # IMPORTANT: Register on each worker too
            from ray.util.collective.backend_registry import register_collective_backend
            from my_backend import MyCustomBackend
            register_collective_backend("MY_BACKEND", MyCustomBackend)
            # Do NOT call init_collective_group here!

        def compute(self):
            tensor = np.array([float(self.rank + 1)], dtype=np.float32)
            allreduce(tensor, op=ReduceOp.SUM)
            return tensor.item()

    # Create workers
    actors = [Worker.remote(rank=i) for i in range(3)]

    # Declare collective group from driver (creates info actor)
    create_collective_group(
        actors=actors,
        world_size=3,
        ranks=[0, 1, 2],
        backend="MY_BACKEND",
        group_name="default",
    )

    # Setup each worker (only registers backend, no init_collective_group)
    ray.get([a.setup.remote() for a in actors])

    # Run computation - group is auto-initialized on first collective call
    results = ray.get([a.compute.remote() for a in actors])
    print(f"Results: {results}")

    ray.shutdown()

Approach 2: Worker-Managed
^^^^^^^^^^^^^^^^^^^^^^^^^^

Each worker explicitly calls ``init_collective_group`` to initialize its group membership. The driver does **not** call ``create_collective_group``.

.. code-block:: python

    import ray
    import numpy as np
    from ray.util.collective import allreduce, broadcast, init_collective_group
    from ray.util.collective.backend_registry import register_collective_backend
    from ray.util.collective.types import ReduceOp

    # Import your custom backend
    from my_backend import MyCustomBackend

    ray.init()

    @ray.remote
    class Worker:
        def __init__(self, rank):
            self.rank = rank

        def setup(self, world_size):
            # Register backend
            from ray.util.collective.backend_registry import register_collective_backend
            from my_backend import MyCustomBackend
            register_collective_backend("MY_BACKEND", MyCustomBackend)

            # Explicitly initialize group membership
            init_collective_group(
                world_size=world_size,
                rank=self.rank,
                backend="MY_BACKEND",
                group_name="default",
            )

        def compute(self):
            tensor = np.array([float(self.rank + 1)], dtype=np.float32)
            allreduce(tensor, op=ReduceOp.SUM)
            return tensor.item()

    # Create workers
    actors = [Worker.remote(rank=i) for i in range(3)]

    # Do NOT call create_collective_group here - workers handle init themselves

    # Setup each worker (registers backend and initializes group)
    ray.get([a.setup.remote(3) for a in actors])

    # Run computation
    results = ray.get([a.compute.remote() for a in actors])
    print(f"Results: {results}")

    ray.shutdown()

Comparison Table
^^^^^^^^^^^^^^^^

+-------------------+-----------------------------+------------------------------+
| Aspect            | Driver-Managed (Approach 1) | Worker-Managed (Approach 2)  |
+===================+=============================+==============================+
| Driver calls      | ``create_collective_group`` | Nothing                      |
+-------------------+-----------------------------+------------------------------+
| Worker calls      | ``register_collective_``    | ``register_collective_``     |
|                   | ``backend`` only            | ``backend`` +                |
|                   |                             | ``init_collective_group``    |
+-------------------+-----------------------------+------------------------------+
| Group init        | Automatic (on first         | Explicit (in worker setup)   |
|                   | collective operation)       |                              |
+-------------------+-----------------------------+------------------------------+
| Use case          | Declarative, centralized    | Fine-grained control over    |
|                   | management                  | initialization               |
+-------------------+-----------------------------+------------------------------+

Complete Example
----------------

See ``python/ray/util/collective/examples/mock_internal_kv_example.py`` for a complete working example that demonstrates both approaches:

- ``test_mock_backend_create_group()`` - Driver-managed approach
- ``test_mock_backend_init_group()`` - Worker-managed approach

Doctest Example
---------------

The following demonstrates the driver-managed approach:

.. doctest::

    >>> import ray
    >>> import numpy as np
    >>> from ray.util.collective import allreduce, create_collective_group
    >>> from ray.util.collective.backend_registry import register_collective_backend
    >>> from ray.util.collective.types import ReduceOp
    >>> from ray.util.collective.examples.mock_internal_kv_example import MockInternalKVGroup
    >>> register_collective_backend("MOCK", MockInternalKVGroup)
    >>> ray.init()
    <Ray ... ...>
    >>> @ray.remote
    ... class Worker:
    ...     def __init__(self, rank):
    ...         self.rank = rank
    ...     def setup(self):
    ...         from ray.util.collective.backend_registry import register_collective_backend
    ...         from ray.util.collective.examples.mock_internal_kv_example import MockInternalKVGroup
    ...         register_collective_backend("MOCK", MockInternalKVGroup)
    ...         # Do NOT call init_collective_group - using driver-managed approach
    ...     def compute(self):
    ...         tensor = np.array([float(self.rank + 1)], dtype=np.float32)
    ...         allreduce(tensor, op=ReduceOp.SUM)
    ...         return tensor.item()
    >>> actors = [Worker.remote(rank=i) for i in range(2)]
    >>> create_collective_group(
    ...     actors=actors,
    ...     world_size=2,
    ...     ranks=[0, 1],
    ...     backend="MOCK",
    ...     group_name="default",
    ... )
    >>> ray.get([a.setup.remote() for a in actors])
    [None, None]
    >>> results = ray.get([a.compute.remote() for a in actors])
    >>> results
    [3.0, 3.0]
    >>> ray.shutdown()

See Also
--------

- Check ``mock_internal_kv_example.py`` for a complete working example demonstrating both approaches
- See the ``BaseGroup`` class in the API reference for all required methods
- See the ``register_collective_backend`` function in the API reference for registration details