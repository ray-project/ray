.. _ray-collective-custom-backend:

Custom Collective Backends
========================

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
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: python

    from ray.util.collective.backend_registry import register_collective_backend

    register_collective_backend("MY_BACKEND", MyCustomBackend)

Important: Registration Requirements
----------------------------------

**Your backend must be registered on both the driver and all actors before creating collective groups.**

This is because each process (driver and each actor) needs to know about your backend class to instantiate it.

**Note on create_collective_group vs init_collective_group:**

There are two ways to initialize collective groups:

1. **Declarative approach** (recommended): Use ``create_collective_group`` on the driver to declare the group, then call ``init_collective_group`` in each worker. This is shown in the example above.

2. **Imperative approach**: Call ``init_collective_group`` directly in each worker without using ``create_collective_group`` on the driver.

Choose one approach for your use case, don't mix them for the same group.

Example: Using a Custom Backend
-------------------------------

Here's a complete example using mock backend from ``mock_internal_kv_example.py``:

.. code-block:: python

    import ray
    import numpy as np
    from ray.util.collective import (
        allreduce,
        broadcast,
        create_collective_group,
        init_collective_group,
    )
    from ray.util.collective.backend_registry import register_collective_backend
    from ray.util.collective.types import Backend, ReduceOp

    # Import of mock backend
    from ray.util.collective.examples.mock_internal_kv_example import MockInternalKVGroup

    # Register on of driver
    register_collective_backend("MOCK", MockInternalKVGroup)

    ray.init()

    @ray.remote
    class Worker:
        def __init__(self, rank):
            self.rank = rank

        def setup(self, world_size):
            # IMPORTANT: Register on each worker too
            from ray.util.collective.backend_registry import register_collective_backend
            from ray.util.collective.examples.mock_internal_kv_example import MockInternalKVGroup
            from ray.util.collective.types import Backend

            register_collective_backend("MOCK", MockInternalKVGroup)

            init_collective_group(
                world_size=world_size,
                rank=self.rank,
                backend=Backend.MOCK,
                group_name="default",
            )

        def compute(self):
            tensor = np.array([float(self.rank + 1)], dtype=np.float32)
            allreduce(tensor, op=ReduceOp.SUM)
            return tensor.item()

    # Create workers
    actors = [Worker.remote(rank=i) for i in range(3)]

    # Create collective group from driver
    create_collective_group(
        actors=actors,
        world_size=3,
        ranks=[0, 1, 2],
        backend=Backend.MOCK,
        group_name="default",
    )

    # Setup each worker
    ray.get([a.setup.remote(3) for a in actors])

    # Run computation
    results = ray.get([a.compute.remote() for a in actors])
    print(f"Results: {results}")  # Should be [6.0, 6.0, 6.0]

    ray.shutdown()

Doctest Example
--------------

The following is a simplified doctest that demonstrates basic functionality:

.. doctest::

    >>> import ray
    >>> import numpy as np
    >>> from ray.util.collective import (
    ...     allreduce,
    ...     create_collective_group,
    ...     init_collective_group,
    ...     )
    >>> from ray.util.collective.backend_registry import register_collective_backend
    >>> from ray.util.collective.types import Backend, ReduceOp
    >>> from ray.util.collective.examples.mock_internal_kv_example import MockInternalKVGroup
    >>> register_collective_backend("MOCK", MockInternalKVGroup)
    >>> ray.init()
    <Ray ... ...>
    >>> @ray.remote
    ... class Worker:
    ...     def __init__(self, rank):
    ...         self.rank = rank
    ...     def setup(self, world_size):
    ...         from ray.util.collective.backend_registry import register_collective_backend
    ...         from ray.util.collective.examples.mock_internal_kv_example import MockInternalKVGroup
    ...         from ray.util.collective.types import Backend
    ...         register_collective_backend("MOCK", MockInternalKVGroup)
    ...         init_collective_group(
    ...             world_size=world_size,
    ...             rank=self.rank,
    ...             backend=Backend.MOCK,
    ...             group_name="default",
    ...         )
    ...     def compute(self):
    ...         tensor = np.array([float(self.rank + 1)], dtype=np.float32)
    ...         allreduce(tensor, op=ReduceOp.SUM)
    ...         return tensor.item()
    >>> actors = [Worker.remote(rank=i) for i in range(2)]
    >>> create_collective_group(
    ...     actors=actors,
    ...     world_size=2,
    ...     ranks=[0, 1],
    ...     backend=Backend.MOCK,
    ...     group_name="default",
    ... )
    >>> ray.get([a.setup.remote(2) for a in actors])
    [None, None]
    >>> results = ray.get([a.compute.remote() for a in actors])
    >>> results
    [3.0, 3.0]
    >>> ray.shutdown()

See Also
--------

- Check ``mock_internal_kv_example.py`` for a complete working example
- See the ``BaseGroup`` class in the API reference for all required methods
- See the ``register_collective_backend``udi function in the API reference for registration details
