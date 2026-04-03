.. _ray-collective-custom-backend:

Custom Collective Backends
==========================

This guide shows how to create and use custom collective backends with Ray.

Overview
--------

Ray collective operations support custom backends through a registration API. You can implement your own backend by:

1. Creating a class that inherits from :class:`~ray.util.collective.collective_group.base_collective_group.BaseGroup`
2. Implementing required collective operations
3. Registering your backend with :func:`~ray.util.collective.backend_registry.register_collective_backend`

Creating a Custom Backend
-------------------------

Step 1: Define Your Backend Class
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Your backend class must inherit from :class:`~ray.util.collective.collective_group.base_collective_group.BaseGroup` and implement required methods. See the :class:`~ray.util.collective.collective_group.base_collective_group.BaseGroup` API reference for the complete list of required methods.

Here's an example using the ``MockInternalKVGroup`` backend that uses Ray's internal KV store for communication:

.. testcode::
    :skipif: True

    from ray.util.collective.examples.mock_internal_kv_example import MockInternalKVGroup

    # MockInternalKVGroup is a complete implementation that inherits from BaseGroup
    # and implements all required collective operations using Ray's internal KV store
    backend_cls = MockInternalKVGroup

    # Check that it has the required methods
    assert hasattr(backend_cls, 'backend')
    assert hasattr(backend_cls, 'check_backend_availability')
    assert hasattr(backend_cls, 'allreduce')
    assert hasattr(backend_cls, 'broadcast')
    assert hasattr(backend_cls, 'barrier')

Step 2: Register Your Backend
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Next, register your collective backend on the driver and the actors participating in the collective group with :func:`~ray.util.collective.backend_registry.register_collective_backend`.

.. code-block:: python

    from ray.util.collective.backend_registry import register_collective_backend

    register_collective_backend("MY_BACKEND", MyCustomBackend)

.. note::

    **Your backend must be registered on both the driver and all actors before using collective operations.** This is because each process (driver and each actor) needs to know about your backend class to instantiate it.

Initializing Collective Groups
-------------------------------

There are **two distinct approaches** to initialize collective groups. **Choose one approach for your use case - do not mix them for the same group.**

.. note::

    Using both :func:`~ray.util.collective.collective.create_collective_group` and :func:`~ray.util.collective.collective.init_collective_group` together for the same group is not supported and will cause errors.

Approach 1: Driver-Managed (Recommended)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Use :func:`~ray.util.collective.collective.create_collective_group` on the driver to declare the group. Workers do **not** call :func:`~ray.util.collective.collective.init_collective_group` - the group is automatically initialized when workers call collective operations.

This approach is recommended because it provides a declarative, centralized way to manage collective groups. The driver has full visibility into all participants and can coordinate the initialization process.

.. code-block:: python

    import ray
    import numpy as np
    from ray.util.collective import allreduce, create_collective_group
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

    # Declare collective group from driver
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

Each worker explicitly calls :func:`~ray.util.collective.collective.init_collective_group` to initialize its group membership. The driver does **not** call :func:`~ray.util.collective.collective.create_collective_group`.

This approach provides more control over initialization timing within each worker, which can be useful for advanced scenarios where workers need to perform custom setup before or after group initialization.

.. code-block:: python

    import ray
    import numpy as np
    from ray.util.collective import allreduce, init_collective_group
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
| Use case          | Declarative, centralized    | More control over            |
|                   | management                  | initialization timing        |
+-------------------+-----------------------------+------------------------------+

Complete Example: Mock Backend using Internal KV
------------------------------------------------

The following example demonstrates a complete custom backend implementation using Ray's internal KV store for communication. This ``MockInternalKVGroup`` backend is useful for testing and understanding how custom backends work.

.. testcode::
    :skipif: True

    import ray
    import numpy as np
    from ray.util.collective import allreduce, create_collective_group
    from ray.util.collective.backend_registry import register_collective_backend
    from ray.util.collective.types import ReduceOp
    from ray.util.collective.examples.mock_internal_kv_example import MockInternalKVGroup

    # Register the mock backend
    register_collective_backend("MOCK", MockInternalKVGroup)

    ray.init()

    @ray.remote
    class Worker:
        def __init__(self, rank):
            self.rank = rank

        def setup(self):
            # Register backend on each worker
            register_collective_backend("MOCK", MockInternalKVGroup)

        def compute(self):
            tensor = np.array([float(self.rank + 1)], dtype=np.float32)
            allreduce(tensor, op=ReduceOp.SUM)
            return tensor.item()

    # Create workers
    actors = [Worker.remote(rank=i) for i in range(2)]

    # Create collective group from driver
    create_collective_group(
        actors=actors,
        world_size=2,
        ranks=[0, 1],
        backend="MOCK",
        group_name="default",
    )

    # Setup workers
    ray.get([a.setup.remote() for a in actors])

    # Run computation
    results = ray.get([a.compute.remote() for a in actors])
    print(f"Results: {results}")  # [3.0, 3.0]

    ray.shutdown()

.. testoutput::
    :skipif: True

    Results: [3.0, 3.0]

See Also
--------

- :class:`~ray.util.collective.collective_group.base_collective_group.BaseGroup` - Base class for custom backends
- :func:`~ray.util.collective.backend_registry.register_collective_backend` - Register a custom backend
- :func:`~ray.util.collective.collective.create_collective_group` - Create a collective group (driver-managed)
- :func:`~ray.util.collective.collective.init_collective_group` - Initialize a collective group (worker-managed)
