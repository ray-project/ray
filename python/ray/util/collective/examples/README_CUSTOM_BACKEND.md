# Custom Collective Backend Example for Ray

This example demonstrates how to create a custom collective backend for Ray using the `BackendRegistry` and `BaseGroup` classes.

## Quick Start

Based on the existing GLOO and NCCL backends, you can first check `gloo_allreduce_register_example.py` and `nccl_allreduce_register_example.py` to understand the basic usage methods.

## Creating Your Own Backend

### Step 1: Inherit from BaseGroup

```python
from ray.util.collective.collective_group.base_collective_group import BaseGroup

class MyCustomBackend(BaseGroup):
    def __init__(self, world_size, rank, group_name):
        super().__init__(world_size, rank, group_name)
```

### Step 2: Implement Required Methods

```python
    @classmethod
    def backend(cls):
        return "MY_BACKEND"

    @classmethod
    def check_backend_availability(cls) -> bool:
        # Check if your backend dependencies are available
        return True

    def broadcast(self, tensor, broadcast_options=BroadcastOptions()):
        # Implement broadcast logic
        pass

    def allreduce(self, tensor, allreduce_options=AllReduceOptions()):
        # Implement allreduce logic
        pass

    def barrier(self, barrier_options=BarrierOptions()):
        # Implement barrier logic
        pass

    # Implement other required methods (reduce, allgather, etc.)
```

### Step 3: Register Your Backend

```python
from ray.util.collective.backend_registry import register_collective_backend

register_collective_backend("MY_BACKEND", MyCustomBackend)
```

## UserCase: Use Your Own Backend with Ray

```python
import ray
from ray.util.collective import (
    allreduce,
    broadcast,
    create_collective_group,
    init_collective_group,
)
from ray.util.collective.backend_registry import register_collective_backend
from ray.util.collective.types import ReduceOp

# Import and register the mock backend
from mock_backend import MyCustomBackend
register_collective_backend("MOCK", MyCustomBackend)

# Initialize Ray
ray.init()

# Create actors
@ray.remote
class Worker:
    def __init__(self, rank):
        self.rank = rank

    def setup(self, world_size):
        # Register backend in each worker
        from mock_backend import MyCustomBackend
        register_collective_backend("MOCK", MyCustomBackend)

        init_collective_group(
            world_size=world_size,
            rank=self.rank,
            backend="MOCK",
            group_name="default",
        )

    def broadcast_test(self):
        if self.rank == 0:
            # Implement broadcast logic
            pass
        else:
            tensor = np.array([0.0, 0.0, 0.0], dtype=np.float32)

        broadcast(tensor, src_rank=0)
        return tensor.tolist()

# Create collective group
actors = [Worker.remote(rank=i) for i in range(3)]
create_collective_group(
    actors=actors,
    world_size=3,
    ranks=[0, 1, 2],
    backend="MOCK",
    group_name="default",
)

# Setup and test
ray.get([a.setup.remote(3) for a in actors])
results = ray.get([a.broadcast_test.remote() for a in actors])
print(f"Broadcast results: {results}")
```

## Implementation Details
We have designed a specific example code to facilitate your understanding. You can refer to it in `mock_kv_collective_group.py` for a custom backend while `mock_internal_kv_example.py` for a testing example.

The `MockInternalKVGroup` in `mock_kv_collective_group.py` demonstrates:

- **Broadcast**: Source rank writes data to internal_kv, other ranks read it
- **Allreduce**: Each rank writes data, then all ranks read and aggregate
- **Barrier**: Each rank signals arrival, waits for all ranks to arrive
- **Tensor serialization**: Handles both numpy arrays and torch tensors
- **Timeout handling**: Waits with timeout for synchronization

## API Reference

- `BackendRegistry`: Global registry for collective backends
  - `put(name, group_cls)`: Register a backend
  - `get(name)`: Get a registered backend class
  - `check(name)`: Check if backend is available

- `BaseGroup`: Abstract base class for collective backends
  - Required properties: `rank`, `world_size`, `group_name`
  - Required class methods: `backend()`, `check_backend_availability()`
  - Required instance methods: `broadcast()`, `allreduce()`, `barrier()`, etc.
