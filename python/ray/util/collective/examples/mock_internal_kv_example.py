import numpy as np

import ray
from ray.util.collective import (
    allreduce,
    broadcast,
    create_collective_group,
    init_collective_group,
)
from ray.util.collective.backend_registry import register_collective_backend

# Import mock backend
from ray.util.collective.examples.mock_internal_kv_collective_group import (
    MockInternalKVGroup,
)
from ray.util.collective.types import Backend, ReduceOp


def test_mock_backend():
    """Test with Ray actors."""

    # Initialize Ray
    ray.init()
    # Register mock backend
    register_collective_backend("MOCK", MockInternalKVGroup)

    @ray.remote
    class Worker:
        def __init__(self, rank):
            self.rank = rank

        def setup(self, world_size):
            """Initialize collective group for this worker."""
            from ray.util.collective.backend_registry import register_collective_backend
            from ray.util.collective.examples.mock_internal_kv_collective_group import (
                MockInternalKVGroup,
            )
            from ray.util.collective.types import Backend

            # Register backend in each worker process
            register_collective_backend("MOCK", MockInternalKVGroup)

            init_collective_group(
                world_size=world_size,
                rank=self.rank,
                backend=Backend.MOCK,
                group_name="default",
            )

        def broadcast_test(self):
            """Test broadcast operation."""
            if self.rank == 0:
                tensor = np.array([42.0, 43.0, 44.0], dtype=np.float32)
            else:
                tensor = np.array([0.0, 0.0, 0.0], dtype=np.float32)

            broadcast(tensor, src_rank=0)
            return tensor.tolist()

        def allreduce_test(self):
            """Test allreduce operation."""
            tensor = np.array([float(self.rank + 1)], dtype=np.float32)
            allreduce(tensor, op=ReduceOp.SUM)
            return tensor.item()

    # Create 3 workers
    actors = [Worker.remote(rank=i) for i in range(3)]

    # Create collective group
    create_collective_group(
        actors=actors,
        world_size=3,
        ranks=[0, 1, 2],
        backend=Backend.MOCK,
        group_name="default",
    )

    # Setup each worker
    ray.get([a.setup.remote(3) for a in actors])

    # Test 1: Broadcast
    results = ray.get([a.broadcast_test.remote() for a in actors])
    expected = [[42.0, 43.0, 44.0]] * 3
    if results == expected:
        print("Broadcast test passed!")
    else:
        print(f"Broadcast test failed! Expected {expected}, got {results}")

    # Test 2: AllReduce
    results = ray.get([a.allreduce_test.remote() for a in actors])

    if results == [6.0, 6.0, 6.0]:
        print("AllReduce test passed!")
    else:
        print(f"AllReduce test failed! Expected [6.0, 6.0, 6.0], got {results}")

    # Cleanup
    ray.shutdown()
    print("All tests completed!")


if __name__ == "__main__":
    test_mock_backend()
