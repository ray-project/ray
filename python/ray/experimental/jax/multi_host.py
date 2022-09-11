import jax

import ray
from ray.train._internal.utils import get_address_and_port

ray.init()

NUM_WORKERS = 16

@ray.remote(num_gpus=1)
class Worker:
    def __init__(self, rank):
        import jax
        # Note: No jax calls should be made here before initializing jax.distributed
        self.rank = rank

    def init_jax_distributed_group(
        self, coordinator_address, world_size
    ):
        print(f"initialize worker with rank: {self.rank}, world_size: {world_size}, coordinator_address: {coordinator_address}")
        jax.distributed.initialize(
            coordinator_address=coordinator_address,
            num_processes=world_size,
            process_id=self.rank,
        )
        return True

    def get_coordinator_address(self) -> str:
        assert self.rank == 0, "Should only use rank 0 to get address and port"
        address, port = get_address_and_port()
        return f"{address}:{port}"

    def put_x(self):
        self.x = jax.device_put(self.rank)

    def get_x(self):
        return self.x

    def get_device_count(self):
        return f"Worker[{self.rank}]: {jax.device_count()}"

    def get_local_device_count(self):
        return f"Worker[{self.rank}]: {jax.local_device_count()}"

    def get_devices(self):
        return f"Worker[{self.rank}]: {str(jax.devices())}"

workers = [
    Worker.remote(rank)
    for rank in range(NUM_WORKERS)
]
coordinator = workers[0]
coordinator_address = ray.get(coordinator.get_coordinator_address.remote())
print(ray.get([w.init_jax_distributed_group.remote(coordinator_address, NUM_WORKERS) for w in workers]))
print(f"Device count: {ray.get([w.get_device_count.remote() for w in workers])}")
print(f"Local device count: {ray.get([w.get_local_device_count.remote() for w in workers])}")
print(f"Get devices: {ray.get(coordinator.get_devices.remote())}")