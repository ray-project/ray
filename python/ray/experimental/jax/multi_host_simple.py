from typing import List

import jax
import jax.numpy as jnp
import numpy as np
from flax.jax_utils import replicate
from flax.training.common_utils import shard

import ray
from ray.train._internal.utils import get_address_and_port

ray.init()

NUM_WORKERS = 4
NUM_GPU_PER_WORKER = 4

@ray.remote(num_gpus=NUM_GPU_PER_WORKER)
class Worker:
    def __init__(self, rank):
        # Note: No jax calls should be made here before initializing jax.distributed
        self.rank = rank

    def init_jax_distributed_group(self, coordinator_address, world_size):
        print(
            f"initialize worker with rank: {self.rank}, "
            f"world_size: {world_size}, "
            f"coordinator_address: {coordinator_address}"
        )
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

    def init_data(self):
        n_devices = jax.local_device_count()
        # global_n_devices = jax.device_count()
        self.local_data_shard = jnp.ones(jax.local_device_count()) # (4, 3)
        # self.global_data_shard = jnp.array([self.rank, self.rank, self.rank] * global_n_devices).reshape(4, 4, 3)
        return f"hi: {self.rank}"

    # def set_buffer(self):
    #     pass 

    def local_pmap(self):
        return jax.pmap(lambda x: jax.lax.psum(x, 'i'), axis_name='i')(self.local_data_shard)

    def get_device_count(self):
        return f"Worker[{self.rank}]: {jax.device_count()}"

    def get_local_device_count(self):
        return f"Worker[{self.rank}]: {jax.local_device_count()}"

    def get_devices(self):
        return f"Worker[{self.rank}]: {str(jax.devices())}"


workers = [
    Worker.options(
        # scheduling_strategy=NodeAffinitySchedulingStrategy(
        #     node_id=ray.get_runtime_context().node_id,
        #     soft=False,
        # )
    ).remote(rank)
    for rank in range(NUM_WORKERS)
]
coordinator = workers[0]
coordinator_address = ray.get(coordinator.get_coordinator_address.remote())
ray.get(
    [
        w.init_jax_distributed_group.remote(coordinator_address, NUM_WORKERS)
        for w in workers
    ]
)

print(
    f"Device count: {ray.get([w.get_device_count.remote() for w in workers])} \n"
)
print(
    f"Local device count: {ray.get([w.get_local_device_count.remote() for w in workers])} \n"
)
print(f"Get devices: {ray.get(coordinator.get_devices.remote())} \n")

print(ray.get([w.init_data.remote() for w in workers]))
# # With psum, all GPU devices on host are visible to the worker actor
# print(ray.get([workers[0].convolution_pmap.remote(), workers[1].convolution_pmap.remote()]))
print(ray.get([w.local_pmap.remote() for w in workers]))
# print(ray.get(workers[0].pmap_normalized_convolution_with_psum.remote()))


