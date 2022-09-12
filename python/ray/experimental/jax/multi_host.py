import jax
import jax.numpy as jnp

import ray
from ray.train._internal.utils import get_address_and_port
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy

ray.init()

NUM_WORKERS = 1
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
        self.w = jnp.array([2.0, 3.0, 4.0])
        self.xs = jnp.arange(5 * n_devices).reshape(-1, 5)
        self.ws = jnp.stack([self.w] * n_devices)

    def convolution(self, x, w):
        output = []
        for i in range(1, len(x) - 1):
            output.append(jnp.dot(x[i - 1 : i + 2], w))
        return jnp.array(output)

    def convolution_vmap(self):
        return jax.vmap(self.convolution)(self.xs, self.ws)

    def convolution_pmap(self):
        # Replicated self.w via broadcasting
        return jax.pmap(self.convolution, in_axes=(0, None))(self.xs, self.w)

    def normalized_convolution(self, x, w):
        output = []
        for i in range(1, len(x)-1):
            output.append(jnp.dot(x[i-1:i+2], w))
        output = jnp.array(output)
        return output / jax.lax.psum(output, axis_name="p")

    def pmap_with_psum(self):
        return jax.pmap(lambda x: jax.lax.psum(x, "i"), axis_name="i")(self.xs)

    def pmap_normalized_convolution_with_psum(self):
        return jax.pmap(self.normalized_convolution, in_axes=(0, None), axis_name="p")(self.xs, self.w)

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

ray.get([w.init_data.remote() for w in workers])
# With psum, all GPU devices on host are visible to the worker actor
print(ray.get(workers[0].pmap_with_psum.remote()))
print(ray.get(workers[0].pmap_normalized_convolution_with_psum.remote()))
