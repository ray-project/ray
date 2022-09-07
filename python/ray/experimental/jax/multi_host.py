import jax

import ray
from ray.train._internal.utils import get_address_and_port
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy

NUM_WORKERS = 2

address, port = get_address_and_port()
coordinator_address = f"{address}:{port}"


@ray.remote(num_cpus=0)
class Coordinator:
    def __init__(self, coordinator_address):
        print(f"initialize coordinator with address: {coordinator_address}")
        jax.distributed.initialize(
            coordinator_address=coordinator_address,
            num_processes=NUM_WORKERS + 1,
            process_id=0,
        )

    def ready(self):
        return True

    def get_device_count(self):
        return f"Device count: {jax.device_count()}, Local device count: {jax.local_device_count()}, devices: {jax.devices()}"


@ray.remote(num_gpus=2)
class Worker:
    def __init__(self, coordinator_address, index):
        print(jax.local_devices())
        print(
            f"initialize with coordinator_address: {coordinator_address}, process index: {index}"
        )
        jax.distributed.initialize(
            coordinator_address=coordinator_address,
            num_processes=NUM_WORKERS,
            process_id=index,
        )
        self.x = jax.device_put(1)

    def ready(self):
        return True

    def get_x(self):
        return self.x

    def get_device_count(self):
        return f"Device count: {jax.device_count()}, Local device count: {jax.local_device_count()}, devices: {jax.devices()}"


coordinator = Coordinator.options(
    scheduling_strategy=NodeAffinitySchedulingStrategy(
        node_id="c56b02d49432bb84f43d6ce4d9b2090ea0b4b84431e95c2a5679a0d9",  # hack to get id
        soft=False,
    )
).remote(coordinator_address)
workers = [
    Worker.remote(coordinator_address, index + 1)
    for index in range(NUM_WORKERS)
]

print(ray.get([w.ready.remote() for w in workers + [coordinator]]))

print(ray.get([w.get_x.remote() for w in workers]))
print(ray.get([w.get_device_count.remote() for w in workers]))
print(f"On headnode -- {ray.get(coordinator.get_device_count.remote())}")
