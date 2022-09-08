import jax

import ray
from ray.train._internal.utils import get_address_and_port
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy

ray.init()

NUM_WORKERS = 7

address, port = get_address_and_port()
coordinator_address = f"{address}:{port}"

@ray.remote(num_gpus=1)
class Coordinator:
    def __init__(self, coordinator_address):
        import jax
        print(f"initialize coordinator with address: {coordinator_address}")
        jax.distributed.initialize(
            coordinator_address=coordinator_address,
            num_processes=NUM_WORKERS + 1,
            process_id=0,
        )

    def ready(self):
        return True

    def get_device_count(self):
        return f"Process[0]: {jax.device_count()}"

    def get_local_device_count(self):
        return f"Process[0]: {jax.local_device_count()}"

    def get_devices(self):
        return f"Process[0]: {str(jax.devices())}"


@ray.remote(num_gpus=1)
class Worker:
    def __init__(self, coordinator_address, index):
        import jax
        print(f"initialize worker with index: {index}")
        jax.distributed.initialize(
            coordinator_address=coordinator_address,
            num_processes=NUM_WORKERS + 1,
            process_id=index,
        )
        self.x = jax.device_put(1)
        self.index = index

    def ready(self):
        return True

    def get_x(self):
        return self.x

    def get_device_count(self):
        return f"Process[{self.index}]: {jax.device_count()}"

    def get_local_device_count(self):
        return f"Process[{self.index}]: {jax.local_device_count()}"

    def get_devices(self):
        return f"Process[{self.index}]: {str(jax.devices())}"

# Ensure this runs on the head node
coordinator = Coordinator.options(
    scheduling_strategy=NodeAffinitySchedulingStrategy(
        ray.get_runtime_context().get_node_id(),
        soft=False,
    )
).remote(coordinator_address)

workers = [
    Worker.remote(coordinator_address, index + 1)
    for index in range(NUM_WORKERS)
]

print(ray.get([w.ready.remote() for w in workers + [coordinator]]))
print(ray.get([w.get_x.remote() for w in workers]))

print(f"Device count: {ray.get([w.get_device_count.remote() for w in workers])}")
print(f"Local device count: {ray.get([w.get_local_device_count.remote() for w in workers])}")
print(ray.get([w.get_devices.remote() for w in workers]))

print(f"On headnode get_device_count -- {ray.get(coordinator.get_device_count.remote())}")
print(f"On headnode get_local_device_count: -- {ray.get(coordinator.get_local_device_count.remote())}")
print(f"On headnode get_devices: -- {ray.get(coordinator.get_devices.remote())}")
