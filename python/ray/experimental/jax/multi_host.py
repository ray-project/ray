import jax

import ray

from ray.train._internal.utils import get_address_and_port
address, port = get_address_and_port()

print(jax.local_devices())
# jax.distributed.initialize(
#     coordinator_address=f"{address}:{port}",
#     num_processes=2,
#     process_id=0
# )