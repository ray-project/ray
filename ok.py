import ray

@ray.remote
def allocate_memory():
    chunks = []
    bits_to_allocate = 8 * 100 * 1024 * 1024  # ~0.1 GiB
    while True:
        chunks.append([0] * bits_to_allocate)


try:
    ray.get(allocate_memory.remote())
except ray.exceptions.OutOfMemoryError as ex:
    print(ex)