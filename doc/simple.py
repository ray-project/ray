import ray

@ray.remote
def allocate_memory():
    chunks = []
    bits_to_allocate = 8 * 1024 * 1024 * 1024 # 1 GiB
    while True:
        chunks.append([0] * bits_to_allocate)

tasks = [allocate_memory.remote() for _ in range(1)]
ray.get(tasks)
