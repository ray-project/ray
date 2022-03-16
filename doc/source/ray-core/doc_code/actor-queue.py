import ray
from ray.util.queue import Queue

ray.init()
# You can pass this object around to different tasks/actors
queue = Queue(maxsize=100)


@ray.remote
def consumer(queue):
    next_item = queue.get(block=True)
    print(f"got work {next_item}")


consumers = [consumer.remote(queue) for _ in range(2)]

[queue.put(i) for i in range(10)]
