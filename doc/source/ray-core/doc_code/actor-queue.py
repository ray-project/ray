import ray
from ray.util.queue import Queue

ray.init()
# You can pass this object around to different tasks/actors
queue = Queue(maxsize=100)


@ray.remote
def consumer(id, queue):
    while not queue.empty():
        next_item = queue.get(block=True)
        print(f"consumer {id} got work {next_item}")


[queue.put(i) for i in range(10)]
print("Put work 1 - 10 to queue...")

consumers = [consumer.remote(id, queue) for id in range(2)]
