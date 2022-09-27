import ray
from ray.util.queue import Queue

ray.init()

@ray.remote
class Actor:
    def __init__(self, queue):
        self.queue = queue

    def get_data(self):
        data = self.queue.get()
        print(data)
        return data

queue = Queue(maxsize=1)
actor = Actor.remote(queue)

data_future = actor.get_data.remote()
queue.put(1)
print(ray.get(data_future))

