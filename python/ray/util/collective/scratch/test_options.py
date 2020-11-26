import ray
ray.init()

@ray.remote(num_gpus=1)
class Actor:
    def __init__(self):
        pass

    def compute(self):
        pass

# might work?
worker = Actor.options(collective={})# .remote()
print(worker._collective)
