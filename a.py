import ray
import ray.util.collective as collective
import numpy as np

@ray.remote(num_cpus=1)
class Worker:
   def __init__(self):
       self.send = np.ones((4, ))
       self.recv = np.zeros((4, ))

#    def setup(self, world_size, rank):
#        collective.init_collective_group(world_size, rank, "gloo", "default")
#        return True

   def compute(self):
       collective.allreduce(self.send, "default")
       return self.send

   def destroy(self):
       collective.destroy_group()

# imperative
num_workers = 2
workers = []


# declarative
for i in range(num_workers):
   w = Worker.remote()
   workers.append(w)
_options = {
   "group_name": "default",
#    "world_size": num_workers,
#    "ranks": [0, 1],
   "backend": "gloo",
   "include_driver": True
}
# collective.create_collective_group(workers, **_options, include_driver=True)
collective.create_and_init_collective_group(workers, **_options)
fut = [w.compute.remote() for w in workers]
send = np.ones((4, ))
collective.allreduce(send, "default")
results = ray.get(fut)
print(results)
