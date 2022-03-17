import ray
from ray.util import ActorPool


@ray.remote
class Actor:
    def double(self, n):
        return n * 2


a1, a2 = Actor.remote(), Actor.remote()
pool = ActorPool([a1, a2])

# pool.map(..) returns a Python generator object ActorPool.map
gen = pool.map(lambda a, v: a.double.remote(v), [1, 2, 3, 4])
print(list(gen))
# [2, 4, 6, 8]
