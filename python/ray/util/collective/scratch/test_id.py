import ray
import ray.worker
ray.init()

@ray.remote
class Actor:
    def __init__(self):
        pass
    def compute(self):

        worker = ray.worker.global_worker
        return (worker.core_worker.get_actor_id()) # 0 #self.__ray_metadata___

actor = Actor.remote()
actor2 = Actor.remote()
print(actor._ray_actor_id, actor2._ray_actor_id)
k = actor.compute.remote()
print(ray.get(k))
print(actor._ray_actor_id)
