import ray
import numpy as np


@ray.remote
class SendActor:
    def __init__(self):
        pass

    def _init(self, refs):
        self._ref = refs[0]
        self._buf = ray.get(self._ref)

    def put(self, i):
        ray.worker.global_worker.put_object(np.ones(100, dtype=np.uint8) * i, object_ref=self._ref)


@ray.remote
class ReceiveActor:
    def __init__(self):
        pass

    def _init(self, recv_refs):
        self._ref = recv_refs[0]

    def get(self):
        return ray.get(self._ref)


s = SendActor.remote()
r = ReceiveActor.remote()

ref = ray.put(np.zeros(100, dtype=np.uint8))
ray.get(s._init.remote([ref]))
ray.get(r._init.remote([ref]))

buf = ray.get(ref)
# NOTE(swang): Have the driver unseal because we don't have a way to signal the
# SendActor when the object has been unsealed.
ray.worker.global_worker.core_worker.unseal_object(ref)
del buf

ray.get(s.put.remote(3))
print(ray.get(ref))
print(ray.get(r.get.remote()))
