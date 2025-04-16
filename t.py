import time

import ray


@ray.remote
class A:
    def hi(self):
        pass

    async def f(self):
        ray.actor.exit_actor()


a = A.remote()
ray.get(a.hi.remote())

ray.kill(a, no_restart=True)

try:
    ray.get(a.f.remote())
except:
    pass

time.sleep(100000)
