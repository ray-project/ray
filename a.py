import ray

ray.init()


@ray.remote
class A:
    def kill(self):
        import os

        os._exit(1)

    def get(self):
        return 1


a = A.remote()


@ray.remote
class B:
    def get_result(self, a):
        return ray.get(a.get.remote())


b = B.remote()
a.kill.remote()
try:
    ray.get(b.get_result.remote(a))
except Exception as e:
    print(isinstance(e.as_instanceof_cause(), ray.exceptions.RayActorError))
    print(isinstance(e.cause, ray.exceptions.RayActorError))
