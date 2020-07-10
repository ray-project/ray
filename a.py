import ray
ray.init()

@ray.remote
class A:
    def ping(self):
        return 3
print('create the first actor')
a = A.options(name='a').remote()
ray.get(a.ping.remote())
print('create the second actor')
b = A.options(name='a').remote()
print(ray.get(b.ping.remote()))
print('sleep')
import time
time.sleep(100)
