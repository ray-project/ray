import ray
import time
print(ray.init()["metrics_export_port"])

@ray.remote
def f():
    time.sleep(100)

@ray.remote
class A:
    def func(self):
        time.sleep(100)

a = f.remote()
b = f.remote()
c = A.remote()
d = c.func.remote()
ray.get(a)