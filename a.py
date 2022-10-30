import ray
ray.init()
import threading
l = threading.Lock()
@ray.remote
def f():
    print(l)
f.remote()
