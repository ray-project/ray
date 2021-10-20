import ray
ray.init()

@ray.remote
def f():
    import time
    time.sleep(30)

a = f.remote()
import time
time.sleep(40)

