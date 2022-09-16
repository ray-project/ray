import ray
import time

@ray.remote
def f():
    import time
    time.sleep(.2)

@ray.remote
def g(x):
    import time
    time.sleep(.2)

[g.remote(f.remote()) for _ in range(100)]

ray.progress_bar()
