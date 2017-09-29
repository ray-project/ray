import time
import ray

ray.init()

@ray.remote
def f():
    time.sleep(1)
    return 1

# Execute f in parallel.
object_ids = [f.remote() for i in range(4)]
done = ray.get(object_ids)
