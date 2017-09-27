import time
import ray

ray.init(num_cpus=4)

@ray.remote
def f():
    time.sleep(1)
    return True

start = time.time()

obj_ids = [f.remote() for i in range(8)]
done = ray.get(obj_ids)
end = time.time()

print(end - start)
# 2 seconds
