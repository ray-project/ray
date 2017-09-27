import time
import ray

ray.init(num_cpus=4)

@ray.remote
def f():
    time.sleep(1)
    return True

start = time.time()

futures = [f.remote() for i in range(8)]
done = ray.get(futures)
end = time.time()

print(end - start)
# 2 seconds