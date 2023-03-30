import time
import ray

# Instead of "from tqdm import tqdm", use:
from ray.experimental.tqdm_ray import tqdm


@ray.remote
def f(name):
    for x in tqdm(range(100), desc=name):
        time.sleep(0.1)


ray.get([f.remote("task 1"), f.remote("task 2")])
