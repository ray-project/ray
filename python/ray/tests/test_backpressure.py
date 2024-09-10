import ray
import time


def f(x):
    time.sleep(0.1)
    return x


def g(x):
    time.sleep(10000000)
    return x

ray.init(runtime_env={'working_dir': None})
ray.data.range(1000).map(f).map(g, num_cpus=0.1).limit(400).materialize()
