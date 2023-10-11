import ray

ray.init()

@ray.remote(num_returns="streaming", _streaming_generator_backpressure_size_bytes=0)
def f():
    for i in range(3):
        print("yield ", i)
        yield i
        print("yield done ", i)

gen = f.remote()

import time
time.sleep(300)
