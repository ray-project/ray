import time

import ray

# ray.init(num_cpus=1)


@ray.remote(num_returns="streaming", _streaming_generator_backpressure_size_bytes=0)
def f():
    for i in range(3):
        print("yield ", i)
        yield i
        print("yield done ", i)


gen = f.remote()
next(gen)
print("Wait")
time.sleep(50)
print("Delete")
del gen


time.sleep(300)
