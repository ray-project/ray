import ray


@ray.remote
def f():
    return 0


@ray.remote
def g():
    import time
    start = time.time()
    while time.time() < start + 1:
        ray.get([f.remote() for _ in range(10)])


# 10MB -> hangs after ~5 iterations
# 20MB -> hangs after ~20 iterations
# 50MB -> hangs after ~50 iterations
ray.init(redis_max_memory=1024 * 1024 * 50)

i = 0
for i in range(100):
    i += 1
    a = g.remote()
    [ok], _ = ray.wait([a])
    print("iter", i)
