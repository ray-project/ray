import ray


def f(x):
    print("Processing", x)
    import time

    time.sleep(1)
    return x


for x in ray.data.range(10, parallelism=10).lazy().map(f).iter_batches(batch_size=None):
    print(x)
