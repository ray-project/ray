import ray

ray.init(num_cpus=4)


def sleep(x):
    import time

    time.sleep(0.1)
    return x


for x in (
    ray.data.range(50, parallelism=2)
    .map(sleep, num_cpus=0.3)
    .random_shuffle()
    .iter_rows()
):
    print("OUTPUT", x)
