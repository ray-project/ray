import ray

ds = ray.data.range(10)
ds.repeat(4).show_windows()

ds = ray.data.range(10)
ds.repeat(4).map(lambda x: x, num_cpus=0.8).map(lambda x: x).show_windows()
