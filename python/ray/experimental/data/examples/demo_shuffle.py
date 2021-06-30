import ray

ray.init()

ds = ray.experimental.data.range(100000000)
ds.repartition(1000)
