import ray

ray.init()

ds = ray.data.range(100000000)
ds.repartition(1000)
