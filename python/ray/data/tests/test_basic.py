import ray


ds = ray.data.range(10).lazy()
ds.show()
ds = ds.map(lambda x: x + 1)
ds = ds.map(lambda x: x + 1, num_cpus=0.5)
ds = ds.map(lambda x: x + 1)
ds.show()
print(ds.stats())
