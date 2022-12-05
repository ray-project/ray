import ray


ds = ray.data.range(10)
ds.show()
print(ds.stats())
