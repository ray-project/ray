import ray


ds = ray.data.range(10).lazy()
ds.show()
# ds.map_batches(lambda x: x + 1).show()
print(ds.stats())
