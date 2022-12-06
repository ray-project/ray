import ray


ds = ray.data.range(10).lazy()
ds.show()
ds.map(lambda x: x + 1).show()
# print(ds.stats())
