import ray

ds = ray.data.range(100, parallelism=100).map_batches(lambda x: x).materialize()

ds.write_parquet("/tmp/test")
print("stats after", ds.stats())
print("stats internal", ds._write_ds.stats())
