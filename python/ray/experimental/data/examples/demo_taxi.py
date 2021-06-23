import ray

ray.init()

ds = ray.experimental.data.read_parquet(
    "/home/eric/Desktop/taxi_2019", columns=["mta_tax"])
ds.show()
