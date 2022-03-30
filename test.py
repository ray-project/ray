import ray
import polars as pl


#ds = ray.data.range_arrow(10000000, parallelism=2)  # {'value': int64}
#
#ds = ds.select_polars(lambda: [
#    pl.col("value"),
#    (pl.col("value") * 2).alias("result"),
#])
ds = ray.data.range_arrow(100000, parallelism=4).select_polars(lambda: [pl.col("value") % 10]).groupby("value").max("value")


#ds = ds.map(lambda row: {"value": row["value"], "result": row["value"] * 2})

ds.show()  # {'value': int64, 'result': int64}
print(ds.stats())
