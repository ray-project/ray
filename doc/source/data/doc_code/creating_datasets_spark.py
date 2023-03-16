# Not tested in CI currently!

import ray

# fmt: off
# __from_spark_begin__
import raydp

spark = raydp.init_spark(app_name="Spark -> Datasets Example",
                         num_executors=2,
                         executor_cores=2,
                         executor_memory="500MB")
df = spark.createDataFrame([(i, str(i)) for i in range(10000)], ["col1", "col2"])
# Create a tabular Dataset from a Spark DataFrame.
ds = ray.data.from_dask(df)
# -> Dataset(num_blocks=10, num_rows=10000, schema={col1: int64, col2: string})

ds.show(3)
# -> {'col1': 0, 'col2': '0'}
# -> {'col1': 1, 'col2': '1'}
# -> {'col1': 2, 'col2': '2'}
# __from_spark_end__
# fmt: on
