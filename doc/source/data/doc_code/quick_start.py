# flake8: noqa
# fmt: off

# __data_setup_begin__

import ray

# Create a Dataset of Python objects.
ds = ray.data.range(10000)
# -> Dataset(num_blocks=200, num_rows=10000, schema=<class 'int'>)

ds.take(5)
# -> [0, 1, 2, 3, 4]

ds.count()
# -> 10000

# Create a Dataset of Arrow records.
ds = ray.data.from_items([{"col1": i, "col2": str(i)} for i in range(10000)])
# -> Dataset(num_blocks=200, num_rows=10000, schema={col1: int64, col2: string})

ds.show(5)
# -> {'col1': 0, 'col2': '0'}
# -> {'col1': 1, 'col2': '1'}
# -> {'col1': 2, 'col2': '2'}
# -> {'col1': 3, 'col2': '3'}
# -> {'col1': 4, 'col2': '4'}

ds.schema()
# -> col1: int64
# -> col2: string

# __data_setup_end__

# __data_load_begin__
import pandas as pd
import dask.dataframe as dd

# Create a Dataset from a list of Pandas DataFrame objects.
pdf = pd.DataFrame({"one": [1, 2, 3], "two": ["a", "b", "c"]})
ds = ray.data.from_pandas([pdf])

# Create a Dataset from a Dask-on-Ray DataFrame.
dask_df = dd.from_pandas(pdf, npartitions=10)
ds = ray.data.from_dask(dask_df)
# __data_load_end__


# __data_transform_begin__
ds = ray.data.range(10000)
ds = ds.map(lambda x: x * 2)
# -> Map Progress: 100%|████████████████████| 200/200 [00:00<00:00, 1123.54it/s]
# -> Dataset(num_blocks=200, num_rows=10000, schema=<class 'int'>)
ds.take(5)
# -> [0, 2, 4, 6, 8]

ds.filter(lambda x: x > 5).take(5)
# -> Map Progress: 100%|████████████████████| 200/200 [00:00<00:00, 1859.63it/s]
# -> [6, 8, 10, 12, 14]

ds.flat_map(lambda x: [x, -x]).take(5)
# -> Map Progress: 100%|████████████████████| 200/200 [00:00<00:00, 1568.10it/s]
# -> [0, 0, 2, -2, 4]
# __data_transform_end__
