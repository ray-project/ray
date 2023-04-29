# flake8: noqa

import ray

# For tfrecords
ray.init(runtime_env={"pip": ["tensorflow_metadata"]})

# fmt: off
# __write_parquet_begin__
import ray

ds = ray.data.range(1000)
# -> Datastream(num_blocks=200, num_rows=1000, schema={id: int64})

# Write out just one file.
ds.repartition(1).write_parquet("/tmp/one_parquet")
# -> /tmp/one_parquet/d757569dfb2845589b0ccbcb263e8cc3_000000.parquet

# Write out multiple files.
ds.repartition(3).write_parquet("/tmp/multi_parquet")
# -> /tmp/multi_parquet/2b529dc5d8eb45e5ad03e69fb7ad8bc0_000000.parquet
# -> /tmp/multi_parquet/2b529dc5d8eb45e5ad03e69fb7ad8bc0_000001.parquet
# -> /tmp/multi_parquet/2b529dc5d8eb45e5ad03e69fb7ad8bc0_000002.parquet
# __write_parquet_end__
# fmt: on

# fmt: off
# __write_csv_begin__
import ray

ds = ray.data.range(1000)
# -> Datastream(num_blocks=200, num_rows=1000, schema={id: int64})

# Write out just one file.
ds.repartition(1).write_csv("/tmp/one_csv")
# -> /tmp/one_csv/d757569dfb2845589b0ccbcb263e8cc3_000000.csv

# Write out multiple files.
ds.repartition(3).write_csv("/tmp/multi_csv")
# -> /tmp/multi_csv/2b529dc5d8eb45e5ad03e69fb7ad8bc0_000000.csv
# -> /tmp/multi_csv/2b529dc5d8eb45e5ad03e69fb7ad8bc0_000001.csv
# -> /tmp/multi_csv/2b529dc5d8eb45e5ad03e69fb7ad8bc0_000002.csv
# __write_csv_end__
# fmt: on

# fmt: off
# __write_json_begin__
import ray

ds = ray.data.range(1000)
# -> Datastream(num_blocks=200, num_rows=1000, schema={id: int64})

# Write out just one file.
ds.repartition(1).write_json("/tmp/one_json")
# -> /tmp/one_json/ab693fde13634f4c8cdaef1db9595ac1_000000.json

# Write out multiple files.
ds.repartition(3).write_json("/tmp/multi_json")
# -> /tmp/multi_json/f467636b3c41420bb109505ab56c6eae_000000.json
# -> /tmp/multi_json/f467636b3c41420bb109505ab56c6eae_000001.json
# -> /tmp/multi_json/f467636b3c41420bb109505ab56c6eae_000002.json
# __write_json_end__
# fmt: on

# fmt: off
# __write_numpy_begin__
import ray
import numpy as np

ds = ray.data.from_numpy(np.arange(1000))
# -> Datastream(
#        num_blocks=1,
#        num_rows=1000,
#        schema={data: <ArrowTensorType: shape=(), dtype=int64>},
#    )
ds.show(2)
# -> {'data': array(0)}
# -> {'data': array(1)}

# Write out just one file.
ds.repartition(1).write_numpy("/tmp/one_numpy", column="data")
# -> /tmp/one_numpy/78c91652e2364a7481cf171bed6d96e4_000000.npy

# Write out multiple files.
ds.repartition(3).write_numpy("/tmp/multi_numpy", column="data")
# -> /tmp/multi_numpy/b837e5b5a18448bfa3f8388f5d99d033_000000.npy
# -> /tmp/multi_numpy/b837e5b5a18448bfa3f8388f5d99d033_000001.npy
# -> /tmp/multi_numpy/b837e5b5a18448bfa3f8388f5d99d033_000002.npy
# __write_numpy_end__
# fmt: on

# fmt: off
# __write_tfrecords_begin__
import ray

ds = ray.data.from_items(
    [
        {"some_int": 1, "some_float": 1.0, "some_bytestring": b"abc"},
        {"some_int": 2, "some_float": 2.0, "some_bytestring": b"def"},
    ]
)
# -> Datastream(
#        num_blocks=2, 
#        num_rows=2, 
#        schema={some_int: int64, some_float: double, some_bytestring: binary}
#    )

ds.show(2)
# -> {'some_int': 1, 'some_float': 1.0, 'some_bytestring': b'abc'}
# -> {'some_int': 2, 'some_float': 2.0, 'some_bytestring': b'def'}

# Write out just one file.
ds.repartition(1).write_tfrecords("/tmp/one_tfrecord")
# -> /tmp/one_tfrecord/6d41f90ee8ac4d7db0aa4f43efd3070c_000000.tfrecords

# Write out multiple files.
ds.repartition(2).write_tfrecords("/tmp/multi_tfrecords")
# -> /tmp/multi_tfrecords/1ba614cf75de47e184b7d8f4a1cdfc80_000000.tfrecords
# -> /tmp/multi_tfrecords/1ba614cf75de47e184b7d8f4a1cdfc80_000001.tfrecords
# __write_tfrecords_end__
# fmt: on


ds = ray.data.from_items(
    [
        {"some_int": 1, "some_float": 1.0, "some_bytestring": b"abc"},
        {"some_int": 2, "some_float": 2.0, "some_bytestring": b"def"},
    ]
)
