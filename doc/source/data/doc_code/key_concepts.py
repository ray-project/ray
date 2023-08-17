# flake8: noqa

# fmt: off
# __resource_allocation_1_begin__
import ray
from ray import tune

# This workload will use spare cluster resources for execution.
def objective(*args):
    ray.data.range(10).show()

# Create a cluster with 4 CPU slots available.
ray.init(num_cpus=4)

# By setting `max_concurrent_trials=3`, this ensures the cluster will always
# have a sparse CPU for Dataset. Try setting `max_concurrent_trials=4` here,
# and notice that the experiment will appear to hang.
tuner = tune.Tuner(
    tune.with_resources(objective, {"cpu": 1}),
    tune_config=tune.TuneConfig(
        num_samples=1,
        max_concurrent_trials=3
    )
)
tuner.fit()
# __resource_allocation_1_end__
# fmt: on

ray.shutdown()

# fmt: off
# __resource_allocation_2_begin__
import ray
from ray import tune

# This workload will use reserved cluster resources for execution.
def objective(*args):
    ray.data.range(10).show()

# Create a cluster with 4 CPU slots available.
ray.init(num_cpus=4)

# This runs smoothly since _max_cpu_fraction_per_node is set to 0.8, effectively
# reserving 1 CPU for Dataset task execution.
tuner = tune.Tuner(
    tune.with_resources(objective, tune.PlacementGroupFactory(
        [{"CPU": 1}],
        _max_cpu_fraction_per_node=0.8,
    )),
    tune_config=tune.TuneConfig(num_samples=1)
)
tuner.fit()
# __resource_allocation_2_end__
# fmt: on

# fmt: off
# __block_move_begin__
import ray
from ray.data import DataContext

ctx = DataContext.get_current()
ctx.optimize_fuse_stages = False

def map_udf(df):
    df["sepal.area"] = df["sepal.length"] * df["sepal.width"]
    return df

ds = ray.data.read_parquet("s3://anonymous@ray-example-data/iris.parquet") \
    .lazy() \
    .map_batches(map_udf) \
    .filter(lambda row: row["sepal.area"] > 15)
# __block_move_end__
# fmt: on

# fmt: off
# __dataset_pipelines_execution_begin__
import numpy as np
import PIL
from io import BytesIO

import ray

# ML ingest re-reading from storage on every epoch.
torch_ds = ray.data.read_parquet("s3://anonymous@ray-example-data/iris.parquet") \
    .repeat() \
    .random_shuffle_each_window() \
    .iter_torch_batches()

# Streaming batch inference pipeline that pipelines the transforming of a single
# file with the reading of a single file (at most 2 file's worth of data in-flight
# at a time).
infer_ds = ray.data.read_binary_files("s3://anonymous@ray-example-data/mnist_subset_partitioned/") \
    .window(blocks_per_window=1) \
    .map(lambda bytes_: np.asarray(PIL.Image.open(BytesIO(bytes_)).convert("L"))) \
    .map_batches(lambda imgs: [img.mean() > 0.5 for img in imgs])
# __dataset_pipelines_execution_end__
# fmt: on
