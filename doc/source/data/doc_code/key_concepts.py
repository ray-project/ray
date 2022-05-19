# flake8: noqa

# fmt: off
# __resource_allocation_begin__
import ray
from ray.data.context import DatasetContext
from ray.util.scheduling_strategies import PlacementGroupSchedulingStrategy

# Create a single-CPU local cluster.
ray.init(num_cpus=1)
ctx = DatasetContext.get_current()
# Create a placement group that takes up the single core on the cluster.
placement_group = ray.util.placement_group(
    name="core_hog",
    strategy="SPREAD",
    bundles=[
        {"CPU": 1},
    ],
)
ray.get(placement_group.ready())

# Tell Datasets to use the placement group for all Datasets tasks.
ctx.scheduling_strategy = PlacementGroupSchedulingStrategy(placement_group)
# This Dataset workload will use that placement group for all read and map tasks.
ds = ray.data.range(100, parallelism=2) \
    .map(lambda x: x + 1)

assert ds.take_all() == list(range(1, 101))
# __resource_allocation_end__
# fmt: on

# fmt: off
# __block_move_begin__
import ray
from ray.data.context import DatasetContext

ctx = DatasetContext.get_current()
ctx.optimize_fuse_stages = False

def map_udf(df):
    df["sepal.area"] = df["sepal.length"] * df["sepal.width"]
    return df

ds = ray.data.read_parquet("example://iris.parquet") \
    .experimental_lazy() \
    .map_batches(map_df) \
    .filter(lambda row: row["sepal.area"] > 15)
# __block_move_end__
# fmt: on

# fmt: off
# __dataset_pipelines_execution_begin__
import ray

# ML ingest re-reading from storage on every epoch.
torch_ds = ray.data.read_parquet("example://iris.parquet") \
    .repeat() \
    .random_shuffle() \
    .to_torch()

# Streaming batch inference pipeline that pipelines the transforming of a single
# file with the reading of a single file (at most 2 file's worth of data in-flight
# at a time).
infer_ds = ray.data.read_binary_files("example://mniset_subset_partitioned/") \
    .window(blocks_per_window=1) \
    .map(lambda bytes_: np.asarray(PIL.Image.open(BytesIO(bytes_)).convert("L"))) \
    .map_batches(lambda imgs: [img.mean() > 0.5 for img in imgs])
# __dataset_pipelines_execution_end__
# fmt: on
