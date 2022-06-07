# flake8: noqa

# fmt: off
# __resource_allocation_1_begin__
import ray
from ray import tune

# This Dataset workload will use spare cluster resources for execution.
def objective(*args):
    ray.data.range(10).show()

# Create a cluster with 4 CPU slots available.
ray.init(num_cpus=4)

# This runs, since Tune schedules one trial on 1 CPU, leaving 3 spare CPUs in the
# cluster for Dataset execution. However, deadlock can occur if you set num_samples=4,
# which would leave no extra CPUs for Datasets! To resolve these issues, see the
# "Inside Trial Placement Group" example tab.
tune.run(objective, num_samples=1, resources_per_trial={"cpu": 1})
# __resource_allocation_1_end__
# fmt: on

# fmt: off
# __resource_allocation_2_begin__
import ray
from ray import tune
from ray.data.context import DatasetContext
from ray.tune.error import TuneError
from ray.util.scheduling_strategies import PlacementGroupSchedulingStrategy

# Tune launches its trainable functions in placement groups.
def objective(*args):
    # Tell Datasets to use the current placement group for all Datasets tasks.
    ctx = DatasetContext.get_current()
    ctx.scheduling_strategy = PlacementGroupSchedulingStrategy(
        ray.util.get_current_placement_group())
    # This Dataset workload will use that placement group for all read and map tasks.
    ray.data.range(10).show()

# Create a cluster with 4 CPU slots available.
ray.init(num_cpus=4)

# This will error, since Tune has no resources reserved for Dataset tasks.
try:
    tune.run(objective)
except TuneError:
    print("This failed as expected")

# This runs fine, since there are 4 CPUs in the trial's placement group. The first
# CPU slot is used to run the objective function, leaving 3 for Dataset tasks.
tune.run(
    objective, resources_per_trial=tune.PlacementGroupFactory([{"CPU": 1}] * 4),
)
# __resource_allocation_2_end__
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
    .map_batches(map_udf) \
    .filter(lambda row: row["sepal.area"] > 15)
# __block_move_end__
# fmt: on

# fmt: off
# __dataset_pipelines_execution_begin__
import ray

# ML ingest re-reading from storage on every epoch.
torch_ds = ray.data.read_parquet("example://iris.parquet") \
    .repeat() \
    .random_shuffle_each_window() \
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
