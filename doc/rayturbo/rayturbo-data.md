<!--- These will get pulled into anyscale docs -->
# Ray Data and RayTurbo Data

[Ray Data](https://docs.ray.io/en/latest/data/data.html) is an open source scalable data processing library designed for machine learning workloads. Ray Data provides flexible and performant APIs for distributed data processing and uses streaming execution to efficiently process large datasets. It’s particularly suited for offline batch inference and data preprocessing and ingest for ML training.

RayTurbo Data provides optimizations and additional features to open source Ray Data that improve the performance and production reliability for unstructured data processing like:

- Accelerated metadata fetching
- Improved autoscaling
- Job-level checkpointing

## Accelerated metadata fetching

RayTurbo Data offers an improved implementation of metadata fetching for file-based data sources, which
significantly improves the time for first read of large datasets.

Anyscale enables this feature by default. However, if you set `override_num_blocks`, the implementation falls back to the open source implementation.

## Improved autoscaling

RayTurbo Data provides an enhanced autoscaling implementation that delivers more responsive scaling for data processing workloads. The improved autoscaling allows clusters and actor pools to scale dynamically, enabling jobs to start immediately without waiting for the full cluster to launch. Additionally, jobs can gracefully handle node preemption by scaling down and continuing execution, improving reliability and cost efficiency.

Anyscale enables this feature by default.

## Job-level checkpointing

Existing Ray Data fault tolerance can recover from worker failures, retrying tasks that failed. However, it doesn't support checkpointing the entire job, which is useful for handling failure scenarios such as:

- Driver, head node, or entire cluster failures.
- Unexpected exceptions. For example, rows with malformed values trigger unhandled exceptions in the UDF.

RayTurbo Data offers job-level checkpointing to allow checkpointing the job execution progress. When a job fails, the restarted job can resume from the previous state.

Note: This feature only supports datasets that start with a read op, end with a write op, and only contain map-based operators like `map`, `map_batches`, `filter`, `flat_map`, etc.

To enable checkpointing,

1. Set `DataContext.checkpoint_config` before creating a dataset.
2. Set an ID column with the `id_column` config, to uniquely identify each row. This column must not change across the entire job.

```python
from ray.anyscale.data.checkpoint import CheckpointConfig

ds = ray.data.read_parquet("...")

ds.context.checkpoint_config =  CheckpointConfig(
    id_column="id",
    output_path="s3://my_bucket/checkpoint",
)
ds = ds.map(...)
ds.write_parquet("...")
```

### Configuration options

Configurable attributes include:

- `id_column` (str): Name of the ID column in the input dataset. ID values must be unique across all rows in the dataset and must persist during all operators.

- `checkpoint_path` (str): Path to store the checkpoint data. It can be a path to a cloud object storage, like `s3://bucket/path`, or a file system path. If the latter, the path must be a network-mounted file system, like `/mnt/cluster_storage/`, that's accessible to the entire cluster. If not set, defaults to `${ANYSCALE_ARTIFACT_STORAGE}/ray_data_checkpoint`.

Additional attributes for advanced usages:

- `delete_checkpoint_on_success` (bool): If true, automatically delete checkpoint data when the dataset execution succeeds.

- `override_filesystem` (`pyarrow.fs.FileSystem`): Override the `pyarrow.fs.FileSystem` object used to read or write checkpoint data. Set this option to use custom credentials.

- `override_backend` (CheckpointBackend): Override the CheckpointBackend object used to access the checkpoint backend storage. Set this option only if you want to use the row-backend checkpoint backends. By default, RayTurbo Data uses batch-based backends.
