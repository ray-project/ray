---
myst:
  html_meta:
    description: "Configure persistent storage for Tune with cloud object storage or NFS, plus what changes when running without external storage."
---

(tune-storage-options)=

# Configure persistent storage in Ray Tune

:::{seealso}
Before you choose a storage option, see {ref}`the different types of data stored by Tune <tune-persisted-experiment-data>`.
:::

Configure persistent storage in a distributed Ray cluster to support the following use cases:

- **Trial-level fault tolerance**: When Tune restores a trial, such as after a node failure or after the experiment pauses, the trial might run on a different node. It still needs access to its latest checkpoint.
- **Experiment-level fault tolerance**: To restore an entire experiment, such as after the cluster crashes unexpectedly, Tune needs access to the latest experiment state and all trial checkpoints so it can resume from where the experiment left off.
- **Post-experiment analysis**: A consolidated location that stores data from all trials helps with post-experiment analysis, such as accessing the best checkpoints and hyperparameter configurations after the cluster terminates.
- **Bridge with downstream serving or batch inference tasks**: With configured storage, you can access the models and artifacts that trials generate, share them with others, or use them in downstream tasks.

## Storage options in Tune

Tune supports three storage scenarios:

- Cloud storage, such as AWS S3 or Google Cloud Storage, that all machines in the cluster can access.
- A network filesystem (NFS) mounted on all machines in the cluster.
- The local filesystem as the persistent storage location, when you run Tune on a single node.

:::{note}
You can also configure a network filesystem or cloud storage for single-node experiments. Persisting your experiment results to external storage can be useful, for example, when the instance that runs your experiment clears its local storage after termination.
:::

:::{seealso}
See {class}`~ray.tune.SyncConfig` for the full set of configuration options and more details.
:::

(tune-cloud-checkpointing)=

### Configure Tune with cloud storage

If all nodes in a Ray cluster have access to cloud storage, such as AWS S3 or Google Cloud Storage, Tune can save all experiment outputs in a shared cloud bucket.

To configure cloud storage, set `storage_path` to a remote location for Tune to upload to:

```python
from ray import tune

tuner = tune.Tuner(
    trainable,
    run_config=tune.RunConfig(
        name="experiment_name",
        storage_path="s3://bucket-name/sub-path/",
    )
)
tuner.fit()
```

In this example, you can find all experiment results in the shared storage at `s3://bucket-name/sub-path/experiment_name` for further processing.

:::{note}
The head node doesn't have local access to all experiment results. To further process a result such as the best checkpoint, first fetch it from cloud storage.

Also restore the experiment from the experiment directory at the cloud storage URI, not from the local experiment directory on the head node. See {ref}`an example of restoring from a cloud storage URI <tune-syncing-restore-from-uri>`.
:::

### Configure Tune with a network filesystem

If all Ray nodes have access to a network filesystem, such as AWS EFS or Google Cloud Filestore, they can all write experiment outputs to it.

Set `storage_path` to a path on the shared network filesystem:

```python
from ray import tune

tuner = tune.Tuner(
    trainable,
    run_config=tune.RunConfig(
        name="experiment_name",
        storage_path="/mnt/path/to/shared/storage/",
    )
)
tuner.fit()
```

In this example, you can find all experiment results in the shared storage at `/mnt/path/to/shared/storage/experiment_name` for further processing.

(tune-default-syncing)=

### Configure Tune without external persistent storage

Without NFS or cloud storage, Tune's behavior depends on whether the experiment runs on a single node or on a multi-node cluster.

#### On a single-node cluster

If you run an experiment on a single node, such as a laptop, Tune uses the local filesystem as the default storage location for checkpoints and other artifacts. By default, Tune saves results to a subdirectory of `~/ray_results` with a unique, auto-generated name. To change this location, set `storage_path` and `name` in {class}`~ray.tune.RunConfig`.

```python
from ray import tune

tuner = tune.Tuner(
    trainable,
    run_config=tune.RunConfig(
        storage_path="/tmp/custom/storage/path",
        name="experiment_name",
    )
)
tuner.fit()
```

In this example, you can find all experiment results locally at `/tmp/custom/storage/path/experiment_name` for further processing.

#### On a multi-node cluster (deprecated)

:::{warning}
Using the head node's local filesystem as the persistent storage location on a multi-node cluster is deprecated. By default, if you save trial checkpoints on a multi-node cluster without NFS or cloud storage set up, Tune raises an error. See [GitHub issue 37177](https://github.com/ray-project/ray/issues/37177) for more information.
:::

## Examples

This section shows how to configure the storage location and synchronization options, and how to resume an experiment if it gets interrupted. See {ref}`tune-fault-tolerance-ref` for more information on resuming experiments.

The example explains how Tune saves *trial checkpoints* across the cluster and to the external storage location, if you provide one. See {ref}`tune-persisted-experiment-data` for an overview of other experiment data that Tune needs to persist.

### Example: Run Tune with cloud storage

This example assumes that you run the script from your Ray cluster's head node.

In the following script, `my_trainable` is a Tune {ref}`trainable <trainable-docs>` that implements saving and loading checkpoints.

```python
import os
import ray
from ray import tune
from your_module import my_trainable


tuner = tune.Tuner(
    my_trainable,
    run_config=tune.RunConfig(
        # Name of your experiment
        name="my-tune-exp",
        # Configure how experiment data and checkpoints are persisted.
        # Cloud storage checkpointing survives the cluster when instances
        # are terminated, and performs better.
        storage_path="s3://my-checkpoints-bucket/path/",
        checkpoint_config=tune.CheckpointConfig(
            # Keep the best five checkpoints at all times
            # (with the highest AUC scores, a metric reported by the trainable)
            checkpoint_score_attribute="max-auc",
            checkpoint_score_order="max",
            num_to_keep=5,
        ),
    ),
)
# This starts the run!
results = tuner.fit()
```

In this example, Tune saves trial checkpoints to `s3://my-checkpoints-bucket/path/my-tune-exp/<trial_name>/checkpoint_<step>`.

(tune-syncing-restore-from-uri)=

If the run stops for any reason, such as you pressing Ctrl+C or the run terminating because of out-of-memory issues, you can resume it at any time from the experiment state saved in the cloud:

```python
from ray import tune
tuner = tune.Tuner.restore(
    "s3://my-checkpoints-bucket/path/my-tune-exp",
    trainable=my_trainable,
    resume_errored=True,
)
tuner.fit()
```

Options for restoring an experiment include `resume_unfinished`, `resume_errored`, and `restart_errored`. See the {meth}`~ray.tune.Tuner.restore` documentation for more details.

## Advanced configuration

See {ref}`Ray Train's section on advanced storage configuration <train-storage-advanced>`. All of those configurations also apply to Ray Tune.
