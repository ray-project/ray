# Configure scale and GPUs

Outside your training function, create a {class}`~ray.train.ScalingConfig` object that sets the following two parameters:

- {class}`num_workers <ray.train.ScalingConfig>`: The number of distributed training worker processes.
- {class}`use_gpu <ray.train.ScalingConfig>`: Whether each worker uses a GPU or a CPU.

```{testcode}
from ray.train import ScalingConfig
scaling_config = ScalingConfig(num_workers=2, use_gpu=True)
```


For details, see {ref}`train_scaling_config`.

# Configure persistent storage

Create a {class}`~ray.train.RunConfig` object to specify the path where Ray Train saves results, including checkpoints and artifacts.

```{testcode}
from ray.train import RunConfig

# Local path (/some/local/path/unique_run_name)
run_config = RunConfig(storage_path="/some/local/path", name="unique_run_name")

# Shared cloud storage URI (s3://bucket/unique_run_name)
run_config = RunConfig(storage_path="s3://bucket", name="unique_run_name")

# Shared NFS path (/mnt/nfs/unique_run_name)
run_config = RunConfig(storage_path="/mnt/nfs", name="unique_run_name")
```


:::{warning}
A *shared storage location*, such as cloud storage or NFS, is optional for single-node clusters but required for multi-node clusters. On a multi-node cluster, a local path {ref}`raises an error <multinode-local-storage-warning>` during checkpointing.
:::


For details, see {ref}`persistent-storage-guide`.


# Launch a training job

To launch a distributed training job, pass the training function, scaling configuration, and run configuration to a {class}`~ray.train.torch.TorchTrainer` and call `fit()`.

```{testcode}
:hide:

from ray.train import ScalingConfig

train_func = lambda: None
scaling_config = ScalingConfig(num_workers=1)
run_config = None
```

```{testcode}
from ray.train.torch import TorchTrainer

trainer = TorchTrainer(
    train_func, scaling_config=scaling_config, run_config=run_config
)
result = trainer.fit()
```


# Access training results

After training completes, `trainer.fit()` returns a {class}`~ray.train.Result` object with information about the training run, including the metrics and checkpoints reported during training.

```{testcode}
result.metrics     # The metrics reported during training.
result.checkpoint  # The latest checkpoint reported during training.
result.path        # The path where logs are stored.
result.error       # The exception that was raised, if training failed.
```

For more usage examples, see {ref}`train-inspect-results`.
