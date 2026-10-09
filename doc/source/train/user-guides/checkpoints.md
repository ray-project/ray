---
myst:
  html_meta:
    description: "Save and load Ray Train checkpoints: distributed checkpointing from multiple workers, upload modes, async uploads, and post-training use."
---

(train-checkpointing)=

# Save and load checkpoints

Save snapshots of training progress with Ray Train {class}`Checkpoints <ray.train.Checkpoint>`.

Use checkpoints for the following:

- **Storing the best-performing model weights:** Save your model to persistent storage, and use it for downstream serving or inference.
- **Fault tolerance:** Handle worker process and node failures in a long-running training job, and use preemptible machines.
- **Distributed checkpointing:** {ref}`Upload model shards from multiple workers in parallel <train-distributed-checkpointing>`.

(train-dl-saving-checkpoints)=

(saving-checkpoints-during-training)=

## Save checkpoints during training

A Ray Train {class}`Checkpoint <ray.train.Checkpoint>` is a lightweight interface that represents a *directory* on local or remote storage.

For example, a checkpoint can point to a cloud storage directory such as `s3://my-bucket/my-checkpoint-dir`. A locally available checkpoint points to a location on the local filesystem, such as `/tmp/my-checkpoint-dir`.

To save a checkpoint in the training loop, do the following:

1. Write your model checkpoint to a local directory.

   - A {class}`Checkpoint <ray.train.Checkpoint>` only points to a directory, so you decide what it contains.
   - You can use any serialization format.
   - You can use the checkpoint utilities that training frameworks provide, such as `torch.save`, `pl.Trainer.save_checkpoint`, the Accelerate `accelerator.save_model`, the Transformers `save_pretrained`, and `tf.keras.Model.save`.

1. Create a {class}`Checkpoint <ray.train.Checkpoint>` from the directory using {meth}`Checkpoint.from_directory <ray.train.Checkpoint.from_directory>`.

1. Report the checkpoint to Ray Train using {func}`ray.train.report(metrics, checkpoint=...) <ray.train.report>`.

   - Ray Train uses the metrics you report alongside the checkpoint to {ref}`keep track of the best-performing checkpoints <train-dl-configure-checkpoints>`.
   - This call uploads the checkpoint to persistent storage, if configured. See {ref}`persistent-storage-guide`.


```{figure} ../images/checkpoint_lifecycle.png
The lifecycle of a {class}`~ray.train.Checkpoint`, from saving locally
to disk to uploading to persistent storage through `train.report`.
```

As the preceding figure shows, first write the checkpoint to a local temporary directory. Then call `train.report`, which uploads the checkpoint to its final persistent storage location. After that, you can safely clean up the local temporary directory to free disk space, for example by exiting the `tempfile.TemporaryDirectory` context.

:::{tip}
In standard DDP training, each worker has a copy of the full model, so save and report a checkpoint from only one worker to prevent redundant uploads.

The following example shows the typical pattern:

```{literalinclude} ../doc_code/checkpoints.py
:language: python
:start-after: __checkpoint_from_single_worker_start__
:end-before: __checkpoint_from_single_worker_end__
```

With parallel training strategies such as DeepSpeed ZeRO and FSDP, each worker has only a shard of the full training state, so you can save and report a checkpoint from each worker. For an example, see {ref}`train-distributed-checkpointing`.
:::


The following examples show how to save checkpoints with different training frameworks:

:::::{tab-set}
::::{tab-item} Native PyTorch
```{literalinclude} ../doc_code/checkpoints.py
:language: python
:start-after: __pytorch_save_start__
:end-before: __pytorch_save_end__
```

:::{tip}
In most cases, unwrap the DDP model before you save it to a checkpoint. `model.module.state_dict()` is the state dict without the `"module."` prefix on each key.
:::
::::


:::{tab-item} PyTorch Lightning
Ray Train uses PyTorch Lightning's `Callback` interface to report metrics and checkpoints. Ray Train provides a callback implementation that reports `on_train_epoch_end`.

At the end of each training epoch, the callback does the following:

- Collects all the logged metrics from `trainer.callback_metrics`.
- Saves a checkpoint through `trainer.save_checkpoint`.
- Reports to Ray Train through {func}`ray.train.report(metrics, checkpoint) <ray.train.report>`.

```{literalinclude} ../doc_code/checkpoints.py
:language: python
:start-after: __lightning_save_example_start__
:end-before: __lightning_save_example_end__
```

You can get the saved checkpoint path from {attr}`result.checkpoint <ray.train.Result>` and {attr}`result.best_checkpoints <ray.train.Result>`.

For more advanced usage, such as reporting at a different frequency or reporting custom checkpoint files, implement your own callback. The following example reports a checkpoint every three epochs:

```{literalinclude} ../doc_code/checkpoints.py
:language: python
:start-after: __lightning_custom_save_example_start__
:end-before: __lightning_custom_save_example_end__
```
:::


:::{tab-item} Hugging Face Transformers
Ray Train uses the `Callback` interface of the Hugging Face Transformers `Trainer` to report metrics and checkpoints.

```{rubric} Option 1: Use Ray Train's default report callback
```

Ray Train provides the callback implementation {class}`~ray.train.huggingface.transformers.RayTrainReportCallback`, which reports whenever the `Trainer` saves a checkpoint. It collects the latest logged metrics and reports them together with the latest saved checkpoint. To change the checkpointing frequency, set `save_strategy` and `save_steps`.

```{literalinclude} ../doc_code/checkpoints.py
:language: python
:start-after: __transformers_save_example_start__
:end-before: __transformers_save_example_end__
```

{class}`~ray.train.huggingface.transformers.RayTrainReportCallback` binds the latest metrics and checkpoints together. Configure `logging_strategy`, `save_strategy`, and `evaluation_strategy` so that the monitoring metric is logged at the same step as the checkpoint save.

For example, evaluation metrics such as `eval_loss` are logged during evaluation. To keep the best three checkpoints according to `eval_loss`, align the saving and evaluation frequency. The following are two examples of valid configurations:

```{testcode}
:skipif: True

args = TrainingArguments(
    ...,
    evaluation_strategy="epoch",
    save_strategy="epoch",
)

args = TrainingArguments(
    ...,
    evaluation_strategy="steps",
    save_strategy="steps",
    eval_steps=50,
    save_steps=100,
)

# And more ...
```


```{rubric} Option 2: Implement a custom report callback
```

If the default {class}`~ray.train.huggingface.transformers.RayTrainReportCallback` doesn't fit your use case, implement your own callback. The following example collects the latest metrics and reports whenever the `Trainer` saves a checkpoint.

```{literalinclude} ../doc_code/checkpoints.py
:language: python
:start-after: __transformers_custom_save_example_start__
:end-before: __transformers_custom_save_example_end__
```


In your own Transformers `Trainer` callback, you can customize when to report, such as in `on_save`, `on_epoch_end`, or `on_evaluate`. You can also customize what to report, such as custom metrics and checkpoint files.
:::
:::::


(train-distributed-checkpointing)=

(saving-checkpoints-from-multiple-workers-distributed-checkpointing)=

### Save checkpoints from multiple workers

With model-parallel training strategies in which each worker has only a shard of the full model, you can save and report checkpoint shards from each worker in parallel.

```{figure} ../images/persistent_storage_checkpoint.png
Distributed checkpointing in Ray Train. Each worker uploads its own checkpoint shard
to persistent storage independently.
```

Use distributed checkpointing to save checkpoints during model-parallel training, such as with DeepSpeed, FSDP, or Megatron-LM.

Distributed checkpointing is faster, which means less idle time and makes more frequent checkpointing practical. Each worker can upload its checkpoint shard in parallel, maximizing the network bandwidth of the cluster. Instead of a single node uploading the full model of size `M`, the cluster distributes the load across `N` nodes, each uploading a shard of size `M / N`.

Distributed checkpointing also avoids gathering the full model into a single worker's CPU memory. That gather operation puts a large CPU memory requirement on the worker that performs checkpointing, and it's a common source of out-of-memory (OOM) errors.


The following example shows distributed checkpointing with PyTorch:

```{literalinclude} ../doc_code/checkpoints.py
:language: python
:start-after: __distributed_checkpointing_start__
:end-before: __distributed_checkpointing_end__
```


:::{note}
Checkpoint files with the same name collide between workers. To avoid collisions, add a rank-specific suffix to checkpoint files.

A filename collision doesn't raise an error. Instead, the last uploaded version is the one that persists. This is fine if the file contents are the same across all workers.

Model shard saving utilities from frameworks such as DeepSpeed already create rank-specific filenames, so you usually don't need to handle this yourself.
:::


(train-checkpoint-upload-modes)=

## Checkpoint upload modes

By default, when you call {func}`~ray.train.report`, Ray Train synchronously pushes your checkpoint from `checkpoint.path` on local disk to `checkpoint_dir_name` on your `storage_path`. This is equivalent to calling {func}`~ray.train.report` with {class}`~ray.train.CheckpointUploadMode` set to `ray.train.CheckpointUploadMode.SYNC`.

```{literalinclude} ../doc_code/checkpoints.py
:language: python
:start-after: __checkpoint_upload_mode_sync_start__
:end-before: __checkpoint_upload_mode_sync_end__
```

(train-checkpoint-upload-mode-async)=

### Asynchronous checkpoint uploading

To start the next training step while the checkpoint uploads, use `ray.train.CheckpointUploadMode.ASYNC`, which starts a new thread to upload the checkpoint. Asynchronous uploading helps with larger checkpoints that might take longer to upload. If you want to immediately upload only a small checkpoint, it might add unnecessary complexity, as the following paragraphs describe.

Each `report` call blocks until the previous call's checkpoint upload completes, then starts a new checkpoint upload thread. Ray Train does this to avoid accumulating too many upload threads and potentially running out of memory.

Because `report` returns without waiting for the checkpoint upload to complete, you must keep the local checkpoint directory alive until the checkpoint upload completes. You can't use a directory that's deleted before the upload finishes, such as a `tempfile.TemporaryDirectory`, which Python deletes when its `with` block exits. The following example uses `tempfile.mkdtemp` instead. `report` also has a `delete_local_checkpoint_after_upload` parameter, which defaults to `True` if `checkpoint_upload_mode` is `ray.train.CheckpointUploadMode.ASYNC`, so Ray Train deletes the local checkpoint directory after the upload completes.

```{literalinclude} ../doc_code/checkpoints.py
:language: python
:start-after: __checkpoint_upload_mode_async_start__
:end-before: __checkpoint_upload_mode_async_end__
```

```{figure} ../images/sync_vs_async_checkpointing.png
The difference between synchronous and asynchronous
checkpoint uploading.
```

### Custom checkpoint uploading

By default, {func}`~ray.train.report` uploads the checkpoint from disk to the remote `storage_path` with the PyArrow filesystem copying utilities, then reports the checkpoint to Ray Train. To upload the checkpoint yourself or with a third-party library such as [Torch Distributed Checkpointing](https://docs.pytorch.org/docs/stable/distributed.checkpoint.html), use one of the following options:

:::::{tab-set}
:::{tab-item} Synchronous
To upload the checkpoint synchronously, first upload it to the `storage_path`, then report a reference to the uploaded checkpoint with `ray.train.CheckpointUploadMode.NO_UPLOAD`.

```{literalinclude} ../doc_code/checkpoints.py
:language: python
:start-after: __checkpoint_upload_mode_no_upload_start__
:end-before: __checkpoint_upload_mode_no_upload_end__
```
:::

::::{tab-item} Asynchronous
To upload the checkpoint asynchronously, set `checkpoint_upload_mode` to `ray.train.CheckpointUploadMode.ASYNC` and pass a `checkpoint_upload_fn` to `ray.train.report`. This function takes the `Checkpoint` and `checkpoint_dir_name` that you pass to `ray.train.report` and returns the persisted `Checkpoint`.

```{literalinclude} ../doc_code/checkpoints.py
:language: python
:start-after: __checkpoint_upload_fn_start__
:end-before: __checkpoint_upload_fn_end__
```

:::{warning}
Don't call `ray.train.report` in your `checkpoint_upload_fn`, because it may lead to unexpected behavior. Also avoid collective operations, such as {func}`~ray.train.report` or `model.state_dict()`, which can cause deadlocks. Return a checkpoint object from the upload function only after all checkpoint data is saved.
:::

:::{note}
Don't pass a `checkpoint_upload_fn` with `checkpoint_upload_mode=ray.train.CheckpointUploadMode.NO_UPLOAD`, because Ray Train ignores `checkpoint_upload_fn` in that mode. You can pass a `checkpoint_upload_fn` with `checkpoint_upload_mode=ray.train.CheckpointUploadMode.SYNC`, but that's equivalent to uploading the checkpoint yourself and reporting the checkpoint with `ray.train.CheckpointUploadMode.NO_UPLOAD`.
:::
::::
:::::

(train-dl-configure-checkpoints)=

## Configure checkpointing

Configure checkpointing with {class}`~ray.train.CheckpointConfig`. The primary option keeps only the top `K` checkpoints with respect to a metric, and Ray Train deletes lower-performing checkpoints to save storage space. By default, Ray Train keeps all checkpoints.

```{literalinclude} ../doc_code/key_concepts.py
:language: python
:start-after: __checkpoint_config_start__
:end-before: __checkpoint_config_end__
```


:::{note}
To save the top `num_to_keep` checkpoints with respect to a metric through {py:class}`~ray.train.CheckpointConfig`, always report the metric together with the checkpoints.
:::

(using-checkpoints-during-training)=

## Use checkpoints during training

During training, you might want to access the checkpoints you've reported, and their associated metrics, from the training workers. For example, you might report the best checkpoint so far to an experiment tracker. To do this, call {func}`~ray.train.get_all_reported_checkpoints` from your training function. It returns a list of {class}`~ray.train.ReportedCheckpoint` objects. These represent all the {class}`~ray.train.Checkpoint`s and associated metrics that you've reported so far and that the {ref}`checkpoint configuration <train-dl-configure-checkpoints>` has kept.

This function supports two consistency modes:

- `CheckpointConsistencyMode.COMMITTED`: Block until the checkpoint from the latest `ray.train.report` has been uploaded to persistent storage and committed.
- `CheckpointConsistencyMode.VALIDATED`: Block until the checkpoint from the latest `ray.train.report` has been uploaded to persistent storage, committed, and validated. See {ref}`train-validating-checkpoints`. This is the default consistency mode. It behaves the same as `CheckpointConsistencyMode.COMMITTED` if your report didn't start validation.

```{literalinclude} ../doc_code/checkpoints.py
:language: python
:start-after: __get_all_reported_checkpoints_example_start__
:end-before: __get_all_reported_checkpoints_example_end__
```

(using-checkpoints-after-training)=

## Use checkpoints after training

Access the latest saved checkpoint with {attr}`Result.checkpoint <ray.train.Result>`.

Access the full list of persisted checkpoints with {attr}`Result.best_checkpoints <ray.train.Result>`. If you set {class}`CheckpointConfig(num_to_keep) <ray.train.CheckpointConfig>`, this list contains the best `num_to_keep` checkpoints.

For a full guide on inspecting training results, see {ref}`train-inspect-results`.

{meth}`Checkpoint.as_directory <ray.train.Checkpoint.as_directory>` and {meth}`Checkpoint.to_directory <ray.train.Checkpoint.to_directory>` are the two main APIs for interacting with Ray Train checkpoints:

```{literalinclude} ../doc_code/checkpoints.py
:language: python
:start-after: __inspect_checkpoint_example_start__
:end-before: __inspect_checkpoint_example_end__
```


For Lightning and Transformers, if you use the default `RayTrainReportCallback` to save checkpoints in your training function, retrieve the original checkpoint files as follows:

::::{tab-set}
:::{tab-item} PyTorch Lightning
```{literalinclude} ../doc_code/checkpoints.py
:language: python
:start-after: __inspect_lightning_checkpoint_example_start__
:end-before: __inspect_lightning_checkpoint_example_end__
```
:::

:::{tab-item} Transformers
```{literalinclude} ../doc_code/checkpoints.py
:language: python
:start-after: __inspect_transformers_checkpoint_example_start__
:end-before: __inspect_transformers_checkpoint_example_end__
```
:::
::::


(train-dl-loading-checkpoints)=

## Restore training state from a checkpoint

To enable fault tolerance, modify your training loop to restore training state from a {class}`~ray.train.Checkpoint`.

In the training function, access the {class}`Checkpoint <ray.train.Checkpoint>` to restore from with {func}`ray.train.get_checkpoint <ray.train.get_checkpoint>`.

During {ref}`automatic failure recovery <train-fault-tolerance>`, {func}`ray.train.get_checkpoint <ray.train.get_checkpoint>` returns the latest reported checkpoint.

For details on restoration and fault tolerance, see {ref}`train-fault-tolerance`.

::::{tab-set}
:::{tab-item} Native PyTorch
```{literalinclude} ../doc_code/checkpoints.py
:language: python
:start-after: __pytorch_restore_start__
:end-before: __pytorch_restore_end__
```
:::


:::{tab-item} PyTorch Lightning
```{literalinclude} ../doc_code/checkpoints.py
:language: python
:start-after: __lightning_restore_example_start__
:end-before: __lightning_restore_example_end__
```
:::
::::


:::{note}
These examples use {meth}`Checkpoint.as_directory <ray.train.Checkpoint.as_directory>` to view the checkpoint contents as a local directory.

*If the checkpoint points to a local directory*, this method returns the local directory path without making a copy.

*If the checkpoint points to a remote directory*, this method downloads the checkpoint to a local temporary directory and returns the path to that directory.

*If multiple processes on the same node call this method simultaneously*, only one process performs the download, and the others wait for it to finish. After the download finishes, all processes receive the same local temporary directory to read from.

After all processes finish working with the checkpoint, the temporary directory is cleaned up.
:::
