---
myst:
  html_meta:
    description: "Handle worker, node, and driver failures in Ray Train, including which checkpoint is restored and how node preemption is absorbed."
---

(train-fault-tolerance)=

# Handle failures and node preemption

:::{important}
This guide covers fault tolerance for Ray Train V2, which is available starting in Ray 2.43 when you set the environment variable `RAY_TRAIN_V2_ENABLED=1`. This guide assumes that you've set this environment variable.

For information about the deprecation and migration, see {ref}`Fault tolerance API deprecations <train-fault-tolerance-deprecation-info>`.
:::


Ray Train provides fault tolerance at three levels:

- **Worker process fault tolerance**: Handles errors in one or more workers while they run the training function.
- **Worker node fault tolerance**: Handles node failures during training.
- **Job driver fault tolerance**: Handles a crash of the Ray Train driver process, after which training needs to start again, possibly on a new cluster.

This guide shows how to configure and use these fault tolerance mechanisms. It also shows how to {ref}`save a just-in-time checkpoint <train-preemption-fault-tolerance>` before a node is preempted, so that a restart loses as little progress as possible.

(train-worker-fault-tolerance)=

## Worker process and node fault tolerance

*Worker process failures* are errors that occur within the training function of a worker, such as GPU out-of-memory (OOM) errors, cloud storage access errors, or other runtime errors.

*Node failures* are errors that bring down the entire node, including node preemption, OOM, network partitions, or other hardware failures. This section covers worker node failures. {ref}`Job driver fault tolerance <train-job-driver-fault-tolerance>` covers recovery from head node failures.

You can configure Ray Train to recover automatically from worker process and worker node failures. When Ray Train detects a failure, it shuts down all the workers. New nodes join the cluster if necessary, and Ray Train starts a new set of workers. The restarted workers can resume training by loading the latest checkpoint.

To retain progress after recovery, implement logic in your training function for both {ref}`saving <train-dl-saving-checkpoints>` *and* {ref}`loading checkpoints <train-dl-loading-checkpoints>`. Otherwise, training starts from scratch.

Each recovery from a worker process or node failure counts as a retry. To configure the number of retries, set the `max_failures` attribute of the {class}`~ray.train.FailureConfig` in the {class}`~ray.train.RunConfig` that you pass to the trainer. The default, `max_failures=0`, disables worker fault tolerance.

```{literalinclude} ../doc_code/fault_tolerance.py
:language: python
:start-after: __failure_config_start__
:end-before: __failure_config_end__
```

The following example shows a complete PyTorch training script with worker fault tolerance:

```{literalinclude} ../doc_code/fault_tolerance.py
:language: python
:start-after: __worker_fault_tolerance_start__
:end-before: __worker_fault_tolerance_end__
```


(which-checkpoint-will-be-restored)=

### Which checkpoint is restored?

Ray Train populates {func}`ray.train.get_checkpoint() <ray.train.get_checkpoint>` with the latest available {ref}`checkpoint reported to Ray Train <train-checkpointing>`. The {class}`~ray.train.Checkpoint` object that this method returns has the {meth}`~ray.train.Checkpoint.as_directory` and {meth}`~ray.train.Checkpoint.to_directory` methods, which download the checkpoint from the {class}`RunConfig(storage_path) <ray.train.RunConfig>` to local disk.

:::{note}
{meth}`~ray.train.Checkpoint.as_directory` and {meth}`~ray.train.Checkpoint.to_directory` download the checkpoint only once per node, even if the node runs multiple workers. The workers share the same checkpoint directory on local disk.
:::

### Illustrated example

Consider a cluster with a CPU head node and two GPU worker nodes. Four GPU training workers run on the two worker nodes. You've configured the {ref}`storage path <persistent-storage-guide>` to use cloud storage, which is where checkpoints are saved.

```{figure} ../images/fault_tolerance/worker_failure_start.png
:align: left

Training has been running for some time, and the latest checkpoint has been saved to cloud storage.
```

```{figure} ../images/fault_tolerance/worker_node_failure.png
:align: left

One of the GPU worker nodes fails because of a hardware fault. Ray Train detects this failure and shuts down all the workers.
Because the number of failures detected so far is less than the configured `max_failures`, Ray Train attempts to restart training
rather than exit and raise an error.
```

```{figure} ../images/fault_tolerance/worker_node_replacement.png
:align: left

Ray Train requests a new worker node to join the cluster and waits for it to come up.
```

```{figure} ../images/fault_tolerance/worker_group_recovery.png
:align: left

The new worker node has joined the cluster.
Ray Train restarts all the worker processes and provides them with the latest checkpoint.
The workers download the checkpoint from storage and use it to resume training.
```


(train-preemption-fault-tolerance)=

## Just-in-time checkpointing on node preemption

Cloud providers preempt spot instances and preemptible virtual machines with short notice. AWS gives about two minutes of notice before it preempts a spot instance, and GCP gives about 30 seconds. If your training run only checkpoints periodically, a preemption discards every step since the last checkpoint.

Ray Train can react to that notice. When Ray marks a node that hosts one of your workers as *draining* because of a preemption, {func}`ray.train.get_preemption_info() <ray.train.get_preemption_info>` returns a {class}`~ray.train.PreemptionInfo`. Your training function can then save a *just-in-time checkpoint* before the node is preempted, so the restarted run resumes from the step where the preemption happened.

To use just-in-time checkpointing, first make sure that your cluster sends preemption signals to Ray, then call {func}`~ray.train.get_preemption_info` in your training function.

(train-preemption-drain-signal)=

### Send preemption signals to Ray

Ray Train only reacts to preemptions that Ray Core knows about. It reads the draining nodes and their deadlines from the Ray Global Control Service (GCS). Something outside Ray Train has to watch for the preemption notice from the cloud provider and mark the node as draining with the `DRAIN_NODE_REASON_PREEMPTION` reason. How you set this up depends on where you run Ray:

::::{tab-set}
:::{tab-item} Managed platforms
Managed Ray platforms such as Anyscale watch for preemption notices on every node and drain the node for you. You don't need to configure anything.
:::

:::{tab-item} KubeRay
KubeRay starts Ray in each Pod with `ray start --block`. When that process receives `SIGTERM`, it sends the drain request to the GCS itself, with the `DRAIN_NODE_REASON_PREEMPTION` reason, before it shuts down Ray. You don't need to run a separate process to send it. Kubernetes sends `SIGTERM` when it deletes a Pod. Whether Kubernetes deletes the Pods on a preempted node depends on your cluster setup, such as graceful node shutdown on GKE or the AWS Node Termination Handler on Amazon EKS.

The drain deadline is `RAY_GRACEFUL_SHUTDOWN_DRAIN_TIMEOUT_S` seconds after `SIGTERM`, and the default is 30 seconds. To change it, set the environment variable on the Ray container. Set `terminationGracePeriodSeconds` on the Pod to a higher value than this timeout, so that Kubernetes doesn't kill the Pod before the drain window ends.

The drain window starts at `SIGTERM`, not at the preemption notice from the cloud provider, so it's limited by the Pod's termination grace period. To start the drain as soon as the cloud provider sends the notice, run a watcher as described in the **Self-managed clusters** tab.
:::

:::{tab-item} Self-managed clusters
Run a process on each node that polls the preemption notice from the cloud provider, such as the spot instance action on AWS or the preempted flag on GCP. When the notice arrives, drain the node with `ray drain-node --node-id <node-id> --reason DRAIN_NODE_REASON_PREEMPTION --reason-message <message> --deadline-remaining-seconds <seconds>`.

The `ray drain-node` command is a developer API, and Ray's public API stability guarantees don't cover it.
:::
::::

Choose a drain deadline that's shorter than the preemption notice. Ray Train restarts the run at the deadline, and it can only shut down the workers cleanly while the preempted node is still reachable. For example, with the two-minute AWS spot notice, a 60-second deadline leaves time for Ray Train to shut down the workers before AWS preempts the instance.

To test your just-in-time checkpointing on any cluster, drain a worker node manually with `ray drain-node`.

### How does Ray Train handle a preemption?

Ray Train handles a preemption in four steps:

1. Ray Train polls Ray Core every few seconds for draining nodes and ignores nodes that don't host training workers. On a TPU slice, a drain on any host marks every worker in the slice as preempted, because the cloud provider preempts the whole slice at once.
1. Every worker, including workers on healthy nodes, receives the same {class}`~ray.train.PreemptionInfo` from {func}`~ray.train.get_preemption_info`. It lists the preempted node IDs, the affected world ranks in `preempted_ranks`, and the drain deadline in `deadline_ms`, which is a UNIX timestamp in milliseconds.
1. Ray Train keeps every worker running so that your code can save a checkpoint. It waits until all workers exit or the drain deadline passes. If the drain has no deadline, Ray Train waits 120 seconds after it detects the preemption.
1. Ray Train shuts down the worker group and restarts the run from the latest reported checkpoint on replacement nodes.

Each restart counts against `FailureConfig(max_preemption_failures)`, a retry budget that's separate from `max_failures`. The default value of `-1` retries without a limit, so preemptions don't use up the retries that you reserve for real failures such as out-of-memory errors. After the run exhausts the budget, `trainer.fit()` raises a {class}`~ray.train.PreemptionError`.

### Save a just-in-time checkpoint

Call {func}`~ray.train.get_preemption_info` in your training loop. When it returns a {class}`~ray.train.PreemptionInfo`, save and report one more checkpoint. The restarted run resumes from the just-in-time checkpoint:

```{literalinclude} ../doc_code/fault_tolerance.py
:language: python
:start-after: __preemption_jit_checkpoint_start__
:end-before: __preemption_jit_checkpoint_end__
:emphasize-lines: 30, 34-41, 53-55
```

Follow these rules when you call {func}`~ray.train.get_preemption_info`:

- Call it from every worker the same number of times. It acts as a barrier that broadcasts the rank 0 result to all workers, so every worker acts on the same answer. If one worker skips a call, the other workers hang.
- Save the checkpoint once, then keep training. The return value stays set until the run restarts, so track whether you already saved, as `saved_on_preemption` does in the example.
- Fit the checkpoint upload into the drain window. Ray Train registers the checkpoint only after the upload to {ref}`persistent storage <persistent-storage-guide>` completes. If the upload takes longer than the drain window, see {ref}`train-preemption-slow-checkpoints`.

:::{warning}
Don't return from the training function after you save the just-in-time checkpoint. When the training function returns without an error, Ray Train marks the run as finished, even while a preemption is in progress.
:::

(train-preemption-slow-checkpoints)=

### Finish slow checkpoints without the preempted workers

By default, {func}`ray.train.report() <ray.train.report>` and {func}`~ray.train.get_preemption_info` wait for every worker. If the preempted node shuts down before its workers reach `report`, the surviving workers block until the drain deadline, and the just-in-time checkpoint is lost. Ray Train also shuts down the surviving workers as soon as the drain deadline passes, which can interrupt an upload.

To finish a checkpoint that takes longer than the drain window, set two {class}`~ray.train.FailureConfig` options:

```{literalinclude} ../doc_code/fault_tolerance.py
:language: python
:start-after: __preemption_relax_collectives_start__
:end-before: __preemption_relax_collectives_end__
:emphasize-lines: 5-10
```

With `relax_collectives_on_preemption=True`, {func}`~ray.train.report` and {func}`~ray.train.get_preemption_info` complete with only the surviving workers, so the survivors can commit the checkpoint on their own. `preemption_grace_s` keeps the surviving workers running for that many seconds past the drain deadline, so the upload can finish before Ray Train restarts the run.

If a training step and a checkpoint upload fit inside the drain window, you don't need either option. The preempted workers are still alive and take part in the checkpoint.

:::{warning}
Only set `relax_collectives_on_preemption=True` if the surviving workers can write a complete checkpoint on their own. The option has three limitations:

- Rank 0 must survive. Rank 0 sends the checkpoint directory name to the other workers, so if rank 0 is on a preempted node, Ray Train ignores the option and waits for every worker.
- The checkpoint can't depend on state that only the preempted workers hold. Data-parallel training where rank 0 saves a full model replica, as in the preceding example, works. If each worker saves its own shard, as with FSDP or DeepSpeed ZeRO, Ray Train can commit a checkpoint that's missing the shards of the preempted workers.
- Ray Train only relaxes two collectives: {func}`~ray.train.report` and {func}`~ray.train.get_preemption_info`. Every other collective still waits for every worker and hangs after a preempted peer shuts down. This includes {func}`ray.train.collective.barrier` and {func}`ray.train.collective.broadcast_from_rank_zero`, as well as framework collectives in your code such as `torch.distributed.barrier()` or a gather of a sharded state dict.
:::


(train-restore-guide)=
(train-job-driver-fault-tolerance)=


## Job driver fault tolerance

Job driver fault tolerance handles cases where the Ray Train driver process is interrupted. The Ray Train driver process is the process that calls `trainer.fit()`, and it usually runs on the head node of the cluster.

Any of the following events can interrupt the driver process:

- You interrupt the run manually, for example with Ctrl+C.
- The head node, which runs the driver process, crashes. For example, it runs out of memory or out of disk.
- The entire cluster goes down, for example because of a network error that affects all nodes.

In these cases, the Ray Train driver needs to be launched again. To pick up where the previous run left off, the relaunched driver needs to find a minimal amount of run state. This state includes the latest reported checkpoints, which are located at the {ref}`storage path <persistent-storage-guide>`. Ray Train fetches the latest checkpoint information from storage and passes it to the newly launched worker processes to resume training.

To find this run state, Ray Train relies on you passing the same {class}`RunConfig(storage_path, name) <ray.train.RunConfig>` pair as the previous run. If the `storage_path` or `name` doesn't match, Ray Train can't find the previous run state and starts a new run from scratch.

:::{warning}
If you reuse a `name` unintentionally, Ray Train fetches the previous run state, even if you're trying to start a new run. Always pass a unique run name when you launch a new run, so that `name` uniquely identifies a training job.
:::

:::{note}
Job driver crashes and interrupts don't count toward the `max_failures` limit of {ref}`worker fault tolerance <train-worker-fault-tolerance>`.
:::


The following example training script shows best practices for job driver fault tolerance:

```{literalinclude} ../doc_code/fault_tolerance.py
:language: python
:start-after: __job_driver_fault_tolerance_start__
:end-before: __job_driver_fault_tolerance_end__
```


Launch the entrypoint script with the following command:

```bash
python entrypoint.py --storage_path s3://my_bucket/ --run_name unique_run_id=da823d5
```


If the job is interrupted, run the same command to resume training. This example uses `da823d5` as the run ID, which you choose when you launch the job. You can often reuse this ID for other purposes, such as the `wandb` or `mlflow` run ID.


### Illustrated example

Consider a cluster with a CPU head node and two GPU worker nodes. Four GPU training workers run on the two worker nodes. You've configured the storage path to use cloud storage, which is where checkpoints are saved.


```{figure} ../images/fault_tolerance/cluster_failure_start.png
:align: left

Training has been running for some time, and the latest checkpoints and run state have been saved to storage.
```


```{figure} ../images/fault_tolerance/head_node_failure.png
:align: left

The head node crashes, for example because of an out-of-memory error, which interrupts the Ray Train driver process.
```

```{figure} ../images/fault_tolerance/cluster_failure.png
:align: left

The head node failure brings down the entire cluster.
```

```{figure} ../images/fault_tolerance/cluster_recovery.png
:align: left

A manual cluster restart or a job submission system brings up a new Ray cluster.
The Ray Train driver process runs on a new head node.
Ray Train fetches the run state information from storage at `{storage_path}/{name}`, such as `s3://my_bucket/my_run_name`,
and passes the latest checkpoint to the newly launched worker processes to resume training.
```


(train-fault-tolerance-deprecation-info)=

## Fault tolerance API deprecations

### `<Framework>Trainer.restore` API deprecation

The `<Framework>Trainer.restore` and `<Framework>Trainer.can_restore` APIs are deprecated as of Ray 2.43 and will be removed in a future release.

#### Motivation

The old API saved user code to pickled files, which could cause deserialization issues that left runs unrecoverable.

It also made configuration confusing. The old API loaded some configurations from the pickled files, required you to re-specify certain arguments, and accepted optional re-specification of another subset of arguments. As a result, it was unclear which configuration the restored run used.

#### Migration steps

To migrate from the old `<Framework>Trainer.restore` API to the new pattern, do the following:

1. Enable the environment variable `RAY_TRAIN_V2_ENABLED=1`.
1. Replace `<Framework>Trainer.restore` with the regular `<Framework>Trainer` constructor, and pass the same `storage_path` and `name` as the previous run.

### `<Framework>Trainer(restore_from_checkpoint)` API deprecation

The `<Framework>Trainer(restore_from_checkpoint)` API is deprecated as of Ray 2.43 and will be removed in a future release.

#### Motivation

This API was a common source of confusion and provided little value. It only set the initial value of `ray.train.get_checkpoint()` and didn't load any other run state.

#### Migration steps

Pass the initial checkpoint through the `train_loop_config` argument instead. For a code example, see the Train V2 migration guide in the following section.


### Additional resources

- [Train V2 migration guide](https://github.com/ray-project/ray/issues/49454): Full migration guide for Train V2
- [Train V2 Ray Enhancement Proposal (REP)](https://github.com/ray-project/enhancements/blob/main/reps/2024-10-18-train-tune-api-revamp/2024-10-18-train-tune-api-revamp.md): Technical details about the API change
- {ref}`train-fault-tolerance-deprecated-api`: Documentation for the old API
