---
myst:
  html_meta:
    description: "Fault-tolerant training with torchft in Ray Train: replica groups keep training through worker failures, and replacements recover state from a live peer instead of a checkpoint."
---

(train-torchft)=

# Fault-tolerant training with torchft

[torchft](https://github.com/meta-pytorch/torchft) adds per-step fault tolerance to data-parallel PyTorch training. With the Ray Train integration, training keeps running through a worker failure. The surviving workers continue, Ray Train replaces only the failed worker, and the replacement recovers model and optimizer state from a live peer over the network instead of loading a checkpoint.

:::{warning}
This integration is alpha. The API may change, and {ref}`several limitations <train-torchft-limitations>` apply. It requires Ray 2.56.0 or later.
:::

Compare this with {ref}`default fault tolerance <train-fault-tolerance>`, where any worker failure shuts down every worker, and the restarted worker group resumes from the latest checkpoint. The torchft integration avoids both costs: healthy workers don't restart, and recovery doesn't read a checkpoint from storage. This matters most for large data-parallel jobs with frequent failures, such as training on spot or preemptible instances, where full restarts and checkpoint reloads add up.

## How it works

torchft splits a data-parallel job into *replica groups*, each holding a full copy of the model. Training is data-parallel across replica groups. Two torchft components coordinate them:

- The *Lighthouse* is a coordination server that decides, every step, which replica groups are part of the current *quorum*, the set of replica groups that train together.
- A *Manager* runs in every worker's training function. It talks to the Lighthouse, rebuilds the cross-replica communicator when membership changes, and skips the optimizer step on every replica if any replica failed mid-step, so the model stays consistent.

When you pass `torch_config=TorchftConfig(...)` to a {class}`~ray.train.torch.TorchTrainer`, Ray Train does the following:

- Starts the torchft Lighthouse as a Ray actor on the node that runs the Ray Train controller, which is usually the head node, and sets the `TORCHFT_LIGHTHOUSE` address on every worker.
- Makes each Ray Train worker its own replica group and creates one process group per replica group. This integration supports DistributedDataParallel (DDP) only, so each replica group has exactly one worker.
- On a worker failure, replaces only the failed replica group, in the same placement group bundles, instead of shutting down and restarting the whole worker group.

When a worker fails mid-step, torchft discards that step on the surviving workers instead of committing a partial update. If at least `min_replicas` replica groups survive, they keep training without the failed one. Meanwhile, Ray Train starts a replacement worker. The replacement's Manager joins the quorum and *heals*: it copies the current model and optimizer state from a live peer over the network, then trains as a full member. If fewer than `min_replicas` replica groups survive, training waits until the replacement joins.

## Requirements

- Ray 2.56.0 or later.
- torchft installed on every node, with a PyTorch version that your torchft version supports. The APIs this guide uses are available in torchft 0.2.0 and later. Ray tests the integration against torchft nightly builds.
- A training function written for DDP. See {ref}`the limitations <train-torchft-limitations>` for what the integration doesn't support.

## Quick start

The following script trains a linear model on two CPU workers and keeps training through a worker failure. It's based on the [torchft linear example](https://github.com/ray-project/ray/blob/master/python/ray/train/v2/examples/pytorch/torchft_linear_example.py) that Ray CI tests:

```{literalinclude} ../doc_code/torchft_fault_tolerance.py
:language: python
:start-after: __torchft_quickstart_start__
:end-before: __torchft_quickstart_end__
```

The rest of this section walks through each change relative to a standard Ray Train script.

### Configure the trainer

Pass a {class}`~ray.train.torch.TorchftConfig` as the `torch_config`, with at least `min_replicas`:

```python
torch_config=TorchftConfig(lighthouse_kwargs={"min_replicas": 1}, backend="gloo")
```

Also set `max_failures` in {class}`~ray.train.FailureConfig` to 1 or higher. Otherwise, the first worker failure ends the run instead of replacing the failed replica group.

### Create the Manager

Create a torchft process group and a `Manager` with `state_dict` and `load_state_dict` callbacks. Use `ProcessGroupGloo` on CPU or `ProcessGroupNCCL` on GPU. The callbacks must include both the model and the optimizer state, because they're what a replacement replica group heals from. Because each Ray Train worker is its own replica group, pass `world_size=1`, `rank=0`, and a `replica_id` that's unique per worker, such as one derived from the Ray Train world rank. The optional `checkpoint_transport` configures how state transfers between peers during healing. `PGTransport` sends it over the process group.

### Wrap the model and optimizer

Wrap the model with `torchft.DistributedDataParallel` and the optimizer with `torchft.Optimizer`. These replace {func}`ray.train.torch.prepare_model`, so don't call it. The wrapped optimizer starts quorum negotiation on `zero_grad()` and commits the step on `step()` only if the whole quorum finished it.

### Shard the data

Shard the dataset with `torchft.DistributedSampler` instead of {func}`ray.train.torch.prepare_data_loader`. It shards first across replica groups, then across the ranks within each group. With DDP, each group has one rank:

```python
sampler = DistributedSampler(
    train_dataset,
    replica_rank=ray.train.get_context().get_world_rank(),
    num_replica_groups=ray.train.get_context().get_world_size(),
    group_rank=0,
    num_replicas=1,
)
```

### Train by step count, not epochs

Loop on `while manager.current_step() < num_steps`. The Manager's step count advances across failures, so a replacement replica group continues from the current step instead of starting over. Epoch-based loops don't work, because torchft doesn't transfer the data iterator position to a replacement worker.

### Shut down the Manager

Call `manager.shutdown()` at the end of the training function. Without it, a worker that rejoins late can hang in `backward()`, waiting for peers that already finished.

## Configure the Lighthouse

`TorchftConfig(lighthouse_kwargs=...)` passes its keyword arguments to the `torchft.coordination.LighthouseServer` constructor. The torchft version you install defines the accepted keys. As of torchft 0.2.0, they're the following:

- `min_replicas` (required): The minimum number of replica groups that must be alive to form a quorum. With `min_replicas` equal to `num_workers`, any failure pauses training until the replacement joins. With a smaller value, training continues on the survivors while Ray Train starts the replacement.
- `bind`: The address the Lighthouse listens on. Ray Train defaults this to `"[::]:0"`, which picks a free port.
- `join_timeout_ms`, `quorum_tick_ms`, `heartbeat_timeout_ms`: Quorum timing parameters. See the [torchft documentation](https://github.com/meta-pytorch/torchft) for details.

Also pass the same value as `min_replica_size` to the `Manager` in your training function. Both settings count replica groups, not workers within a replica group. The Manager commits a step only if at least `min_replica_size` replica groups took part in it.

`TorchftConfig` forwards its remaining keyword arguments, such as `backend`, to {class}`~ray.train.torch.TorchConfig`.

## Behavior on a worker failure

What happens when a worker fails depends on `min_replicas` and `max_failures`. The following table shows the behavior for a run with two workers when one fails mid-training:

| `min_replicas` | `max_failures` | Behavior |
| :-- | :-- | :-- |
| 1 | 1 | The surviving replica group keeps training without replaying the interrupted step. Ray Train replaces the failed worker, and the replacement heals and rejoins. |
| 2 | 0 | The run raises immediately. Ray Train never replaces a replica group when `max_failures` is 0. |
| 2 | 1 | Training pauses below the quorum size, resumes when the replacement joins, and replays the interrupted step. |

In every recovered case, the interrupted step doesn't commit a partial update. Either the quorum replays it, or the survivors skip it and move on. Each replica group replacement counts against `max_failures`.

(train-torchft-limitations)=

## Limitations

This integration is alpha. The following limitations apply:

- DDP only, with one worker per replica group. The integration doesn't support fully sharded data parallel (FSDP), hybrid sharded data parallel (HSDP), or tensor or pipeline parallelism.
- No Ray Data support. Load data with a PyTorch `DataLoader` and `torchft.DistributedSampler`, as in the preceding example.
- Step-based training only. torchft doesn't transfer the data iterator position to a replacement worker, so training can skip or repeat batches around a failure. Train for a fixed number of steps.
- With `min_replicas` smaller than `num_workers`, {func}`ray.train.report` doesn't complete while a replica group is missing, because it waits for all `num_workers` workers. Call it only when all workers are present, such as once at the end of training, or set `min_replicas` equal to `num_workers` if you rely on frequent reporting.
- The Lighthouse runs on the node that runs the Ray Train controller, with no persistence. If that node fails, the job fails, and {ref}`job driver fault tolerance <train-job-driver-fault-tolerance>` applies as usual.
- The worker count stays fixed. Ray Train replaces failed workers in place but doesn't resize the worker group. For adapting the worker count to resource availability, see {ref}`train-elastic-training`.

## Next steps

- {ref}`Default fault tolerance <train-fault-tolerance>`: Checkpoint-based recovery, which also covers job driver failures.
- {ref}`Elastic training <train-elastic-training>`: Adapt the worker count to resource availability.
- [torchft](https://github.com/meta-pytorch/torchft): The torchft repository, with its design notes and the `train_ddp.py` example.
