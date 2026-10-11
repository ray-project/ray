---
myst:
  html_meta:
    description: "API reference for the ray.train APIs you call inside a training function: reporting, checkpoints, context, dataset shards, and collective operations."
---

(train-loop-api)=

# Training function utilities

Call these `ray.train` APIs from inside your training function to report metrics and checkpoints, read the training context, and get dataset shards. The collective operations live in `ray.train.collective`.

```{eval-rst}
.. currentmodule:: ray
```

## Classes

```{eval-rst}
.. autosummary::
    :nosignatures:
    :toctree: doc/

    ~train.Checkpoint
    ~train.CheckpointUploadMode
    ~train.CheckpointConsistencyMode
    ~train.TrainContext
    ~train.ValidationFn
    ~train.ValidationTaskConfig
```

```{eval-rst}
.. autosummary::
    :nosignatures:
    :template: autosummary/class_without_autosummary.rst
    :toctree: doc/

    ~train.PreemptionInfo
```

## Functions

```{eval-rst}
.. autosummary::
    :nosignatures:
    :toctree: doc/

    ~train.get_all_reported_checkpoints
    ~train.get_checkpoint
    ~train.get_context
    ~train.get_dataset_shard
    ~train.get_preemption_info
    ~train.report
```

## Collective operations

```{eval-rst}
.. autosummary::
    :nosignatures:
    :toctree: doc/

    ~train.collective.barrier
    ~train.collective.broadcast_from_rank_zero
```
