---
myst:
  html_meta:
    description: "API reference for the Ray Train configuration classes in ray.train: ScalingConfig, RunConfig, CheckpointConfig, FailureConfig, and more."
---

(ray-train-configs-api)=

# Configuration

These `ray.train` classes configure a training run: how many workers to use, where to store results, and how to checkpoint, validate, and recover from failures.

```{eval-rst}
.. currentmodule:: ray
```

```{eval-rst}
.. autosummary::
    :nosignatures:
    :toctree: doc/

    ~train.CheckpointConfig
    ~train.DataConfig
    ~train.FailureConfig
    ~train.LoggingConfig
    ~train.RunConfig
    ~train.ScalingConfig
    ~train.ValidationConfig
```

```{eval-rst}
.. autosummary::
    :nosignatures:
    :template: autosummary/class_without_autosummary.rst
    :toctree: doc/

    ~train.DatasetCheckpointConfig
```
