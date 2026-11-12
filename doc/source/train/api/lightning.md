---
myst:
  html_meta:
    description: "API reference for ray.train.lightning, the utilities for running PyTorch Lightning training with Ray Train."
---

(train-lightning-integration)=

# PyTorch Lightning

The `ray.train.lightning` module adapts a PyTorch Lightning `Trainer` to run on Ray Train.

```{eval-rst}
.. currentmodule:: ray
```

```{eval-rst}
.. autosummary::
    :nosignatures:
    :toctree: doc/

    ~train.lightning.prepare_trainer
    ~train.lightning.RayLightningEnvironment
    ~train.lightning.RayDDPStrategy
    ~train.lightning.RayFSDPStrategy
    ~train.lightning.RayDeepSpeedStrategy
    ~train.lightning.RayTrainReportCallback
```
