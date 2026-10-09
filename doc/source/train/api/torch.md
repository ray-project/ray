---
myst:
  html_meta:
    description: "API reference for ray.train.torch: TorchTrainer, TorchConfig, TorchXLAConfig, and the PyTorch training loop utilities."
---

(train-pytorch-integration)=

# PyTorch

The `ray.train.torch` module runs distributed PyTorch training. `TorchXLAConfig` lives in `ray.train.torch.xla`.

```{eval-rst}
.. currentmodule:: ray
```

## Trainer and configs

```{eval-rst}
.. autosummary::
    :nosignatures:
    :toctree: doc/

    ~train.torch.TorchTrainer
    ~train.torch.TorchConfig
    ~train.torch.xla.TorchXLAConfig
```

## Training loop utilities

```{eval-rst}
.. autosummary::
    :nosignatures:
    :toctree: doc/

    ~train.torch.get_device
    ~train.torch.get_devices
    ~train.torch.prepare_model
    ~train.torch.prepare_data_loader
    ~train.torch.enable_reproducibility
```
