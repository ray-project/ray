---
myst:
  html_meta:
    description: "API reference for the errors Ray Train raises, exported from ray.train."
---

(train-api-exceptions)=

# Exceptions

Ray Train raises these `ray.train` errors when a training run fails.

```{eval-rst}
.. currentmodule:: ray
```

```{eval-rst}
.. autosummary::
    :nosignatures:
    :template: autosummary/class_without_autosummary.rst
    :toctree: doc/

    ~train.ControllerError
    ~train.PreemptionError
    ~train.WorkerGroupError
    ~train.TrainingFailedError
```
