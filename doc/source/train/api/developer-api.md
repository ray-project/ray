---
myst:
  html_meta:
    description: "Developer API reference for extending Ray Train: the DataParallelTrainer base class, backend base classes, and user callbacks."
---

(train-api-developer)=

# Developer APIs

These developer APIs extend Ray Train with custom trainers, backends, and callbacks.

```{eval-rst}
.. currentmodule:: ray
```

## Trainer base class

```{eval-rst}
.. autosummary::
    :nosignatures:
    :toctree: doc/

    ~train.v2.api.data_parallel_trainer.DataParallelTrainer
```

## Backend base classes

```{eval-rst}
.. _train-backend:
.. _train-backend-config:

.. autosummary::
    :nosignatures:
    :template: autosummary/class_without_autosummary.rst
    :toctree: doc/

    ~train.backend.Backend
    ~train.backend.BackendConfig
```

## Trainer callbacks

```{eval-rst}
.. autosummary::
    :nosignatures:
    :toctree: doc/

    ~train.UserCallback
```
