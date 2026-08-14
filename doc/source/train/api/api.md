---
myst:
  html_meta:
    description: "API reference index for Ray Train, covering the PyTorch, Lightning, Transformers, TensorFlow/Keras, XGBoost, and LightGBM trainers and configs."
---

(train-api)=

# Ray Train API

```{eval-rst}
.. currentmodule:: ray
```

:::{important}
These API references are for the revamped Ray Train V2 implementation that is available starting from Ray 2.43 by enabling the environment variable `RAY_TRAIN_V2_ENABLED=1`. These APIs assume that the environment variable has been enabled.

See {ref}`train-deprecated-api` for the old API references and the [Ray Train V2 Migration Guide](https://github.com/ray-project/ray/issues/49454).
:::

## PyTorch Ecosystem

```{eval-rst}
.. autosummary::
    :nosignatures:
    :toctree: doc/

    ~train.torch.TorchTrainer
    ~train.torch.TorchConfig
    ~train.torch.xla.TorchXLAConfig
```

(train-pytorch-integration)=

### PyTorch

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

(train-lightning-integration)=

### PyTorch Lightning

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

(train-transformers-integration)=

### Hugging Face Transformers

```{eval-rst}
.. autosummary::
    :nosignatures:
    :toctree: doc/

    ~train.huggingface.transformers.prepare_trainer
    ~train.huggingface.transformers.RayTrainReportCallback
```

## More Frameworks

### TensorFlow/Keras

```{eval-rst}
.. autosummary::
    :nosignatures:
    :toctree: doc/

    ~train.tensorflow.TensorflowTrainer
    ~train.tensorflow.TensorflowConfig
    ~train.tensorflow.prepare_dataset_shard
    ~train.tensorflow.keras.ReportCheckpointCallback
```

### XGBoost

```{eval-rst}
.. autosummary::
    :nosignatures:
    :toctree: doc/

    ~train.xgboost.XGBoostTrainer
    ~train.xgboost.RayTrainReportCallback
```

### LightGBM

```{eval-rst}
.. autosummary::
    :nosignatures:
    :toctree: doc/

    ~train.lightgbm.LightGBMTrainer
    ~train.lightgbm.get_network_params
    ~train.lightgbm.RayTrainReportCallback
    ~train.lightgbm.normalize_pandas_for_lightgbm
```

### JAX

```{eval-rst}
.. autosummary::
    :nosignatures:
    :toctree: doc/

    ~train.v2.jax.JaxTrainer
```

(ray-train-configs-api)=

## Ray Train Configuration

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

(train-loop-api)=

## Ray Train Utilities

**Classes**

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

**Functions**

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

**Collective**

```{eval-rst}
.. autosummary::
    :nosignatures:
    :toctree: doc/

    ~train.collective.barrier
    ~train.collective.broadcast_from_rank_zero
```

## Ray Train Output

```{eval-rst}
.. autosummary::
    :nosignatures:
    :template: autosummary/class_without_autosummary.rst
    :toctree: doc/

    ~train.ReportedCheckpoint
    ~train.ReportedCheckpointStatus
    ~train.Result
```

## Ray Train Errors

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

## Ray Tune Integration Utilities

```{eval-rst}
.. autosummary::
    :nosignatures:
    :toctree: doc/

    tune.integration.ray_train.TuneReportCallback
```

## Ray Train Developer APIs

### Trainer Base Class

```{eval-rst}
.. autosummary::
    :nosignatures:
    :toctree: doc/

    ~train.v2.api.data_parallel_trainer.DataParallelTrainer
```

### Train Backend Base Classes

```{eval-rst}
.. _train-backend:
.. _train-backend-config:

.. autosummary::
    :nosignatures:
    :toctree: doc/
    :template: autosummary/class_without_autosummary.rst

    ~train.backend.Backend
    ~train.backend.BackendConfig
```

### Trainer Callbacks

```{eval-rst}
.. autosummary::
    :nosignatures:
    :toctree: doc/

    ~train.UserCallback
```
