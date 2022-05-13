(train-api)=

# Ray Train API

(train-api-trainer)=

## Trainer

```{eval-rst}
.. autoclass:: ray.train.Trainer
    :members:
```

(train-api-iterator)=

### TrainingIterator

```{eval-rst}
.. autoclass:: ray.train.TrainingIterator
    :members:
```

(train-api-backend-config)=

## Backend Configurations

(train-api-torch-config)=

### TorchConfig

```{eval-rst}
.. autoclass:: ray.train.torch.TorchConfig
```

(train-api-tensorflow-config)=

### TensorflowConfig

```{eval-rst}
.. autoclass:: ray.train.tensorflow.TensorflowConfig
```

(train-api-horovod-config)=

### HorovodConfig

```{eval-rst}
.. autoclass:: ray.train.horovod.HorovodConfig
```

(train-api-backend-interfaces)=

### Backend interfaces (for developers only)

#### Backend

```{eval-rst}
.. autoclass:: ray.train.backend.Backend
```

#### BackendConfig

```{eval-rst}
.. autoclass:: ray.train.backend.BackendConfig

```

## Callbacks

(train-api-callback)=

### TrainingCallback

```{eval-rst}
.. autoclass:: ray.train.TrainingCallback
    :members:
```

(train-api-print-callback)=

### PrintCallback

```{eval-rst}
.. autoclass:: ray.train.callbacks.PrintCallback
```

(train-api-json-logger-callback)=

### JsonLoggerCallback

```{eval-rst}
.. autoclass:: ray.train.callbacks.JsonLoggerCallback
```

(train-api-tbx-logger-callback)=

### TBXLoggerCallback

```{eval-rst}
.. autoclass:: ray.train.callbacks.TBXLoggerCallback
```

(train-api-mlflow-logger-callback)=

### MLflowLoggerCallback

```{eval-rst}
.. autoclass:: ray.train.callbacks.MLflowLoggerCallback

```

(train-api-torch-tensorboard-profiler-callback)=

### TorchTensorboardProfilerCallback

```{eval-rst}
.. autoclass:: ray.train.callbacks.TorchTensorboardProfilerCallback
```

### ResultsPreprocessors

(train-api-results-preprocessor)=

#### ResultsPreprocessor

```{eval-rst}
.. autoclass:: ray.train.callbacks.results_preprocessors.ResultsPreprocessor
    :members:
```

#### SequentialResultsPreprocessor

```{eval-rst}
.. autoclass:: ray.train.callbacks.results_preprocessors.SequentialResultsPreprocessor
```

#### IndexedResultsPreprocessor

```{eval-rst}
.. autoclass:: ray.train.callbacks.results_preprocessors.IndexedResultsPreprocessor
```

#### ExcludedKeysResultsPreprocessor

```{eval-rst}
.. autoclass:: ray.train.callbacks.results_preprocessors.ExcludedKeysResultsPreprocessor
```

## Checkpointing

(train-api-checkpoint-strategy)=

### CheckpointStrategy

```{eval-rst}
.. autoclass:: ray.train.CheckpointStrategy
```

(train-api-func-utils)=

## Training Function Utilities

### train.report

```{eval-rst}
.. autofunction::  ray.train.report
```

### train.load_checkpoint

```{eval-rst}
.. autofunction::  ray.train.load_checkpoint
```

### train.save_checkpoint

```{eval-rst}
.. autofunction::  ray.train.save_checkpoint
```

### train.get_dataset_shard

```{eval-rst}
.. autofunction::  ray.train.get_dataset_shard
```

### train.world_rank

```{eval-rst}
.. autofunction::  ray.train.world_rank
```

### train.local_rank

```{eval-rst}
.. autofunction:: ray.train.local_rank
```

### train.world_size

```{eval-rst}
.. autofunction:: ray.train.world_size
```

(train-api-torch-utils)=

## PyTorch Training Function Utilities

(train-api-torch-prepare-model)=

### train.torch.prepare_model

```{eval-rst}
.. autofunction:: ray.train.torch.prepare_model
```

(train-api-torch-prepare-data-loader)=

### train.torch.prepare_data_loader

```{eval-rst}
.. autofunction:: ray.train.torch.prepare_data_loader
```

### train.torch.prepare_optimizer

```{eval-rst}
.. autofunction:: ray.train.torch.prepare_optimizer

```

### train.torch.backward

```{eval-rst}
.. autofunction:: ray.train.torch.backward
```

(train-api-torch-get-device)=

### train.torch.get_device

```{eval-rst}
.. autofunction:: ray.train.torch.get_device
```

### train.torch.enable_reproducibility

```{eval-rst}
.. autofunction:: ray.train.torch.enable_reproducibility
```

(train-api-torch-worker-profiler)=

### train.torch.accelerate

```{eval-rst}
.. autofunction:: ray.train.torch.accelerate
```

### train.torch.TorchWorkerProfiler

```{eval-rst}
.. autoclass:: ray.train.torch.TorchWorkerProfiler
    :members:
```

(train-api-tensorflow-utils)=

## TensorFlow Training Function Utilities

### train.tensorflow.prepare_dataset_shard

```{eval-rst}
.. autofunction:: ray.train.tensorflow.prepare_dataset_shard
```
