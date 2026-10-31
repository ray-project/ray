---
myst:
  html_meta:
    description: "API reference for ray.train.tensorflow: TensorflowTrainer, TensorflowConfig, and the Keras callback in ray.train.tensorflow.keras."
---

(train-api-tensorflow)=

# TensorFlow and Keras

The `ray.train.tensorflow` module runs distributed TensorFlow training. The Keras callback lives in `ray.train.tensorflow.keras`.

```{eval-rst}
.. currentmodule:: ray
```

```{eval-rst}
.. autosummary::
    :nosignatures:
    :toctree: doc/

    ~train.tensorflow.TensorflowTrainer
    ~train.tensorflow.TensorflowConfig
    ~train.tensorflow.prepare_dataset_shard
    ~train.tensorflow.keras.ReportCheckpointCallback
```
