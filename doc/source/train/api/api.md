---
myst:
  html_meta:
    description: "API reference index for Ray Train, covering the PyTorch, Lightning, Transformers, TensorFlow/Keras, XGBoost, LightGBM, and JAX integrations, configs, and training function utilities."
---

(train-api)=

# Ray Train API

The Ray Train API reference documents Ray Train's public Python API. Each page below covers one group of APIs, such as a framework integration or the configuration classes.

:::{important}
These API references are for the revamped Ray Train V2 implementation, which is the default starting in Ray 2.51. In Ray 2.43 through 2.50, enable it by setting the environment variable `RAY_TRAIN_V2_ENABLED=1`.

See {ref}`train-deprecated-api` for the old API references and the [Ray Train V2 Migration Guide](https://github.com/ray-project/ray/issues/49454).
:::

```{toctree}
:maxdepth: 2

torch
lightning
transformers
tensorflow
xgboost
lightgbm
jax
config
train-loop
result
exceptions
tune-integration
developer-api
```
