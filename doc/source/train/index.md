---
myst:
  html_meta:
    description: "Distributed model training and fine-tuning at scale across PyTorch, Lightning, Hugging Face Transformers, XGBoost, and JAX."
---

(train-docs)=

# Ray Train: Scalable model training

```{toctree}
:hidden:

Overview <overview>
PyTorch guide <getting-started-pytorch>
PyTorch Lightning guide <getting-started-pytorch-lightning>
Hugging Face Transformers guide <getting-started-transformers>
XGBoost guide <getting-started-xgboost>
JAX guide <getting-started-jax>
more-frameworks
User guides <user-guides/index>
Tutorials </_collections/train/tutorials/README>
Examples <examples>
Benchmarks <benchmarks>
```

::::{div} sd-d-flex-row sd-align-major-center sd-align-minor-center
:::{div} sd-w-50
```{raw} html
:file: images/logo.svg
```
:::
::::

Ray Train is a scalable machine learning library for distributed training and fine-tuning.

Use Ray Train to scale model training code from a single machine to a cluster of machines in the cloud, whether you have large models or large datasets. Ray Train abstracts away the complexities of distributed computing.

Ray Train supports many frameworks, including the following:

```{list-table}
:widths: 1 1
:header-rows: 1

* - PyTorch ecosystem
  - More frameworks
* - PyTorch
  - TensorFlow
* - PyTorch Lightning
  - Keras
* - Hugging Face Transformers
  - Horovod
* - Hugging Face Accelerate
  - XGBoost
* - DeepSpeed
  - LightGBM
```

## Install Ray Train

To install Ray Train, run the following command:

```console
$ pip install -U "ray[train]"
```

To learn more about installing Ray and its libraries, see {ref}`Installing Ray <installation>`.

## Get started

::::{grid} 1 2 2 2
:gutter: 1
:class-container: container pb-6

:::{grid-item-card}
**Overview**
^^^

Understand the key concepts for distributed training with Ray Train.

+++
```{button-ref} train-overview
:color: primary
:outline:
:expand:

Learn the basics
```
:::

:::{grid-item-card}
**PyTorch**
^^^

Get started with distributed model training using Ray Train and PyTorch.

+++
```{button-ref} train-pytorch
:color: primary
:outline:
:expand:

Try Ray Train with PyTorch
```
:::

:::{grid-item-card}
**PyTorch Lightning**
^^^

Get started with distributed model training using Ray Train and Lightning.

+++
```{button-ref} train-pytorch-lightning
:color: primary
:outline:
:expand:

Try Ray Train with Lightning
```
:::

:::{grid-item-card}
**Hugging Face Transformers**
^^^

Get started with distributed model training using Ray Train and Transformers.

+++
```{button-ref} train-pytorch-transformers
:color: primary
:outline:
:expand:

Try Ray Train with Transformers
```
:::

:::{grid-item-card}
**JAX**
^^^

Get started with distributed model training using Ray Train and JAX.

+++
```{button-ref} train-jax
:color: primary
:outline:
:expand:

Try Ray Train with JAX
```
:::
::::

## Learn more

::::{grid} 1 2 2 2
:gutter: 1
:class-container: container pb-6

:::{grid-item-card}
**More frameworks**
^^^

Don't see your framework? See the guides for more frameworks.

+++
```{button-ref} train-more-frameworks
:color: primary
:outline:
:expand:

Try Ray Train with other frameworks
```
:::

:::{grid-item-card}
**User guides**
^^^

Get how-to instructions for common training tasks with Ray Train.

+++
```{button-ref} train-user-guides
:color: primary
:outline:
:expand:

Read how-to guides
```
:::

:::{grid-item-card}
**Tutorials**
^^^

Work through hands-on tutorials that cover ML workload patterns, from vision to recommendation systems.

+++
```{button-ref} /_collections/train/tutorials/README
:color: primary
:outline:
:expand:
:ref-type: doc

Follow tutorials
```
:::

:::{grid-item-card}
**Examples**
^^^

Browse end-to-end code examples for different use cases.

+++
```{button-ref} examples
:color: primary
:outline:
:expand:
:ref-type: doc

Learn through examples
```
:::

:::{grid-item-card}
**API**
^^^

See the API reference for full descriptions of the Ray Train API.

+++
```{button-ref} train-api
:color: primary
:outline:
:expand:

Read the API reference
```
:::
::::
