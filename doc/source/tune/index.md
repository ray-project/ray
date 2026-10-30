---
myst:
  html_meta:
    description: "Hyperparameter tuning and experiment execution at any scale, with algorithms such as PBT and ASHA and integrations for PyTorch, XGBoost, and Optuna."
---

(tune-main)=

# Ray Tune: Hyperparameter tuning

```{toctree}
:hidden:

Getting started <getting-started>
Key concepts <key-concepts>
tutorials/index
examples/index
faq
```

```{image} images/tune_overview.png
:scale: 50%
:align: center
```

Ray Tune is a Python library for experiment execution and hyperparameter tuning at any scale. You can tune models built with {ref}`PyTorch <tune-pytorch-cifar-ref>`, {ref}`XGBoost <tune-xgboost-ref>`, {doc}`TensorFlow and Keras <examples/tune-mnist-keras>`, and {doc}`other machine learning frameworks <examples/index>` by running state-of-the-art algorithms such as {ref}`Population Based Training (PBT) <tune-scheduler-pbt>` and {ref}`Hyperband and ASHA <tune-scheduler-hyperband>`. Tune also integrates with many other hyperparameter optimization tools, including {doc}`Ax <examples/ax-example>`, {doc}`BayesOpt <examples/bayesopt-example>`, {doc}`BOHB <examples/bohb-example>`, {doc}`Nevergrad <examples/nevergrad-example>`, and {doc}`Optuna <examples/optuna-example>`.

Select a tab to see code examples for different machine learning frameworks.

:::::{tab-set}

::::{tab-item} Quickstart

To run this example, install Ray Tune with `pip install "ray[tune]"`.

This quickstart example minimizes the `objective` function, which computes `a**2 + b`. The closer `a` is to zero and the smaller `b` is, the smaller the result. You define a *search space* for `a` and `b`, and Ray Tune explores the space for good values.

:::{callout}
```{literalinclude} ../../../python/ray/tune/tests/example.py
:language: python
:start-after: __quick_start_begin__
:end-before: __quick_start_end__
```

```{annotations}
<1> Define an objective function.

<2> Define a search space.

<3> Start a Tune run and print the best result.
```
:::
::::

::::{tab-item} Keras and Hyperopt

To tune a Keras model with Hyperopt, wrap the model in an objective function and use the function's `config` to select hyperparameters. This example tunes only the `activation` parameter of the model's first layer, but you can tune any model parameter. After you define the search space, initialize the `HyperOptSearch` object and pass it to `Tuner` as the `search_alg` argument of `tune.TuneConfig`. Tell Ray Tune which metric to optimize and whether to maximize or minimize it.

:::{callout}
```{literalinclude} doc_code/keras_hyperopt.py
:language: python
:start-after: __keras_hyperopt_start__
:end-before: __keras_hyperopt_end__
```

```{annotations}
<1> Wrap a Keras model in an objective function.

<2> Define a search space and initialize the search algorithm.

<3> Start a Tune run that maximizes accuracy.
```
:::
::::

::::{tab-item} PyTorch and Optuna

To tune a PyTorch model with Optuna, wrap the model in an objective function and use the function's `config` to select hyperparameters. This example tunes only the `momentum` and `lr` parameters of the model's optimizer, where `lr` is the learning rate, but you can tune any other model parameter. After you define the search space, initialize the `OptunaSearch` object and pass it to `Tuner` as the `search_alg` argument of `tune.TuneConfig`. Tell Ray Tune which metric to optimize and whether to maximize or minimize it. This example stops each trial after five iterations, but you can define other stopping rules.

:::{callout}
```{literalinclude} doc_code/pytorch_optuna.py
:language: python
:start-after: __pytorch_optuna_start__
:end-before: __pytorch_optuna_end__
```

```{annotations}
<1> Wrap a PyTorch model in an objective function.

<2> Define a search space and initialize the search algorithm.

<3> Start a Tune run that maximizes mean accuracy and stops after five iterations.
```
:::
::::

:::::

With Tune, you can also launch a multi-node {ref}`distributed hyperparameter sweep <tune-distributed-ref>` in fewer than 10 lines of code. You can then move your models from training to serving on the same infrastructure with {doc}`Ray Serve </serve/index>`.

::::{grid} 1 2 3 4
:gutter: 1
:class-container: container pb-3

:::{grid-item-card}
**Getting started**
^^^

Learn how to tune a PyTorch model with Tune in the getting started tutorial.

+++
```{button-ref} tune-tutorial
:color: primary
:outline:
:expand:

Get started with Tune
```
:::

:::{grid-item-card}
**Key concepts**
^^^

Understand the key concepts behind Ray Tune. Learn about trials, search algorithms, schedulers, and other features.

+++
```{button-ref} tune-60-seconds
:color: primary
:outline:
:expand:

Tune's key concepts
```
:::

:::{grid-item-card}
**User guides**
^^^

The user guides cover key features of Tune, such as distributed training and early stopping.

+++
```{button-ref} tune-guides
:color: primary
:outline:
:expand:

Learn how to use Tune
```
:::

:::{grid-item-card}
**Examples**
^^^

The examples include practical tutorials for frameworks such as scikit-learn, Keras, TensorFlow, PyTorch, and MLflow, and for state-of-the-art search algorithm integrations.

+++
```{button-ref} tune-examples-ref
:color: primary
:outline:
:expand:

Ray Tune examples
```
:::

:::{grid-item-card}
**Ray Tune FAQ**
^^^

Find answers to common questions in the FAQ.

+++
```{button-ref} tune-faq
:color: primary
:outline:
:expand:

Ray Tune FAQ
```
:::

:::{grid-item-card}
**Ray Tune API**
^^^

Get detailed information about the Ray Tune API, including search spaces, algorithms, and training configurations.

+++
```{button-ref} tune-api-ref
:color: primary
:outline:
:expand:

Read the API reference
```
:::
::::

## Why choose Tune?

Many other hyperparameter optimization libraries exist. If you're new to Tune, you're probably wondering what makes Tune different.

:::{dropdown} Cutting-edge optimization algorithms
:animate: fade-in-slide-down

You're probably looking into hyperparameter optimization because you want to improve your model performance quickly.

With Tune, you can use a variety of cutting-edge optimization algorithms that reduce the cost of tuning by {ref}`terminating bad trials early <tune-scheduler-hyperband>`, {ref}`choosing better parameters to evaluate <tune-search-alg>`, or even {ref}`changing the hyperparameters during training <tune-scheduler-pbt>` to optimize schedules.
:::

:::{dropdown} First-class developer productivity
:animate: fade-in-slide-down

Many hyperparameter optimization frameworks require you to restructure your code to fit the framework. With Tune, you can optimize your model by {ref}`adding a few code snippets <tune-tutorial>`.

Tune also removes boilerplate from your training workflow. It supports {ref}`multiple storage options for experiment results <tune-storage-options>`, such as NFS and cloud storage, and {ref}`logs results to tools <tune-logging>` such as MLflow and TensorBoard. Tune is also highly customizable.
:::

:::{dropdown} Multi-GPU and distributed training out of the box
:animate: fade-in-slide-down

Hyperparameter tuning is highly time-consuming, so you often need to parallelize it. Most other tuning frameworks require you to implement your own multi-process framework or build your own distributed system to speed up hyperparameter tuning.

With Tune, you can transparently {ref}`parallelize across multiple GPUs and multiple nodes <tune-parallelism>`. Tune also has {ref}`fault tolerance and cloud support <tune-distributed-ref>`, so you can scale up your hyperparameter search by 100x while reducing costs by up to 10x with cheap preemptible instances.
:::

:::{dropdown} Coming from another hyperparameter optimization tool?
:animate: fade-in-slide-down

You might already use a hyperparameter tuning tool such as Hyperopt or Bayesian Optimization.

If so, you can use Tune to power up your existing workflow. Tune's {ref}`search algorithms <tune-search-alg>` integrate with a variety of popular hyperparameter tuning libraries, so you can scale up your optimization process without sacrificing performance. For integration code, see the {ref}`examples <tune-examples-ref>`.
:::

## Projects using Tune

To add a project to this list or to request the removal of a listed project, submit a pull request. The following are some of the popular open source repositories and research projects that use Tune:

- [Softlearning](https://github.com/rail-berkeley/softlearning): Softlearning is a reinforcement learning framework for training maximum entropy policies in continuous domains. It includes the official implementation of the Soft Actor-Critic algorithm.
- [Flambe](https://github.com/asappresearch/flambe): Flambe is a machine learning framework that accelerates research and its path to production. See [flambe.ai](https://flambe.ai).
- [Population Based Augmentation](https://github.com/arcelien/pba): Population Based Augmentation (PBA) is an algorithm that quickly and efficiently learns data augmentation functions for neural network training. PBA matches state-of-the-art results on CIFAR with one thousand times less compute.
- [Fast AutoAugment by Kakao](https://github.com/kakaobrain/fast-autoaugment): Fast AutoAugment, accepted at NeurIPS 2019, learns augmentation policies using a more efficient search strategy based on density matching.
- [Allentune](https://github.com/allenai/allentune): Allentune is a hyperparameter search tool for AllenNLP from AllenAI.
- [machinable](https://github.com/frthjf/machinable): The machinable project is a modular configuration system for machine learning research. See [machinable.org](https://machinable.org).
- [NeuroCard](https://github.com/neurocard/neurocard): NeuroCard, accepted at VLDB 2021, is a neural cardinality estimator for multi-table join queries. It uses state-of-the-art deep density models to learn correlations across relational database tables.

## Learn more about Ray Tune

The following blog posts and talks cover Ray Tune:

- [Tune: a Python library for fast hyperparameter tuning at any scale](https://medium.com/data-science/fast-hyperparameter-tuning-at-scale-d428223b081c), a blog post
- [Cutting edge hyperparameter tuning with Ray Tune](https://medium.com/riselab/cutting-edge-hyperparameter-tuning-with-ray-tune-be6c0447afdf), a blog post
- [Slides from a talk at RISECamp 2019](https://docs.google.com/presentation/d/1v3IldXWrFNMK-vuONlSdEuM82fuGTrNUDuwtfx4axsQ/edit?usp=sharing)
- [Video of a talk at RISECamp 2018](https://www.youtube.com/watch?v=38Yd_dXW51Q)
- [A Guide to Modern Hyperparameter Optimization](https://www.youtube.com/watch?v=10uz5U3Gy6E), a video of a talk at PyData LA 2019, with [slides](https://speakerdeck.com/richardliaw/a-modern-guide-to-hyperparameter-optimization)

## Cite Tune

If Tune helps your academic research, consider citing the [Tune paper](https://arxiv.org/abs/1807.05118). The following example shows a BibTeX entry:

```tex
@article{liaw2018tune,
    title={Tune: A Research Platform for Distributed Model Selection and Training},
    author={Liaw, Richard and Liang, Eric and Nishihara, Robert
            and Moritz, Philipp and Gonzalez, Joseph E and Stoica, Ion},
    journal={arXiv preprint arXiv:1807.05118},
    year={2018}
}
```
