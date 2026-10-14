---
myst:
  html_meta:
    description: "Tutorial tuning a PyTorch CNN with the Tuner API, ASHAScheduler for early stopping, and Hyperopt for Bayesian search."
---

<!-- TODO: make this an executable notebook later on. -->

(tune-tutorial)=

# Get started with Ray Tune

This tutorial walks you through setting up a Ray Tune experiment. Starting from a PyTorch model, you use Tune to optimize the model's hyperparameters with early stopping and with Bayesian optimization through Hyperopt.

:::{tip}
If you have suggestions on how to improve this tutorial, [open an issue on GitHub](https://github.com/ray-project/ray/issues/new/choose).
:::

To run this example, install the following packages:

```bash
$ pip install "ray[tune]" torch torchvision
```

## Set up a PyTorch model to tune

First, import the dependencies. The PyTorch and torchvision modules help you create and train a model, and Ray Tune helps you optimize it. The imports also include a scheduler, in this case the `ASHAScheduler`, which you use to tune the model later in this tutorial.

```{literalinclude} /../../python/ray/tune/tests/tutorial.py
:language: python
:start-after: __tutorial_imports_begin__
:end-before: __tutorial_imports_end__
```

Next, define a simple PyTorch model to train. If you're not familiar with PyTorch, the simplest way to define a model is to implement a `nn.Module`. Set up the model in `__init__`, and then implement the `forward` pass. This example uses a small convolutional neural network with one 2D convolutional layer, a fully connected layer, and a softmax function.

```{literalinclude} /../../python/ray/tune/tests/tutorial.py
:language: python
:start-after:  __model_def_begin__
:end-before:  __model_def_end__
```

The following `train` and `test` functions train and evaluate your PyTorch model. If you already know how to write them, skip ahead to the next section.

:::{dropdown} Training and evaluating the model
```{literalinclude} /../../python/ray/tune/tests/tutorial.py
:language: python
:start-after: __train_def_begin__
:end-before: __train_def_end__
```
:::

(tutorial-tune-setup)=

## Set up a `Tuner` for a training run with Tune

The following function trains the PyTorch model for multiple epochs. This function runs in a separate process as a {ref}`Ray actor <actor-guide>`, so you need to report the model's performance back to Tune, which runs in the main Python process.

To do this, call {func}`tune.report() <ray.tune.report>` in the training function, which sends the performance value back to Tune. Because the function runs in a separate process, make sure it's {ref}`serializable by Ray <serialization-guide>`.

```{literalinclude} /../../python/ray/tune/tests/tutorial.py
:language: python
:start-after: __train_func_begin__
:end-before: __train_func_end__
```

Run one trial by calling {ref}`Tuner.fit <tune-run-ref>`, and {ref}`randomly sample <tune-search-space>` the learning rate and momentum from a uniform distribution.

```{literalinclude} /../../python/ray/tune/tests/tutorial.py
:language: python
:start-after: __eval_func_begin__
:end-before: __eval_func_end__
```

`Tuner.fit` returns a {ref}`ResultGrid object <tune-analysis-docs>`. You can use it to plot the trial's performance.

```{literalinclude} /../../python/ray/tune/tests/tutorial.py
:language: python
:start-after: __plot_begin__
:end-before: __plot_end__
```

:::{note}
Tune automatically runs trials in parallel across all available cores and GPUs on your machine or cluster. To limit the number of concurrent trials, use the {ref}`ConcurrencyLimiter <limiter>`.
:::

## Stop trials early with Asynchronous Successive Halving

Next, add early stopping to the optimization process with {ref}`ASHA <tune-scheduler-hyperband>`, a scalable algorithm for [principled early stopping](https://blog.ml.cmu.edu/2018/12/12/massively-parallel-hyperparameter-optimization/).

At a high level, ASHA stops less promising trials and allocates more time and resources to more promising ones. Because the optimization process becomes more efficient, you can afford to **increase the search space by 5x** by adjusting the `num_samples` parameter.

Tune implements ASHA as a trial scheduler, `ASHAScheduler`. Trial schedulers can stop bad trials early, pause trials, clone trials, and alter the hyperparameters of a running trial. For details on the available schedulers and library integrations, see {ref}`Tune trial schedulers <tune-schedulers>`.

```{literalinclude} /../../python/ray/tune/tests/tutorial.py
:language: python
:start-after: __run_scheduler_begin__
:end-before: __run_scheduler_end__
```

To visualize trial progress, you can run the following code in a Jupyter notebook.

```{literalinclude} /../../python/ray/tune/tests/tutorial.py
:language: python
:start-after: __plot_scheduler_begin__
:end-before: __plot_scheduler_end__
```

```{image} /images/tune-df-plot.png
:alt: Plot of trial progress.
:scale: 50%
:align: center
```

You can also visualize results with {ref}`TensorBoard <tensorboard>`.

```bash
$ tensorboard --logdir {logdir}
```

## Use search algorithms in Tune

In addition to {ref}`trial schedulers <tune-schedulers>`, you can further optimize your hyperparameters with an intelligent search technique such as Bayesian optimization. To do this, you can use a Tune {ref}`search algorithm <tune-search-alg>`. Search algorithms use optimization algorithms to navigate the given hyperparameter space intelligently.

Each library has its own way of defining the search space.

```{literalinclude} /../../python/ray/tune/tests/tutorial.py
:language: python
:start-after: __run_searchalg_begin__
:end-before: __run_searchalg_end__
```

:::{note}
You can combine some search algorithms with different trial schedulers. For details, see {ref}`Tune trial schedulers <tune-schedulers>`.
:::

## Evaluate your model after tuning

To evaluate the best trained model, retrieve it with the {ref}`ResultGrid <tune-analysis-docs>` object:

```{literalinclude} /../../python/ray/tune/tests/tutorial.py
:language: python
:start-after: __run_analysis_begin__
:end-before: __run_analysis_end__
```

## Next steps

* See the {ref}`Tune tutorials <tune-guides>` for guides on using Tune with your preferred machine learning library.
* Browse the {ref}`gallery of examples <tune-examples-others>` to see how to use Tune with PyTorch, XGBoost, TensorFlow, and other libraries.
* If you run into problems or have questions, [open an issue on GitHub](https://github.com/ray-project/ray/issues).
* To check on your application, you can use the {ref}`Ray dashboard <observability-getting-started>`.
