---
myst:
  html_meta:
    description: "Core Ray Tune abstractions: trainables (function and class APIs), search spaces, search algorithms, schedulers, Tuner configuration, trials, and ResultGrid."
---

(tune-60-seconds)=

# Key concepts of Ray Tune

<!-- TODO: should we introduce checkpoints as well? -->
<!-- TODO: should we at least mention "Stopper" classes here? -->

This page walks through the key concepts you need to know to use Ray Tune. For practical tutorials, see the {ref}`user guides <tune-guides>`. Tune has six components that you need to understand.

First, you define the hyperparameters you want to tune in a *search space* and pass them into a *trainable* that specifies the objective you want to tune. Then you select a *search algorithm* to optimize your parameters, and optionally use a *scheduler* to stop searches early and speed up your experiments. You pass your trainable, search algorithm, and scheduler, together with other configuration, into the `Tuner`, which runs your experiment and creates *trials*. The `Tuner` returns a `ResultGrid` that you use to inspect your experiment results. The following figure shows an overview of these components. The next sections cover each one in detail.

```{image} images/tune_flow.png
:alt: Flow diagram of the Tune components. Search spaces feed into trainables. Trainables, search algorithms, and schedulers feed into trials, which make up your experiment. Trials feed into analyses of your results.
```

(tune_60_seconds_trainables)=

## Ray Tune trainables

A {ref}`trainable <trainable-docs>` is an object that you can pass into a Tune run. You can define a trainable with either the {ref}`function API <tune-function-api>` or the {ref}`class API <tune-class-api>`. Both are valid, but the function API is the generally recommended choice, and the rest of this guide uses it.

Consider an example that optimizes the simple objective function `a * (x ** 2) + b`, where `a` and `b` are the hyperparameters you tune to minimize the objective. Because the objective also has a variable `x`, you need to test different values of `x`. Given concrete values for `a`, `b`, and `x`, you can evaluate the objective function and get a `score` to minimize.

:::::{tab-set}
:::{tab-item} Function API
With the {ref}`function API <tune-function-api>`, you create a function that takes a dictionary of hyperparameters. The following example names the function `trainable`. It computes a `score` in a "training loop" and reports the score back to Tune:

```{literalinclude} doc_code/key_concepts.py
:language: python
:start-after: __function_api_start__
:end-before: __function_api_end__
```

This example uses `tune.report(...)` to report the intermediate `score` in the training loop, which can be useful in many machine learning tasks. To report only the final `score` outside of this loop, return the score at the end of the `trainable` function with `return {"score": score}`. You can also use `yield {"score": score}` instead of `tune.report()`.
:::

::::{tab-item} Class API
The following example specifies the objective function with the {ref}`class API <tune-class-api>`:

```{literalinclude} doc_code/key_concepts.py
:language: python
:start-after: __class_api_start__
:end-before: __class_api_end__
```

:::{note}
You can't use `tune.report` within a `Trainable` class.
:::
::::
:::::

For more details, see the {ref}`trainable documentation <trainable-docs>` and the {ref}`other Tune examples <tune-examples-others>`. The next section looks at the `config` dictionary that you pass into your trainables.

(tune-key-concepts-search-spaces)=

## Tune search spaces

To optimize your hyperparameters, you have to define a *search space*. A search space defines valid values for your hyperparameters and can specify how these values are sampled, such as from a uniform distribution or a normal distribution.

Tune offers various functions to define search spaces and sampling methods. For the full explanation of each function, see the {ref}`Tune search space API <tune-search-space>`.

The following example covers all the search space functions:

```{literalinclude} doc_code/key_concepts.py
:language: python
:start-after: __config_start__
:end-before: __config_end__
```

(tune_60_seconds_trials)=

## Tune trials

Use {ref}`Tuner.fit <tune-run-ref>` to run and manage hyperparameter tuning and generate your trials. At a minimum, your `Tuner` call takes a trainable as the first argument and a `param_space` dictionary that defines the search space.

`Tuner.fit()` also provides many features, such as {ref}`logging <tune-logging>`, {ref}`checkpointing <tune-trial-checkpoint>`, and {ref}`early stopping <tune-stopping-ref>`. The following example shows a basic Tune run for the example objective `a * (x ** 2) + b`, with a simple search space for `a` and `b`:

```{literalinclude} doc_code/key_concepts.py
:language: python
:start-after: __run_tunable_start__
:end-before: __run_tunable_end__
```

`Tuner.fit` generates a couple of hyperparameter configurations from its arguments and wraps them into {ref}`Trial objects <trial-docstring>`.

Trials contain a lot of information. For example, you can get the hyperparameter configuration from `trial.config`, the trial ID from `trial.trial_id`, the trial's resource specification from `resources_per_trial` or `trial.placement_group_factory`, and many other values.

By default, `Tuner.fit` runs until all trials stop or error. The following example shows the output of a trial run:

<!-- TODO: how to make sure this doesn't get outdated? -->
```bash
== Status ==
Memory usage on this node: 11.4/16.0 GiB
Using FIFO scheduling algorithm.
Resources requested: 1/12 CPUs, 0/0 GPUs, 0.0/3.17 GiB heap, 0.0/1.07 GiB objects
Result logdir: /Users/foo/ray_results/myexp
Number of trials: 1 (1 RUNNING)
+----------------------+----------+---------------------+-----------+--------+--------+----------------+-------+
| Trial name           | status   | loc                 |         a |      b |  score | total time (s) |  iter |
|----------------------+----------+---------------------+-----------+--------+--------+----------------+-------|
| Trainable_a826033a | RUNNING  | 10.234.98.164:31115 | 0.303706  | 0.0761 | 0.1289 |        7.54952 |    15 |
+----------------------+----------+---------------------+-----------+--------+--------+----------------+-------+
```

To run 10 trials, specify the number of samples with `num_samples`. Tune automatically {ref}`determines how many trials run in parallel <tune-parallelism>`. Instead of the number of samples, you can also specify a time budget in seconds with `time_budget_s` if you set `num_samples=-1`.

```{literalinclude} doc_code/key_concepts.py
:language: python
:start-after: __run_tunable_samples_start__
:end-before: __run_tunable_samples_end__
```

Finally, you can optimize your hyperparameters with more complex search spaces, such as random samples or grid search, through Tune's {ref}`search space API <tune-default-search-space>`. The following example samples `a` and `b` uniformly between `[0, 1]`:

```{literalinclude} doc_code/key_concepts.py
:language: python
:start-after: __search_space_start__
:end-before: __search_space_end__
```

To learn more about the ways to configure your Tune runs, see the {ref}`Tuner API reference <tune-run-ref>`.

(search-alg-ref)=

## Tune search algorithms

To optimize the hyperparameters of your training process, use a {ref}`search algorithm <tune-search-alg>`, which suggests hyperparameter configurations. If you don't specify a search algorithm, Tune uses random search by default, which can give you a good starting point for your hyperparameter optimization.

For example, to use Tune with simple Bayesian optimization through the `bayesian-optimization` package, first run `pip install bayesian-optimization`. Then define an `algo` with `BayesOptSearch`, pass it as the `search_alg` argument to `tune.TuneConfig`, and pass the `tune.TuneConfig` to the `Tuner`:

```{literalinclude} doc_code/key_concepts.py
:language: python
:start-after: __bayes_start__
:end-before: __bayes_end__
```

Tune has search algorithms that integrate with many popular optimization libraries, such as {ref}`Hyperopt <tune-hyperopt>` and {ref}`Optuna <tune-optuna>`. Tune automatically converts the search space you provide into the search spaces that the search algorithms and underlying libraries expect. For more details, see the {ref}`search algorithm API documentation <tune-search-alg>`.

The following table gives an overview of all available search algorithms in Tune:

```{list-table}
:widths: 5 5 2 10
:header-rows: 1

* - Search algorithm
  - Summary
  - Website
  - Code example
* - {ref}`Random search and grid search <tune-basicvariant>`
  - Random search or grid search
  -
  - {doc}`/tune/examples/includes/tune-basic-example`
* - {ref}`AxSearch <tune-ax>`
  - Bayesian or bandit optimization
  - [Ax](https://ax.dev/)
  - {doc}`/tune/examples/includes/ax-example`
* - {ref}`HyperOptSearch <tune-hyperopt>`
  - Tree-structured Parzen Estimator
  - [Hyperopt](http://hyperopt.github.io/hyperopt)
  - {doc}`/tune/examples/hyperopt-example`
* - {ref}`BayesOptSearch <bayesopt>`
  - Bayesian optimization
  - [BayesianOptimization](https://github.com/fmfn/BayesianOptimization)
  - {doc}`/tune/examples/includes/bayesopt-example`
* - {ref}`TuneBOHB <suggest-TuneBOHB>`
  - Bayesian optimization and Hyperband
  - [BOHB](https://github.com/automl/HpBandSter)
  - {doc}`/tune/examples/includes/bohb-example`
* - {ref}`NevergradSearch <nevergrad>`
  - Gradient-free optimization
  - [Nevergrad](https://github.com/facebookresearch/nevergrad)
  - {doc}`/tune/examples/includes/nevergrad-example`
* - {ref}`OptunaSearch <tune-optuna>`
  - Optuna search algorithms
  - [Optuna](https://optuna.org/)
  - {doc}`/tune/examples/optuna-example`
```

:::{note}
Unlike {ref}`Tune trial schedulers <tune-schedulers>`, Tune search algorithms can't affect or stop training processes. However, you can use the two together to stop the evaluation of bad trials early.
:::

To implement your own search algorithm, see {ref}`custom search algorithms <byo-algo>`.

Tune also provides the following utilities to use with search algorithms:

* {ref}`repeater`: Runs each sampled hyperparameter with multiple random seeds.
* {ref}`limiter`: Limits the number of concurrent trials during optimization.
* {ref}`shim`: Creates the search algorithm object from a string.

The preceding Bayesian optimization example tells Tune to `stop` after `20` training iterations. Stopping trials with explicit rules is useful, but in many cases you can do even better with schedulers.

(schedulers-ref)=

## Tune schedulers

To make your training process more efficient, you can use a {ref}`trial scheduler <tune-schedulers>`. For example, the `trainable` example on this page minimizes a function in a training loop and calls `tune.report()` to report incremental results for a hyperparameter configuration that a search algorithm selected. Based on these reported results, a scheduler can decide whether to stop the trial early. If you don't specify a scheduler, Tune uses a first-in, first-out (FIFO) scheduler by default. The FIFO scheduler passes through the trials that your search algorithm selects in the order the search algorithm picks them, and doesn't perform any early stopping.

Schedulers can stop, pause, or tweak the hyperparameters of running trials, which can make your hyperparameter tuning process much faster. Unlike search algorithms, trial schedulers don't select which hyperparameter configurations to evaluate.

All schedulers take a `metric`, which is the value your trainable reports. The `metric` is then maximized or minimized according to the `mode` you provide. To use a scheduler, pass a `scheduler` argument to `tune.TuneConfig`, and pass the `tune.TuneConfig` to the `Tuner`. The following example uses the Hyperband scheduler, `HyperBandScheduler`:

```{literalinclude} doc_code/key_concepts.py
:language: python
:start-after: __hyperband_start__
:end-before: __hyperband_end__
```

Tune includes distributed implementations of early stopping algorithms such as the [median stopping rule](https://research.google.com/pubs/pub46180.html), [Hyperband](https://arxiv.org/abs/1603.06560), and [ASHA](https://openreview.net/forum?id=S1Y7OOlRZ). Tune also includes a distributed implementation of [Population Based Training (PBT)](https://www.deepmind.com/blog/population-based-training-of-neural-networks) and [Population Based Bandits (PB2)](https://arxiv.org/abs/2002.02518).

:::{tip}
The easiest scheduler to start with is `ASHAScheduler`, which aggressively stops low-performing trials.
:::

Schedulers can dynamically change trial resource requirements during tuning. {ref}`ResourceChangingScheduler <tune-resource-changing-scheduler>` implements this behavior and can wrap around any other scheduler.

When you use schedulers, you might run into compatibility issues, as the following compatibility matrix shows. You can't use certain schedulers with search algorithms, and certain schedulers require that you implement {ref}`checkpointing <tune-trial-checkpoint>`.

```{list-table} Scheduler compatibility matrix
:header-rows: 1

* - Scheduler
  - Needs checkpointing?
  - Compatible with search algorithms?
  - Example
* - {ref}`ASHA <tune-scheduler-hyperband>`
  - No
  - Yes
  - {doc}`ASHA example </tune/examples/includes/async-hyperband-example>`
* - {ref}`Median stopping rule <tune-scheduler-msr>`
  - No
  - Yes
  - {ref}`Median stopping rule reference <tune-scheduler-msr>`
* - {ref}`Hyperband <tune-original-hyperband>`
  - Yes
  - Yes
  - {doc}`Hyperband example </tune/examples/includes/hyperband-example>`
* - {ref}`BOHB <tune-scheduler-bohb>`
  - Yes
  - Only `TuneBOHB`
  - {doc}`BOHB example </tune/examples/includes/bohb-example>`
* - {ref}`Population Based Training <tune-scheduler-pbt>`
  - Yes
  - No
  - {doc}`PBT example </tune/examples/includes/pbt-function>`
* - {ref}`Population Based Bandits <tune-scheduler-pb2>`
  - Yes
  - No
  - {doc}`Basic example </tune/examples/includes/pb2-example>`, {doc}`PPO example </tune/examples/includes/pb2-ppo-example>`
```

To learn more about trial schedulers, see the {ref}`scheduler API documentation <tune-schedulers>`.

(tune-concepts-analysis)=

## Tune ResultGrid

`Tuner.fit()` returns a {ref}`ResultGrid <tune-analysis-docs>` object with methods that you can use to analyze your training. The following example shows how to access various metrics from a `ResultGrid` object, such as the best available trial or the best hyperparameter configuration for that trial:

```{literalinclude} doc_code/key_concepts.py
:language: python
:start-after: __analysis_start__
:end-before: __analysis_end__
```

This object can also retrieve all training runs as DataFrames, so you can do ad-hoc data analysis over your results.

```{literalinclude} doc_code/key_concepts.py
:language: python
:start-after: __results_start__
:end-before: __results_end__
```

See the {ref}`result analysis user guide <tune-analysis-guide>` for more usage examples.

## What's next?

Now that you have a working understanding of Tune, see the following resources:

* {ref}`tune-guides`: Tutorials for using Tune with your preferred machine learning library.
* {doc}`/tune/examples/index`: End-to-end examples and templates for using Tune with your preferred machine learning library.
* {doc}`/tune/getting-started`: A tutorial that walks you through setting up a Tune experiment.

### Further questions or issues?

```{include} /_includes/_help-links.md
```
