---
myst:
  html_meta:
    description: "Define stopping criteria for a Tune experiment: metric thresholds, per-trial and experiment timeouts, failure limits, and scheduler early stopping."
---

(tune-stopping-guide)=
(tune-stopping-ref)=

# How to define stopping criteria for a Ray Tune experiment

When you run a Ray Tune experiment, it can be hard to know in advance how long training should last. Use stopping criteria to end training when specific conditions hold.

For example, you might want to stop in any of the following cases:

- End each trial after `N` epochs or when the reported evaluation score passes a threshold, whichever comes first.
- Stop the experiment after `T` seconds.
- Stop the experiment when trials encounter runtime errors.
- Stop underperforming trials early with Tune's early-stopping schedulers.

This guide shows how to set up each of these stopping criteria in a Tune experiment.

All the code examples use the following training function:

```{literalinclude} /tune/doc_code/stopping.py
:language: python
:start-after: __stopping_example_trainable_start__
:end-before: __stopping_example_trainable_end__
```

## Stop a Tune experiment manually

To stop an experiment manually, send a `SIGINT` signal to the process running {meth}`Tuner.fit() <ray.tune.Tuner.fit>`. Pressing `Ctrl+C` in the terminal usually does this. Ray Tune then shuts down training gracefully and saves the final experiment state.

:::{note}
If you force a Tune experiment to stop, for example by pressing `Ctrl+C` several times, Tune can't snapshot the experiment state one last time. If you later resume the experiment, it might resume from stale state.
:::

Ray Tune also accepts the `SIGUSR1` signal to interrupt training gracefully. Use it when you run Ray Tune in a remote Ray task, because Ray filters out `SIGINT` and `SIGTERM` signals by default.

## Stop with metric-based criteria

Besides manual stopping, Tune provides several ways to stop experiments programmatically. The simplest is metric-based criteria, a fixed set of thresholds that determine when to stop.

You can define the stopping criteria with a dictionary, a function, or a custom {class}`Stopper <ray.tune.stopper.Stopper>`.

:::::{tab-set}
::::{tab-item} Dictionary
If you pass a dictionary, its keys can be any field in the results that you report with `tune.report` in the function API or return from `step()` in the class API.

:::{note}
This includes {ref}`auto-filled metrics <tune-autofilled-metrics>` such as `training_iteration`.
:::

In the following example, each trial stops when it completes `10` iterations or when it reaches a mean accuracy of `0.8` or more, whichever comes first.

Tune assumes these metrics are increasing, so a trial stops once the reported metric reaches or exceeds the threshold in the dictionary.

```{literalinclude} /tune/doc_code/stopping.py
:language: python
:start-after: __stopping_dict_start__
:end-before: __stopping_dict_end__
```
::::

::::{tab-item} User-defined function
For more flexibility, you can pass a function instead. The function must take `(trial_id: str, result: dict)` as arguments and return `True` if the trial should stop or `False` otherwise.

In the following example, each trial stops when it completes `10` iterations or when it reaches a mean accuracy of `0.8` or more, whichever comes first.

```{literalinclude} /tune/doc_code/stopping.py
:language: python
:start-after: __stopping_fn_start__
:end-before: __stopping_fn_end__
```
::::

::::{tab-item} Custom stopper class
You can implement the {class}`~ray.tune.stopper.Stopper` interface to stop individual trials or entire experiments based on custom stopping criteria. For example, the following stopper stops all trials once any individual trial meets the criteria, and prevents new trials from starting:

```{literalinclude} /tune/doc_code/stopping.py
:language: python
:start-after: __stopping_cls_start__
:end-before: __stopping_cls_end__
```

In this example, once any trial reaches a `mean_accuracy` of `0.8` or more, all trials stop.

:::{note}
When `stop_all` returns `True`, running trials don't stop immediately. Each one stops at the end of its ongoing training iteration, after its next `tune.report` call or `step`.
:::

Ray Tune includes a set of built-in stopper classes. See the {ref}`Stopper <tune-stoppers>` documentation.
::::
:::::

## Stop after a time limit

To stop based on time, you can either stop each trial individually after a timeout or stop the whole experiment after a set amount of time.

### Stop trials individually with a timeout

You can use a dictionary of stopping criteria, as described earlier, with the `time_total_s` metric that Tune fills in automatically.

```{literalinclude} /tune/doc_code/stopping.py
:language: python
:start-after: __stopping_trials_by_time_start__
:end-before: __stopping_trials_by_time_end__
```

:::{note}
If you use the {ref}`function trainable API <tune-function-api>`, include some intermediate reporting through {meth}`tune.report <ray.tune.report>`. Each report automatically records the trial's `time_total_s`, so Tune can use time as a stopping metric.

If the training loop hangs, Tune can't intercept the training and stop the trial for you. In that case, you can implement timeout logic explicitly in the training loop.
:::

### Stop the experiment with a timeout

To stop the experiment after `time_budget_s` seconds, set `time_budget_s` in `tune.TuneConfig`.

```{literalinclude} /tune/doc_code/stopping.py
:language: python
:start-after: __stopping_experiment_by_time_start__
:end-before: __stopping_experiment_by_time_end__
```

:::{note}
If you use the {ref}`function trainable API <tune-function-api>`, include some intermediate reporting through {meth}`tune.report <ray.tune.report>`, for the same reason as in the preceding section.
:::

## Stop on trial failures

Besides stopping trials based on their performance, you can stop the entire experiment if any trial encounters a runtime error. To do this, use the {class}`ray.tune.FailureConfig` class.

With `FailureConfig(fail_fast=True)`, the entire experiment stops immediately if any trial encounters an error.

```{literalinclude} /tune/doc_code/stopping.py
:language: python
:start-after: __stopping_on_trial_error_start__
:end-before: __stopping_on_trial_error_end__
```

This is useful when you're debugging a Tune experiment with many trials.

## Stop trials early with Tune schedulers

You can also stop trials with early-stopping schedulers. These schedulers monitor trial performance and stop trials early if they aren't making sufficient progress.

{class}`~ray.tune.schedulers.AsyncHyperBandScheduler` and {class}`~ray.tune.schedulers.HyperBandForBOHB` are examples of early-stopping schedulers built into Tune. For a full list and more realistic examples, see {ref}`the Tune scheduler API reference <tune-schedulers>`.

The following example combines a dictionary of stopping criteria with an early-stopping scheduler:

```{literalinclude} /tune/doc_code/stopping.py
:language: python
:start-after: __early_stopping_start__
:end-before: __early_stopping_end__
```

## Summary

This guide covered how to stop Tune experiments manually, with metrics, after a time limit, on trial errors, and with early-stopping schedulers.

See the following resources for more information:

- For the built-in stopper classes, see the {ref}`Tune Stopper API reference <tune-stoppers>`.
- If you interrupt an experiment manually, or the cluster dies unexpectedly while trials are still running, you can resume the experiment. See {ref}`tune-fault-tolerance-ref`.
