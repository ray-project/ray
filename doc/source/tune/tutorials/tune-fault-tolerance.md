---
myst:
  html_meta:
    description: "Enable experiment- and trial-level fault tolerance in Tune: restore a run, configure auto-resume, and recover from node and trial failures."
---

(tune-fault-tolerance-ref)=

# How to enable fault tolerance in Ray Tune

In distributed machine learning experiments, fault tolerance can help mitigate the impact of node failures caused by out-of-memory and out-of-disk issues.

Fault tolerance preserves training progress when a node fails, which saves time and resources. In a distributed setting, it also gives you access to the cost savings of preemptible spot instance nodes.

:::{seealso}
Before you enable fault tolerance in a *distributed* Tune experiment, configure persistent storage that consolidates all trial results and checkpoints. See {ref}`tune-storage-options`.
:::

This guide covers how to enable the different types of fault tolerance that Ray Tune offers.

(tune-experiment-level-fault-tolerance)=

## Experiment-level fault tolerance in Tune

At the experiment level, {meth}`Tuner.restore <ray.tune.Tuner.restore>` resumes a previously interrupted experiment from where it left off.

Use {meth}`Tuner.restore <ray.tune.Tuner.restore>` in the following cases:

- The driver script that calls {meth}`Tuner.fit() <ray.tune.Tuner.fit>` errors out, for example because the head node runs out of memory or disk.
- You manually interrupt the experiment with `Ctrl+C`.
- The entire cluster crashes, taking the experiment with it, because of an ephemeral error such as the network going down or Ray object store memory filling up.

:::{note}
{meth}`Tuner.restore <ray.tune.Tuner.restore>` isn't meant for resuming a terminated experiment with modified hyperparameter search spaces or stopping criteria. Experiment restoration is meant to resume and complete the *exact job* that you previously submitted through {meth}`Tuner.fit <ray.tune.Tuner.fit>`.

For example, consider a Tune experiment configured to run for `10` training iterations, where all trials have already completed. You can't use {meth}`Tuner.restore <ray.tune.Tuner.restore>` to restore the experiment, change the number of training iterations to `20`, and then continue training.

Instead, start a *new* experiment and initialize your model weights with a checkpoint from the previous experiment. For an example, see the {ref}`FAQ entry on iterative experimentation <tune-iterative-experimentation>`.
:::

:::{note}
Restoration can't fix bugs in your training loop. For restoration to help, the issue that crashed the experiment needs to be *ephemeral*, so that the retry after restoring can succeed.
:::

(tune-experiment-restore-example)=

### Restore a Tune experiment

Suppose you configure your initial Tune experiment as follows. The training loop is for demonstration purposes only. The important detail is that the trainable {ref}`saves and loads checkpoints <tune-trial-checkpoint>`.

```{literalinclude} /tune/doc_code/fault_tolerance.py
:language: python
:start-after: __ft_initial_run_start__
:end-before: __ft_initial_run_end__
```

Tune saves the experiment's results and checkpoints to `~/ray_results/tune_fault_tolerance_guide`, as configured by {class}`~ray.tune.RunConfig`. If the experiment stops for one of the reasons listed earlier, resume it from this path:

```{literalinclude} /tune/doc_code/fault_tolerance.py
:language: python
:start-after: __ft_restored_run_start__
:end-before: __ft_restored_run_end__
```

:::{tip}
You can also restore the experiment from a cloud bucket path:

```python
tuner = tune.Tuner.restore(
    path="s3://cloud-bucket/tune_fault_tolerance_guide", trainable=trainable
)
```

See {ref}`tune-storage-options`.
:::

### Configure which trials to resume

You can configure which trials Tune resumes, based on each trial's status when the experiment stopped:

- By default, Tune resumes unfinished trials left in the `RUNNING` state.
- You can resume `ERRORED` trials or retry them from scratch.
- You *can't* resume `TERMINATED` trials.

```{literalinclude} /tune/doc_code/fault_tolerance.py
:language: python
:start-after: __ft_restore_options_start__
:end-before: __ft_restore_options_end__
```

(tune-experiment-autoresume-example)=

### Auto-resume

In a production setting, you might want a *single script* that launches the initial training run, or restores the experiment if the initial run already happened.

To do this, use the {meth}`Tuner.can_restore <ray.tune.Tuner.can_restore>` utility:

```{literalinclude} /tune/doc_code/fault_tolerance.py
:language: python
:start-after: __ft_restore_multiplexing_start__
:end-before: __ft_restore_multiplexing_end__
```

The first time you run this script, it launches the initial training run. The second time, it attempts to resume from the outputs of the first run.

### Advanced: Restore a Tune experiment that uses Ray object references

Experiment restoration often happens in a different Ray session than the original run. In that case, Ray automatically garbage collects the original object references. If the experiment state includes object references, for example in each trial's configuration dictionary, retrieving those objects doesn't work after restoration, because the objects the references point to no longer exist.

To work around this, re-create the objects, put them in the Ray object store, and pass the new object references to Tune.

#### Example: Re-create object references to pre-trained models

Suppose you have a large pre-trained model that you want to use in your training loop. For example, it could be an image classification model that calculates an Inception Score to evaluate the quality of a generative model. You might have multiple models to tune over, where each trial samples one of the models.

```{literalinclude} /tune/doc_code/fault_tolerance.py
:language: python
:start-after: __ft_restore_objrefs_initial_start__
:end-before: __ft_restore_objrefs_initial_end__
```

To restore, re-specify the `param_space` through {meth}`Tuner.restore <ray.tune.Tuner.restore>`:

```{literalinclude} /tune/doc_code/fault_tolerance.py
:language: python
:start-after: __ft_restore_objrefs_restored_start__
:end-before: __ft_restore_objrefs_restored_end__
```

:::{note}
If you're tuning over {ref}`Ray Data <data>` datasets, you also need to re-specify them in the `param_space`. Ray Data datasets can contain object references, so the same problems apply.

The following example shows how:

```python
ds_1 = ray.data.from_items([{"x": i, "y": 2 * i} for i in range(128)])
ds_2 = ray.data.from_items([{"x": i, "y": 3 * i} for i in range(128)])

param_space = {
    "datasets": {"train": tune.grid_search([ds_1, ds_2])},
}

tuner = tune.Tuner.restore(..., param_space=param_space)
```
:::

(tune-trial-level-fault-tolerance)=

## Trial-level fault tolerance in Tune

Trial-level fault tolerance handles the failure of individual trials in the cluster. Causes of these failures include the following:

- Preemptible spot instances
- Ephemeral network connection issues
- Nodes running out of memory or out of disk space

Configure failure handling for individual trials with {class}`~ray.tune.FailureConfig`.

The following example configures {class}`~ray.tune.FailureConfig` for the `trainable` from the previous example, which implements saving and loading trial checkpoints:

```{literalinclude} /tune/doc_code/fault_tolerance.py
:language: python
:start-after: __ft_trial_failure_start__
:end-before: __ft_trial_failure_end__
```

With this configuration, when a trial encounters a runtime error, Tune reschedules that trial up to `max_failures=3` times.

Similarly, if node `X` fails, for example because it's preempted or loses its connection, Tune reschedules all trials that ran on node `X` up to three times.

## Summary

This guide covered how to enable experiment-level and trial-level fault tolerance in Ray Tune.

See the following resources for more information:

- {ref}`tune-storage-options`
- {ref}`tune-distributed-ref`
- {ref}`tune-trial-checkpoint`
