---
myst:
  html_meta:
    description: "How Tune works internally: what Tuner.fit does, the lifecycle of a trial, resource management, and the TuneController architecture."
---

# How does Tune work?

This page describes how Ray Tune works internally. It covers what happens when you call `Tuner.fit()`, what the lifecycle of a trial looks like, and what the architectural components of Tune are.

:::{tip}
Before you continue, read {ref}`Key concepts of Ray Tune <tune-60-seconds>`.
:::

## What happens in `Tuner.fit`?

Suppose you run the following code:

```python
space = {"x": tune.uniform(0, 1)}
tuner = tune.Tuner(
    my_trainable,
    param_space=space,
    tune_config=tune.TuneConfig(num_samples=10),
)
results = tuner.fit()
```

Tune evaluates `my_trainable` multiple times in parallel, with different hyperparameters sampled from `uniform(0, 1)`.

Every Tune run consists of a driver process and many worker processes. The driver process is the Python process that calls `Tuner.fit()`, which calls `ray.init()` internally. The Tune driver process runs on the node where you run the script that calls `Tuner.fit()`. The trainable actors run on any node, either the same node or, with distributed Ray only, on worker nodes.

:::{note}
Use {ref}`Ray actors <actor-guide>` to parallelize an instance of a Python class. When you instantiate a class that's a Ray actor, Ray starts an instance of that class in a separate process, either on the same machine or, if you're running a Ray cluster, on another machine. The actor can then execute method calls asynchronously and maintain its own internal state.
:::

The driver spawns parallel worker processes, which are Ray actors. Each worker process evaluates a trial using the trial's hyperparameter configuration and the provided trainable.

While the trainable executes, the Tune driver communicates with each actor through actor methods to receive intermediate training results and to pause or stop actors. See {ref}`trainable-execution` and {ref}`trial-lifecycle`.

When the trainable terminates or is stopped, the actor terminates too.

(trainable-execution)=

### How does Tune execute a trainable?

Tune uses {ref}`Ray actors <actor-guide>` to parallelize the evaluation of multiple hyperparameter configurations. Each actor is a Python process that executes an instance of your trainable.

The definition of your trainable is {ref}`serialized with cloudpickle <serialization-guide>` and sent to each actor process. Each Ray actor starts an instance of the trainable to execute.

If the trainable is a class, it executes iteratively through calls to `train/step`. After each invocation, the driver receives a notification that a result dictionary is ready. The driver then pulls the result through `ray.get`.

If the trainable is a callable or a function, it executes in the Ray actor process on a separate execution thread. Whenever your function calls `tune.report`, the execution thread pauses and waits for the driver to pull a result. After the driver pulls the result, the actor's execution thread resumes automatically. For the implementation, see [function_trainable.py](https://github.com/ray-project/ray/blob/master/python/ray/tune/trainable/function_trainable.py).

### How does Tune manage resources?

Before it runs a trial, the Tune driver checks whether the cluster has available resources. It compares the available resources with the resources the trial requires. See {ref}`resource-requirements`.

If the cluster has space, the Tune driver starts a worker, which is a Ray actor. The actor is scheduled and executes on a node where the resources are available. For more information, see {doc}`tune-resources`.

(trial-lifecycle)=

## What's the lifecycle of a trial?

A trial's lifecycle consists of six stages:

* **Initialization**, or generation: Tune first generates the trial as a hyperparameter sample and configures its parameters according to what you provided in `Tuner`. Tune then places the trial in a queue to execute, with the status `PENDING`.

* **`PENDING`**: A pending trial is waiting to execute on the machine. Every trial has configured resource values. Whenever the trial's resource values are available, Tune runs the trial by starting a Ray actor that holds the configuration and the training function.

* **`RUNNING`**: A running trial has an assigned Ray actor. Multiple trials can run in parallel. For more details, see the {ref}`trainable execution <trainable-execution>` section.

* **`ERRORED`**: If a running trial throws an exception, Tune catches the exception and marks the trial as errored. Exceptions can propagate from an actor to the main Tune driver process. If you set `max_retries`, Tune sets the trial back to `PENDING` and later starts it from the last checkpoint.

* **`TERMINATED`**: A trial terminates if a stopper or scheduler stops it. With the function API, the trial also terminates when the function stops.

* **`PAUSED`**: A trial scheduler can pause a trial, which stops the trial's actor. A paused trial can later resume from the most recent checkpoint.

(tune-s-architecture)=

## What's Tune's architecture?

```{image} ../../images/tune-arch.png
:alt: Diagram of Tune's architecture, with internal components in blue boxes and public-facing components in green boxes.
```

In the diagram, blue boxes are internal components and green boxes are public-facing.

Tune's main components are the {class}`~ray.tune.execution.tune_controller.TuneController`, {class}`~ray.tune.experiment.trial.Trial` objects, a {class}`~ray.tune.search.search_algorithm.SearchAlgorithm`, a {class}`~ray.tune.schedulers.trial_scheduler.TrialScheduler`, and a {class}`~ray.tune.trainable.trainable.Trainable`.

(trial-runner-flow)=

The following figure shows the high-level training flow and how some of the components interact. Scroll horizontally to see the whole figure.

```{figure} ../../images/tune-trial-runner-flow-horizontal.png
:class: horizontal-scroll
:alt: Diagram of Tune's high-level training flow, showing how some of its components interact.
```

### TuneController

[[source code](https://github.com/ray-project/ray/blob/master/python/ray/tune/execution/tune_controller.py)] `TuneController` is the main driver of the training loop. It uses the `TrialScheduler` to prioritize and execute trials, queries the `SearchAlgorithm` for new configurations to evaluate, and handles the fault-tolerance logic.

For fault tolerance, the `TuneController` executes checkpointing if you set `checkpoint_freq`. If you set `max_failures`, it also restarts trials automatically when they fail. For example, suppose a node is lost while a trial's trainable is still executing on that node. If checkpointing is enabled, the trial reverts to the `PENDING` state and resumes from the last available checkpoint when it runs. The `TuneController` also checkpoints the entire experiment execution state on each loop iteration, so you can restart your experiment after a machine failure.

See the {class}`~ray.tune.execution.tune_controller.TuneController` docstring.

### Trial objects

[[source code](https://github.com/ray-project/ray/blob/master/python/ray/tune/experiment/trial.py)] A `Trial` object is an internal data structure that contains metadata about each training run. Each `Trial` object maps one-to-one to a `Trainable` object, but `Trial` objects themselves aren't distributed or remote. `Trial` objects transition among the following states: `PENDING`, `RUNNING`, `PAUSED`, `ERRORED`, and `TERMINATED`.

For the docstring, see {ref}`trial-docstring`.

### SearchAlgorithm

[[source code](https://github.com/ray-project/ray/tree/master/python/ray/tune/search)] The `SearchAlgorithm` is a user-provided object that Tune queries for new hyperparameter configurations to evaluate.

A search algorithm receives a notification every time a trial finishes executing one training step of `train()`, every time a trial errors, and every time a trial completes.

### TrialScheduler

[[source code](https://github.com/ray-project/ray/tree/master/python/ray/tune/schedulers)] A `TrialScheduler` operates over a set of possible trials to run, and prioritizes trial execution based on available cluster resources.

A trial scheduler can kill or pause trials, and can also reorder or prioritize incoming trials.

### Trainables

[[source code](https://github.com/ray-project/ray/blob/master/python/ray/tune/trainable/trainable.py)] Trainables are user-provided objects that run the training process. If you provide a class, it's expected to conform to the `Trainable` interface. If you provide a function, Tune wraps it in a `Trainable` class and executes the function itself on a separate thread.

A trainable executes one step of `train()` before notifying the TrialRunner.
