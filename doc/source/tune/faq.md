---
myst:
  html_meta:
    description: "Common Ray Tune questions: choosing search algorithms and schedulers, early stopping, and trial parallelism tradeoffs."
---

(tune-faq)=

# Ray Tune FAQ

This page answers questions that come up often about Ray Tune. If you still have questions after reading it, let the Ray team know.

```{contents}
:local:
:depth: 1
```

## What are hyperparameters?

What are *hyperparameters*, and how are they different from *model parameters*?

In supervised learning, you train a model with labeled data so the model can correctly identify new data values. A set of parameters, such as the weights in a linear regression, defines everything about the model. These are model parameters, and the model learns them during training.

```{image} /images/hyper-model-parameters.png
:alt: Diagram showing model parameters as a subset of hyperparameters. Hyperparameters, such as model type, architecture, and learning-related parameters, are set before training. Model parameters are learned during training.
```

In contrast, hyperparameters define structural details about the kind of model itself, such as whether you're using a linear regression or a classification, what architecture is best for a neural network, how many layers it has, and what kind of filters it uses. You define them before training. The model doesn't learn them.

```{image} /images/hyper-network-params.png
:alt: Diagram of a convolutional neural network's layers, noting that the number of layers, the kinds of layers, the strides and filter sizes, and the learning rate are all hyperparameters.
```

Other quantities that count as hyperparameters include learning rates and discount rates. For your training process and resulting model to work well, you first need to find the optimal or near-optimal set of hyperparameters.

How do you find the optimal hyperparameters? The most direct approach is a loop that picks a candidate set of values from a reasonably inclusive list of possible values, trains a model, compares the results with previous iterations, and keeps the set that performed best. This process is called *hyperparameter tuning* or *hyperparameter optimization* (HPO). You define a bounded search space for each hyperparameter, and a `config` dictionary collects those definitions.

<!-- TODO: We *really* need to improve this section. -->

## Which search algorithm or scheduler should I choose?
Ray Tune offers {ref}`many different search algorithms <tune-search-alg>` and {ref}`schedulers <tune-schedulers>`. Which one to use depends mostly on your problem. Ask yourself the following questions:

* Is it a small or large problem? How long does it take to train, and how costly are the resources, such as GPUs? Can you run many trials in parallel?
* How many hyperparameters do you want to tune?
* What values are valid for the hyperparameters?

**If your model returns incremental results**, such as results per epoch in deep learning or results per added tree in gradient-boosted decision trees (GBDTs), early stopping usually means you can sample more configurations, because unpromising trials stop before they run their full course. Not all search algorithms can use information from pruned trials. You can't use early stopping without incremental results. With the function API, that means you have to call `tune.report()` more than once, usually in a loop.

**If your model is small**, you can usually run many different configurations. You can use random search to generate configurations, or grid search over some values. You probably still want {ref}`ASHA for early termination of bad trials <tune-scheduler-hyperband>` if your problem supports early stopping.

**If your model is large**, you can try Bayesian optimization-based search algorithms such as {ref}`BayesOpt <bayesopt>` to get good parameter configurations after few trials. {ref}`Ax <tune-ax>` is similar but more robust to noisy data. These algorithms only work well with a small number of hyperparameters. Alternatively, you can use {ref}`Population Based Training <tune-scheduler-pbt>`, which works well with few trials, for example eight or even four. However, Population Based Training outputs a hyperparameter *schedule* rather than one fixed set of hyperparameters.

**If you have a small number of hyperparameters**, Bayesian optimization methods work well. Try {ref}`BOHB <tune-scheduler-bohb>` or {ref}`Optuna <tune-optuna>` with the {ref}`ASHA <tune-scheduler-hyperband>` scheduler to combine the benefits of Bayesian optimization with early stopping.

**If you only have continuous values for hyperparameters**, most Bayesian optimization methods work well. Discrete or categorical variables still work, but less well as the number of categories grows.

**If you have many categorical values for hyperparameters**, consider random search or a Bayesian optimization algorithm based on the Tree-structured Parzen Estimator (TPE), such as {ref}`Optuna <tune-optuna>` or {ref}`Hyperopt <tune-hyperopt>`.

**A good default** is usually random search with {ref}`ASHA for early stopping <tune-scheduler-hyperband>` for smaller problems. Use {ref}`BOHB <tune-scheduler-bohb>` for larger problems with a small number of hyperparameters, and {ref}`Population Based Training <tune-scheduler-pbt>` for larger problems with a large number of hyperparameters if a learning schedule is acceptable.

## How do I choose hyperparameter ranges?
A good start is to look at the papers that introduced the algorithms, and at what other people use.

Most algorithms also have sensible defaults for some of their parameters. For instance, [XGBoost's parameter overview](https://xgboost.readthedocs.io/en/latest/parameter.html) lists `max_depth=6` for the maximum decision tree depth. Here, anything between 2 and 10 might make sense, though that depends on your problem.

For **learning rates**, try a log-uniform distribution between 1e-5 and 1e-1, as in `tune.loguniform(1e-5, 1e-1)`.

For **batch sizes**, try powers of 2, such as 2, 4, 8, 16, 32, 64, 128, and 256. The magnitude depends on your problem. For easy problems with lots of data, use larger batch sizes. For harder problems with less data, use smaller batch sizes.

For **layer sizes**, also try powers of 2. For small problems, such as CartPole, use smaller layer sizes. For larger problems, try larger ones.

For **discount factors** in reinforcement learning, try sampling uniformly between 0.9 and 1.0. Depending on the problem, a much stricter range above 0.97 or even above 0.99 can make sense, for example for Atari.

## How can I use nested or conditional search spaces?
Sometimes you might need to define parameters whose values depend on the values of other parameters. Ray Tune offers a few ways to define them.

### Nested spaces
You can nest hyperparameter definitions in dictionaries:

```{literalinclude} doc_code/faq.py
:language: python
:start-after: __basic_config_start__
:end-before: __basic_config_end__
```

Each trial's configuration is nested exactly like the input configuration.

### Conditional spaces
For details, see {ref}`custom and conditional search spaces <tune_custom-search>`. In short, you can pass custom functions to `tune.sample_from()` that return values that depend on other values:

```{literalinclude} doc_code/faq.py
:language: python
:start-after: __conditional_spaces_start__
:end-before: __conditional_spaces_end__
```

### Conditional grid search
If you want to grid search over two parameters that depend on each other, this might not work out of the box. For instance, say that `a` should be a value between 5 and 10 and `b` should be a value between 0 and `a`. In this case, you can't use `tune.sample_from` because it doesn't support grid searching.

Instead, create a list of valid *tuples* with a helper function, as in the following example:

```{literalinclude} doc_code/faq.py
:language: python
:start-after: __iter_start__
:end-before: __iter_end__
```

Your trainable can then split the values with an assignment such as `a, b = config["ab"]` and use `a` and `b` afterward.

## How does early termination, such as Hyperband or ASHA, work?
Early termination algorithms look at intermediate reported values, for example what you report to them through `tune.report()` after each training epoch. After a certain number of steps, they remove the worst-performing trials and keep only the best-performing ones. They judge each trial by ranking the trials on the objective metric, for instance accuracy or loss.

In ASHA, you can decide how many trials to terminate early. `reduction_factor=4` means that ASHA keeps only 25% of all trials each time it reduces them. With `grace_period=n`, you can force ASHA to train each trial for at least `n` epochs.

## Why are all my trials returning "1" iteration?

:::{note}
This most likely applies to the Tune function API.
:::

Ray Tune counts iterations internally every time you call `tune.report()`. If you call `tune.report()` only once at the end of training, the counter increments only once. If you're using the class API, the counter increases after each `step()` call.

It might make sense to report metrics more often than once. For instance, if you train your algorithm for 1000 timesteps, consider reporting intermediate performance values every 100 steps. That way, schedulers such as Hyperband and ASHA can terminate poorly performing trials early.

## What are all these extra outputs?

Ray Tune reports not only hyperparameters from the `config` and metrics passed to `tune.report()`, but also some other outputs, as in the following example:

```bash
Result for easy_objective_c64c9112:
  date: 2020-10-07_13-29-18
  done: false
  experiment_id: 6edc31257b564bf8985afeec1df618ee
  experiment_tag: 7_activation=tanh,height=-53.116,steps=100,width=13.885
  hostname: ubuntu
  iterations: 0
  iterations_since_restore: 1
  mean_loss: 4.688385317424468
  neg_mean_loss: -4.688385317424468
  node_ip: 192.168.1.115
  pid: 5973
  time_since_restore: 7.605552673339844e-05
  time_this_iter_s: 7.605552673339844e-05
  time_total_s: 7.605552673339844e-05
  timestamp: 1602102558
  timesteps_since_restore: 0
  training_iteration: 1
  trial_id: c64c9112
```

See the {ref}`tune-autofilled-metrics` section for a glossary.

## How do I set resources?
To allocate specific resources to a trial, you can wrap your trainable in `tune.with_resources` together with a dictionary or a {class}`PlacementGroupFactory <ray.tune.execution.placement_groups.PlacementGroupFactory>` object:

```{literalinclude} doc_code/faq.py
:dedent:
:language: python
:start-after: __resources_start__
:end-before: __resources_end__
```

The preceding example shows three things:

1. The `cpu` and `gpu` options set how many CPUs and GPUs are available for each trial. Trials can't request more resources than these, except when requesting custom resources, as described in the third item.
1. You can request fractional GPUs. A value of 0.5 means that half of the GPU's memory is available to the trial. Make sure that your model still fits in the fractional memory.
1. You can request custom resources that you supplied to Ray when starting the cluster. A trial is scheduled only on a single node that can provide all the resources you requested.

Ray schedules each Ray worker, and therefore each Tune trial, on only one machine. For instance, if you request 2 GPUs for your trial but your cluster consists of four machines with one GPU each, the trial never gets scheduled.

Make sure that your Ray cluster has machines that can fulfill your resource requests.

Your trainable might start other remote actors, for instance if you're using distributed training through Ray Train. In that case, you can use placement groups to request additional resources:

```{literalinclude} doc_code/faq.py
:dedent:
:language: python
:start-after: __resources_pgf_start__
:end-before: __resources_pgf_end__
```

Here, you're requesting 2 additional CPUs for remote actors. These two additional actors don't have to live on the same node as your main trainable. You can control this through the `strategy` parameter. In this example, `PACK` tries to schedule the actors on the same node but can also schedule them on other nodes. See the {ref}`placement groups documentation <ray-placement-group-doc-ref>` to learn more about these placement strategies.

You can also allocate specific resources to a trial based on a custom rule with a lambda function. The following example allocates GPU resources to trials based on a setting in your parameter space:

```{literalinclude} doc_code/faq.py
:dedent:
:language: python
:start-after: __resources_lambda_start__
:end-before: __resources_lambda_end__
```

## Why is my training stuck and Ray reporting that pending actors or tasks can't be scheduled?

Usually, the trainable starts Ray actors or tasks that its resource request doesn't account for, which leads to a deadlock. Other libraries in the trainable that run on Ray, such as Modin, can also cause this less visibly. To fix the issue, request additional resources for the trial using {ref}`placement groups <ray-placement-group-doc-ref>`, as the preceding section describes.

For example, if your trainable uses Modin DataFrames, operations on them spawn Ray tasks. If you allocate an additional CPU bundle to the trial, those tasks can run without starving for resources.

```{literalinclude} doc_code/faq.py
:dedent:
:language: python
:start-after: __modin_start__
:end-before: __modin_end__
```

## How can I pass further parameter values to my trainable?

Ray Tune expects your trainable functions to accept a single `config` parameter. Sometimes, though, you want to pass constant arguments, such as the number of epochs to run or a dataset to train on. For that, Ray Tune offers a wrapper function, {func}`tune.with_parameters() <ray.tune.with_parameters>`:

```{literalinclude} doc_code/faq.py
:language: python
:start-after: __huge_data_start__
:end-before: __huge_data_end__
```

This function works similarly to `functools.partial`, but it stores the parameters directly in the Ray object store. That means you can pass even huge objects such as datasets, and Ray stores and retrieves them efficiently on your cluster machines.

`tune.with_parameters()` also works with class trainables. For more details and examples, see the {func}`tune.with_parameters() <ray.tune.with_parameters>` API reference.

## How can I reproduce experiments?
Reproducing experiments and their results means that you get exactly the same results each time you run an experiment. To achieve this, the conditions have to be exactly the same on every run. For machine learning (ML) training and tuning, this mostly concerns the random number generators that sample values at various points of the training and tuning lifecycle.

Random number generators create randomness, for instance to sample a value for a hyperparameter you defined. Computers have no true randomness. Instead, sophisticated algorithms generate numbers that *seem* random and fulfill all properties of a random distribution. You can *seed* these algorithms with an initial state, after which they always generate the same random numbers.

```{literalinclude} doc_code/faq.py
:language: python
:start-after: __seeded_1_start__
:end-before: __seeded_1_end__
```

The most commonly used random number generators from Python libraries are those in the native `random` submodule and the `numpy.random` module.

```{literalinclude} doc_code/faq.py
:language: python
:start-after: __seeded_2_start__
:end-before: __seeded_2_end__
```

Randomness occurs in several places in your tuning and training run, and you have to introduce seeds at all of them to get the same behavior:

* **Search algorithm**: You have to seed search algorithms so that they generate the same hyperparameter configurations in each run. You can explicitly instantiate some search algorithms with a random seed. Look for a `seed` parameter in the constructor. For others, try the preceding code block.
* **Schedulers**: Schedulers such as Population Based Training rely on resampling some of the parameters, which requires randomness. Use the preceding code block to set the initial seeds.
* **Training function**: Besides the initial configurations, the training functions themselves have to use seeds. This might concern data splitting, for example. Make sure to set the seed at the start of your training function.

PyTorch and TensorFlow use their own random number generators, which you also have to initialize:

```{literalinclude} doc_code/faq.py
:language: python
:start-after: __torch_tf_seeds_start__
:end-before: __torch_tf_seeds_end__
```

Seed both Ray Tune's schedulers and search algorithms, and the training code. Always seed the schedulers and search algorithms with the same seed. The same is true for the training code, but it's often beneficial for the seeds to differ *between different training runs*.

The following blueprint shows how to do all this in your training code:

```{literalinclude} doc_code/faq.py
:language: python
:start-after: __torch_seed_example_start__
:end-before: __torch_seed_example_end__
```

You can't always control every source of nondeterminism. For instance, if you use schedulers such as ASHA or Population Based Training (PBT), some trials might finish earlier than others, which affects the behavior of the schedulers. Which trials finish first can depend on the current system load, network communication, or other factors in the environment that random seeds can't control. The same is true for search algorithms such as Bayesian optimization, which take previous results into account when sampling new configurations. You can address this with the synchronous modes of PBT and Hyperband, where the schedulers wait for all trials to finish an epoch before deciding which trials to promote.

Before you rely on reproducibility for larger experiments, try it on smaller toy problems first.

(tune-bottlenecks)=

## How can I avoid bottlenecks?
Sometimes you might see a message such as the following:

```text
The `experiment_checkpoint` operation took 2.43 seconds to complete, which may be a performance bottleneck
```

Most commonly, the `experiment_checkpoint` operation throws this warning, but it might be another operation, such as `process_trial_result`.

These operations should usually take less than 500 ms to complete. When one consistently takes longer, it might indicate a problem or an inefficiency. To get rid of this message, first understand where it comes from.

The following are the main reasons this problem comes up.

### The trial configuration is large

This happens, for example, when you pass a dataset or other large object through the `config` parameter. In that case, the dataset is serialized and written to disk repeatedly during experiment checkpointing, which takes a long time.

**Solution**: Use {func}`tune.with_parameters <ray.tune.with_parameters>` to pass large objects to function trainables through the object store. For class trainables, you can do this manually with `ray.put()` and `ray.get()`. If you need to pass a class definition, consider passing an indicator, such as a string, and let the trainable select the class. Generally, your configuration dictionary should only contain primitive types, such as numbers or strings.

### The trial result is large

This happens if you return objects, data, or other large objects through the return value of `step()` in your class trainable or to `tune.report()` in your function trainable. The effect is the same as in the previous case. The results are repeatedly serialized and written to disk, which can take a long time.

**Solution**: Use checkpointing instead, by writing the data to the trainable's current working directory. There are different ways to do that, depending on whether you use the class API or the function API.

### You're training many trials on a cluster or saving large checkpoints

**Solution**: You can use {ref}`cloud checkpointing <tune-cloud-checkpointing>` to save logs and checkpoints to a specified `storage_path`. This is the preferred approach. Because all nodes can access the cloud storage, syncing happens automatically. Your results also stay safe, so you won't lose any of your data, even on preemptible instances.

### You're reporting results too often

The search algorithm, trial scheduler, and callbacks, including loggers and the trial syncer, process each result. If you're reporting a large number of results per trial, for example multiple results per second, this can take a long time.

**Solution**: Report results less often. In class trainables, `step()` could process a larger chunk of data. In function trainables, you can report only every n-th iteration of the training loop. Try to report only as many results as you need to make scheduling or searching decisions. If you need more fine-grained metrics for logging or tracking, consider a separate logging mechanism instead of Ray Tune's built-in progress logging of results.

## How can I develop and test Tune locally?

First, follow the instructions in {ref}`python-develop` to develop Tune without compiling Ray. After you set up Ray, run `pip install -r ray/python/ray/tune/requirements-dev.txt` to install all packages required for Tune development. To run all Tune tests, use the following command:

```shell
pytest ray/python/ray/tune/tests/
```

If you plan to submit a pull request, the Ray team recommends running the unit tests locally first to speed up the review process. Even though hooks run unit tests automatically for each pull request, it's usually quicker to run them on your machine first to catch any obvious mistakes.

## How can I get started contributing to Tune?

The Ray project uses GitHub to track issues, feature requests, and bugs. For a place to start, look at the issues labeled ["good first issue"](https://github.com/ray-project/ray/issues?utf8=%E2%9C%93&q=is%3Aissue+is%3Aopen+label%3A%22good-first-issue%22) and ["help wanted"](https://github.com/ray-project/ray/issues?q=is%3Aopen+is%3Aissue+label%3A%22help-wanted%22). Look for issues with "[tune]" in the title.

:::{note}
If you open a new issue or pull request related to Tune, include "[tune]" in the title and add a `tune` label.
:::

(tune-reproducible)=

## How can I make my Tune experiments reproducible?

Exact reproducibility of machine learning runs is hard to achieve, and even more so in a distributed setting, which introduces more nondeterminism. For instance, if two trials finish at the same time, the convergence of the search algorithm might depend on which trial result is processed first. This depends on the searcher. For random search, it shouldn't make a difference, but for most other searchers it does.

To achieve some amount of reproducibility, set random seeds in two places:

1. On the driver program, for example for the search algorithm. This ensures that at least the initial configurations the search algorithms suggest are the same.
1. In the trainable, if required. Neural networks are usually initialized with random numbers, and many classical ML algorithms, such as GBDTs, use randomness. Make sure to set a seed here so that the initialization is always the same.

The following example always produces the same result, except for trial runtimes:

```{literalinclude} doc_code/faq.py
:language: python
:start-after: __reproducible_start__
:end-before: __reproducible_end__
```

Some searchers use their own random states to sample new configurations. These searchers usually accept a `seed` parameter on initialization. Other searchers use NumPy's `np.random` interface, and you can set their seeds with `np.random.seed()`. Tune doesn't offer an interface to do this in the searcher classes, because setting a random seed globally could have side effects. For instance, it could influence how your dataset is split. So making these global configuration changes is up to you.

## How can I use large datasets in Tune?

You often want to compute a large object, such as training data or model weights, on the driver and use that object within each trial.

Use the `tune.with_parameters()` wrapper function to broadcast large objects to your trainable. Objects passed with this wrapper are stored in the {ref}`Ray object store <objects-in-ray>`, and are automatically fetched and passed to your trainable as a parameter.

:::{tip}
If the objects are small or already exist in the Ray object store, you don't need `tune.with_parameters()`. You can use [partials](https://docs.python.org/3/library/functools.html#functools.partial) or pass them directly to `config` instead.
:::

```{literalinclude} doc_code/faq.py
:language: python
:start-after: __large_data_start__
:end-before: __large_data_end__
```

(tune-cloud-syncing)=

## How can I upload my Tune results to cloud storage?

See {ref}`tune-cloud-checkpointing`.

Make sure that worker nodes have write access to the cloud storage. Otherwise, you get error messages such as `Error message (1): fatal error: Unable to locate credentials`. For an AWS setup, this involves adding an `IamInstanceProfile` configuration for worker nodes. For more tips, see {ref}`Accessing S3 <aws-cluster-s3>`.

(tune-kubernetes)=

## How can I use Tune with Kubernetes?

Configure shared storage. For details, see {ref}`tune-storage-options`.

(tune-docker)=

## How can I use Tune with Docker?

Configure shared storage. For details, see {ref}`tune-storage-options`.

(tune-default-search-space)=

## How do I configure search spaces?

You can specify a grid search or sampling distribution through the dictionary you pass to `Tuner(param_space=...)`.

```{literalinclude} doc_code/faq.py
:dedent:
:language: python
:start-after: __grid_search_start__
:end-before: __grid_search_end__
```

By default, Tune samples each random variable and grid search point once. To take multiple random samples, set `num_samples=N` in `tune.TuneConfig`. If you provide `grid_search` as an argument, Tune repeats the grid `num_samples` times.

```{literalinclude} doc_code/faq.py
:language: python
:start-after: __grid_search_2_start__
:end-before: __grid_search_2_end__
```

Search spaces might not be interoperable across search algorithms. For example, many search algorithms don't support a `grid_search` or `sample_from` parameter. For more information, see the {ref}`Tune search space API <tune-search-space>` page.

(tune-working-dir)=

## How do I access relative file paths in my Tune training function?

Say you launch a Tune experiment with `my_script.py` from inside `~/code`. By default, Tune changes the working directory of each worker to its corresponding trial directory, such as `~/ray_results/exp_name/trial_0000x`. This guarantees separate working directories for each worker process, which avoids conflicts when saving trial-specific outputs.

To change this behavior, you can set the `RAY_CHDIR_TO_TRIAL_DIR=0` environment variable. This tells Tune not to change the working directory to the trial directory, which gives access to paths relative to the original working directory. One caveat is that workers then share the working directory, so use the {meth}`tune.get_context().get_trial_dir() <ray.tune.TuneContext.get_trial_dir>` API to get the path for saving trial-specific outputs.

```{literalinclude} doc_code/faq.py
:dedent:
:emphasize-lines: 3, 10, 11, 12, 16
:language: python
:start-after: __no_chdir_start__
:end-before: __no_chdir_end__
```

:::{warning}
The `TUNE_ORIG_WORKING_DIR` environment variable was the original workaround for accessing paths relative to the original working directory. This environment variable is deprecated. Use the `RAY_CHDIR_TO_TRIAL_DIR` environment variable instead.
:::

(tune-multi-tenancy)=

## How can I run multiple Ray Tune jobs on the same cluster at the same time?

Ray Tune doesn't officially support multi-tenancy, which means running multiple Tune jobs on the same cluster at the same time. The Ray team doesn't test this workflow and recommends using a separate cluster for each tuning job.

There are three reasons for this:

1. When multiple Ray Tune jobs run at the same time, they compete for resources. One job could run all its trials at the same time, while the other job waits a long time for the resources to run its first trial.
1. If starting a new Ray cluster on your infrastructure is easy, there's often no cost benefit to running one large cluster instead of multiple smaller clusters. For instance, running one cluster of 32 instances costs almost the same as running four clusters with eight instances each.
1. Concurrent jobs are harder to debug. If a trial of job A fills the disk, it affects trials from job B on the same node. In practice, it's hard to reason about these conditions from the logs if something goes wrong.

Previously, some internal implementations in Ray Tune assumed that only one job runs at a time. One symptom was trials from job A using parameters specified in job B, which led to unexpected results.

See [GitHub issue #30091](https://github.com/ray-project/ray/issues/30091#issuecomment-1431676976) for more context and a workaround if you run into this problem.

(tune-iterative-experimentation)=

## How can I continue training a completed Tune experiment for longer and with new configurations?

Say you have a Tune experiment that completed with the following configurations:

```{literalinclude} /tune/doc_code/faq.py
:language: python
:start-after: __iter_experimentation_initial_start__
:end-before: __iter_experimentation_initial_end__
```

Next, you want to continue training from a checkpoint that the previous experiment generated, such as the best one, and search over a new hyperparameter search space for another `10` epochs.

{ref}`tune-fault-tolerance-ref` explains that {meth}`Tuner.restore <ray.tune.Tuner.restore>` is meant for resuming an *unfinished* experiment that was interrupted partway through, with the *exact configuration* supplied in the initial training run.

Therefore, `Tuner.restore` isn't suitable for this use case. Do this style of "iterative experimentation" with *new* Tune experiments rather than restoring a single experiment over and over and modifying the experiment spec.

The following example shows how to create a new experiment that builds off of the old one:

```{literalinclude} /tune/doc_code/faq.py
:language: python
:start-after: __iter_experimentation_resume_start__
:end-before: __iter_experimentation_resume_end__
```
