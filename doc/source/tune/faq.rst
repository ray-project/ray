.. _tune-faq:

Ray Tune FAQ
------------

Here we try to answer questions that come up often.
If you still have questions after reading this FAQ, let us know!

.. contents::
    :local:
    :depth: 1


What are Hyperparameters?
~~~~~~~~~~~~~~~~~~~~~~~~~

What are *hyperparameters?* And how are they different from *model parameters*?

In supervised learning, we train a model with labeled data so the model can properly identify new data values.
Everything about the model is defined by a set of parameters, such as the weights in a linear regression. These
are *model parameters*; they are learned during training.

.. image:: /images/hyper-model-parameters.png

In contrast, the *hyperparameters* define structural details about the kind of model itself, like whether or not
we are using a linear regression or classification, what architecture is best for a neural network,
how many layers, what kind of filters, etc. They are defined before training, not learned.

.. image:: /images/hyper-network-params.png

Other quantities considered *hyperparameters* include learning rates, discount rates, etc. If we want our training
process and resulting model to work well, we first need to determine the optimal or near-optimal set of *hyperparameters*.

How do we determine the optimal *hyperparameters*? The most direct approach is to perform a loop where we pick
a candidate set of values from some reasonably inclusive list of possible values, train a model, compare the results
achieved with previous loop iterations, and pick the set that performed best. This process is called
*Hyperparameter Tuning* or *Optimization* (HPO). And *hyperparameters* are specified over a configured and confined
search space, collectively defined for each *hyperparameter* in a ``config`` dictionary.


.. TODO: We *really* need to improve this section.

Which search algorithm/scheduler should I choose?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Ray Tune offers :ref:`many different search algorithms <tune-search-alg>`
and :ref:`schedulers <tune-schedulers>`.
Deciding on which to use mostly depends on your problem:

* Is it a small or large problem (how long does it take to train? How costly
  are the resources, like GPUs)? Can you run many trials in parallel?
* How many hyperparameters would you like to tune?
* What values are valid for hyperparameters?

**If your model returns incremental results** (eg. results per epoch in deep learning,
results per each added tree in GBDTs, etc.) using early stopping usually allows for sampling
more configurations, as unpromising trials are pruned before they run their full course.
Please note that not all search algorithms can use information from pruned trials.
Early stopping cannot be used without incremental results - in case of the functional API,
that means that ``tune.report()`` has to be called more than once - usually in a loop.

**If your model is small**, you can usually try to run many different configurations.
A **random search** can be used to generate configurations. You can also grid search
over some values. You should probably still use
:ref:`ASHA for early termination of bad trials <tune-scheduler-hyperband>` (if your problem
supports early stopping).

**If your model is large**, you can try to either use
**Bayesian Optimization-based search algorithms** like :ref:`BayesOpt <bayesopt>` or
:ref:`Dragonfly <Dragonfly>` to get good parameter configurations after few
trials. :ref:`Ax <tune-ax>` is similar but more robust to noisy data.
Please note that these algorithms only work well with **a small number of hyperparameters**.
Alternatively, you can use :ref:`Population Based Training <tune-scheduler-pbt>` which
works well with few trials, e.g. 8 or even 4. However, this will output a hyperparameter *schedule* rather
than one fixed set of hyperparameters.

**If you have a small number of hyperparameters**, Bayesian Optimization methods
work well. Take a look at :ref:`BOHB <tune-scheduler-bohb>` or :ref:`Optuna <tune-optuna>`
with the :ref:`ASHA <tune-scheduler-hyperband>` scheduler to combine the
benefits of Bayesian Optimization with early stopping.

**If you only have continuous values for hyperparameters** this will work well
with most Bayesian Optimization methods. Discrete or categorical variables still
work, but less good with an increasing number of categories.

**If you have many categorical values for hyperparameters**, consider using random search,
or a TPE-based Bayesian Optimization algorithm such as :ref:`Optuna <tune-optuna>` or
:ref:`HyperOpt <tune-hyperopt>`.

**Our go-to solution** is usually to use **random search** with
:ref:`ASHA for early stopping <tune-scheduler-hyperband>` for smaller problems.
Use :ref:`BOHB <tune-scheduler-bohb>` for **larger problems** with a **small number of hyperparameters**
and :ref:`Population Based Training <tune-scheduler-pbt>` for **larger problems** with a
**large number of hyperparameters** if a learning schedule is acceptable.


How do I choose hyperparameter ranges?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
A good start is to look at the papers that introduced the algorithms, and also
to see what other people are using.

Most algorithms also have sensible defaults for some of their parameters.
For instance, `XGBoost's parameter overview <https://xgboost.readthedocs.io/en/latest/parameter.html>`_
reports to use ``max_depth=6`` for the maximum decision tree depth. Here, anything
between 2 and 10 might make sense (though that naturally depends on your problem).

For **learning rates**, we suggest using a **loguniform distribution** between
**1e-5** and **1e-1**: ``tune.loguniform(1e-5, 1e-1)``.

For **batch sizes**, we suggest trying **powers of 2**, for instance, 2, 4, 8,
16, 32, 64, 128, 256, etc. The magnitude depends on your problem. For easy
problems with lots of data, use higher batch sizes, for harder problems with
not so much data, use lower batch sizes.

For **layer sizes** we also suggest trying **powers of 2**. For small problems
(e.g. Cartpole), use smaller layer sizes. For larger problems, try larger ones.

For **discount factors** in reinforcement learning we suggest sampling uniformly
between 0.9 and 1.0. Depending on the problem, a much stricter range above 0.97
or oeven above 0.99 can make sense (e.g. for Atari).


How can I use nested/conditional search spaces?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Sometimes you might need to define parameters whose value depend on the value
of other parameters. Ray Tune offers some methods to define these.

Nested spaces
'''''''''''''
You can nest hyperparameter definition in sub dictionaries:

.. literalinclude:: doc_code/faq.py
    :language: python
    :start-after: __basic_config_start__
    :end-before: __basic_config_end__

The trial config will be nested exactly like the input config.


Conditional spaces
''''''''''''''''''
:ref:`Custom and conditional search spaces are explained in detail here <tune_custom-search>`.
In short, you can pass custom functions to ``tune.sample_from()`` that can
return values that depend on other values:

.. literalinclude:: doc_code/faq.py
    :language: python
    :start-after: __conditional_spaces_start__
    :end-before: __conditional_spaces_end__


Conditional grid search
'''''''''''''''''''''''
If you would like to grid search over two parameters that depend on each other,
this might not work out of the box. For instance say that *a* should be a value
between 5 and 10 and *b* should be a value between 0 and a. In this case, we
cannot use ``tune.sample_from`` because it doesn't support grid searching.

The solution here is to create a list of valid *tuples* with the help of a
helper function, like this:

.. literalinclude:: doc_code/faq.py
    :language: python
    :start-after: __iter_start__
    :end-before: __iter_end__


Your trainable then can do something like ``a, b = config["ab"]`` to split
the a and b variables and use them afterwards.


How does early termination (e.g. Hyperband/ASHA) work?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Early termination algorithms look at the intermediately reported values,
e.g. what is reported to them via ``tune.report()`` after each training
epoch. After a certain number of steps, they then remove the worst
performing trials and keep only the best performing trials. Goodness of a trial
is determined by ordering them by the objective metric, for instance accuracy
or loss.

In ASHA, you can decide how many trials are early terminated.
``reduction_factor=4`` means that only 25% of all trials are kept each
time they are reduced. With ``grace_period=n`` you can force ASHA to
train each trial at least for ``n`` epochs.


Why are all my trials returning "1" iteration?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

**This is most likely applicable for the Tune function API.**

Ray Tune counts iterations internally every time ``tune.report()`` is
called. If you only call ``tune.report()`` once at the end of the training,
the counter has only been incremented once. If you're using the class API,
the counter is increased after calling ``step()``.

Note that it might make sense to report metrics more often than once. For
instance, if you train your algorithm for 1000 timesteps, consider reporting
intermediate performance values every 100 steps. That way, schedulers
like Hyperband/ASHA can terminate bad performing trials early.


What are all these extra outputs?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

You'll notice that Ray Tune not only reports hyperparameters (from the
``config``) or metrics (passed to ``tune.report()``), but also some other
outputs.

.. code-block:: bash

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

See the :ref:`tune-autofilled-metrics` section for a glossary.

How do I set resources?
~~~~~~~~~~~~~~~~~~~~~~~
If you want to allocate specific resources to a trial, you can use the
``resources_per_trial`` parameter of ``tune.run()``, to which you can pass
a dict or a :class:`PlacementGroupFactory <ray.tune.utils.placement_groups.PlacementGroupFactory>` object:

.. literalinclude:: doc_code/faq.py
    :dedent:
    :language: python
    :start-after: __resources_start__
    :end-before: __resources_end__

The example above showcases three things:

1. The `cpu` and `gpu` options set how many CPUs and GPUs are available for
   each trial, respectively. **Trials cannot request more resources** than these
   (exception: see 3).
2. It is possible to request **fractional GPUs**. A value of 0.5 means that
   half of the memory of the GPU is made available to the trial. You will have
   to make sure yourself that your model still fits on the fractional memory.
3. You can request custom resources you supplied to Ray when starting the cluster.
   Trials will only be scheduled on single nodes that can provide all resources you
   requested.

One important thing to keep in mind is that each Ray worker (and thus each
Ray Tune Trial) will only be scheduled on **one machine**. That means if
you for instance request 2 GPUs for your trial, but your cluster consists
of 4 machines with 1 GPU each, the trial will never be scheduled.

In other words, you will have to make sure that your Ray cluster
has machines that can actually fulfill your resource requests.

In some cases your trainable might want to start other remote actors, for instance if you're
leveraging distributed training via Ray Train. In these cases, you can use
:ref:`placement groups <ray-placement-group-doc-ref>` to request additional resources:

.. literalinclude:: doc_code/faq.py
    :dedent:
    :language: python
    :start-after: __resources_pgf_start__
    :end-before: __resources_pgf_end__

Here, you're requesting 2 additional CPUs for remote tasks. These two additional
actors do not necessarily have to live on the same node as your main trainable.
In fact, you can control this via the ``strategy`` parameter. In this example, ``PACK``
will try to schedule the actors on the same node, but allows them to be scheduled
on other nodes as well. Please refer to the
:ref:`placement groups documentation <ray-placement-group-doc-ref>` to learn more
about these placement strategies.

Why is my training stuck and Ray reporting that pending actor or tasks cannot be scheduled?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

This is usually caused by Ray actors or tasks being started by the
trainable without the trainable resources accounting for them, leading to a deadlock.
This can also be "stealthly" caused by using other libraries in the trainable that are
based on Ray, such as Modin. In order to fix the issue, request additional resources for
the trial using :ref:`placement groups <ray-placement-group-doc-ref>`, as outlined in
the section above.

For example, if your trainable is using Modin dataframes, operations on those will spawn
Ray tasks. By allocating an additional CPU bundle to the trial, those tasks will be able
to run without being starved of resources.

.. literalinclude:: doc_code/faq.py
    :dedent:
    :language: python
    :start-after: __modin_start__
    :end-before: __modin_end__


How can I pass further parameter values to my trainable?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Ray Tune expects your trainable functions to accept only up to two parameters,
``config`` and ``checkpoint_dir``. But sometimes there are cases where
you want to pass constant arguments, like the number of epochs to run,
or a dataset to train on. Ray Tune offers a wrapper function to achieve
just that, called :func:`tune.with_parameters() <ray.tune.with_parameters>`:

.. literalinclude:: doc_code/faq.py
    :language: python
    :start-after: __huge_data_start__
    :end-before: __huge_data_end__


This function works similarly to ``functools.partial``, but it stores
the parameters directly in the Ray object store. This means that you
can pass even huge objects like datasets, and Ray makes sure that these
are efficiently stored and retrieved on your cluster machines.

:func:`tune.with_parameters() <ray.tune.with_parameters>`
also works with class trainables. Please see
:ref:`here for further details <tune-with-parameters>` and examples.


How can I reproduce experiments?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Reproducing experiments and experiment results means that you get the exact same
results when running an experiment again and again. To achieve this, the
conditions have to be exactly the same each time you run the exeriment.
In terms of ML training and tuning, this mostly concerns
the random number generators that are used for sampling in various places of the
training and tuning lifecycle.

Random number generators are used to create randomness, for instance to sample a hyperparameter
value for a parameter you defined. There is no true randomness in computing, rather
there are sophisticated algorithms that generate numbers that *seem* to be random and
fulfill all properties of a random distribution. These algorithms can be *seeded* with
an initial state, after which the generated random numbers are always the same.

.. literalinclude:: doc_code/faq.py
    :language: python
    :start-after: __seeded_1_start__
    :end-before: __seeded_1_end__

The most commonly used random number generators from Python libraries are those in the
native ``random`` submodule and the ``numpy.random`` module.

.. literalinclude:: doc_code/faq.py
    :language: python
    :start-after: __seeded_2_start__
    :end-before: __seeded_2_end__

In your tuning and training run, there are several places where randomness occurs, and
at all these places we will have to introduce seeds to make sure we get the same behavior.

* **Search algorithm**: Search algorithms have to be seeded to generate the same
  hyperparameter configurations in each run. Some search algorithms can be explicitly instantiated with a
  random seed (look for a ``seed`` parameter in the constructor). For others, try to use
  the above code block.
* **Schedulers**: Schedulers like Population Based Training rely on resampling some
  of the parameters, requiring randomness. Use the code block above to set the initial
  seeds.
* **Training function**: In addition to initializing the configurations, the training
  functions themselves have to use seeds. This could concern e.g. the data splitting.
  You should make sure to set the seed at the start of your training function.

PyTorch and TensorFlow use their own RNGs, which have to be initialized, too:

.. literalinclude:: doc_code/faq.py
    :language: python
    :start-after: __torch_tf_seeds_start__
    :end-before: __torch_tf_seeds_end__

You should thus seed both Ray Tune's schedulers and search algorithms, and the
training code. The schedulers and search algorithms should always be seeded with the
same seed. This is also true for the training code, but often it is beneficial that
the seeds differ *between different training runs*.

Here's a blueprint on how to do all this in your training code:

.. literalinclude:: doc_code/faq.py
    :language: python
    :start-after: __torch_seed_example_start__
    :end-before: __torch_seed_example_end__


**Please note** that it is not always possible to control all sources of non-determinism.
For instance, if you use schedulers like ASHA or PBT, some trials might finish earlier
than other trials, affecting the behavior of the schedulers. Which trials finish first
can however depend on the current system load, network communication, or other factors
in the envrionment that we cannot control with random seeds. This is also true for search
algorithms such as Bayesian Optimization, which take previous results into account when
sampling new configurations. This can be tackled by
using the **synchronous modes** of PBT and Hyperband, where the schedulers wait for all trials to
finish an epoch before deciding which trials to promote.

We strongly advise to try reproduction on smaller toy problems first before relying
on it for larger experiments.


.. _tune-bottlenecks:

How can I avoid bottlenecks?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Sometimes you might run into a message like this:

.. code-block::

    The `experiment_checkpoint` operation took 2.43 seconds to complete, which may be a performance bottleneck

Most commonly, the ``experiment_checkpoint`` operation is throwing this warning, but it might be something else,
like ``process_trial_result``.

These operations should usually take less than 500ms to complete. When it consistently takes longer, this might
indicate a problem or inefficiencies. To get rid of this message, it is important to understand where it comes
from.

These are the main reasons this problem comes up:

**The Trial config is very large**

This is the case if you e.g. try to pass a dataset or other large object via the ``config`` parameter.
If this is the case, the dataset is serialized and written to disk repeatedly during experiment
checkpointing, which takes a long time.

**Solution**: Use :func:`tune.with_parameters <ray.tune.with_parameters>` to pass large objects to
function trainables via the objects store. For class trainables you can do this manually via ``ray.put()``
and ``ray.get()``. If you need to pass a class definition, consider passing an
indicator (e.g. a string) instead and let the trainable select the class instead. Generally, your config
dictionary should only contain primitive types, like numbers or strings.

**The Trial result is very large**

This is the case if you return objects, data, or other large objects via the return value of ``step()`` in
your class trainable or to ``tune.report()`` in your function trainable. The effect is the same as above:
The results are repeatedly serialized and written to disk, and this can take a long time.

**Solution**: Usually you should be able to write data to the trial directory instead. You can then pass a
filename back (or assume it is a known location). The trial dir is usually the current working directory. Class
trainables have the ``Trainable.logdir`` property and function trainables the :func:`ray.tune.get_trial_dir`
function to retrieve the logdir. If you really have to, you can also ``ray.put()`` an object to the Ray
object store and retrieve it with ``ray.get()`` on the other side. Generally, your result dictionary
should only contain primitive types, like numbers or strings.

**You are training a large number of trials on a cluster, or you are saving huge checkpoints**

Checkpoints and logs are synced between nodes
- usually at least to the driver on the head node, but sometimes between worker nodes if needed (e.g. when
using :ref:`Population Based Training <tune-scheduler-pbt>`). If these checkpoints are very large (e.g. for
NLP models), or if you are training a large number of trials, this syncing can take a long time.

If nothing else is specified, syncing happens via SSH, which can lead to network overhead as connections are
not kept open by Ray Tune.

**Solution**: There are multiple solutions, depending on your needs:

1. You can disable syncing to the driver in the :class:`tune.SyncConfig <ray.tune.SyncConfig>`. In this case,
   logs and checkpoints will not be synced to the driver, so if you need to access them later, you will have to
   transfer them where you need them manually.

2. You can use :ref:`cloud checkpointing <tune-cloud-checkpointing>` to save logs and checkpoints to a specified `upload_dir`.
   This is the preferred way to deal with this. All syncing will be taken care of automatically, as all nodes
   are able to access the cloud storage. Additionally, your results will be safe, so even when you're working on
   pre-emptible instances, you won't lose any of your data.

**You are reporting results too often**

Each result is processed by the search algorithm, trial scheduler, and callbacks (including loggers and the
trial syncer). If you're reporting a large number of results per trial (e.g. multiple results per second),
this can take a long time.

**Solution**: The solution here is obvious: Just don't report results that often. In class trainables, ``step()``
should maybe process a larger chunk of data. In function trainables, you can report only every n-th iteration
of the training loop. Try to balance the number of results you really need to make scheduling or searching
decisions. If you need more fine grained metrics for logging or tracking, consider using a separate logging
mechanism for this instead of the Ray Tune-provided progress logging of results.

How can I develop and test Tune locally?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

First, follow the instructions in :ref:`python-develop` to develop Tune without compiling Ray.
After Ray is set up, run ``pip install -r ray/python/ray/tune/requirements-dev.txt`` to install all packages
required for Tune development. Now, to run all Tune tests simply run:

.. code-block:: shell

    pytest ray/python/ray/tune/tests/

If you plan to submit a pull request, we recommend you to run unit tests locally beforehand to speed up the review process.
Even though we have hooks to run unit tests automatically for each pull request, it's usually quicker to run them
on your machine first to avoid any obvious mistakes.


How can I get started contributing to Tune?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

We use Github to track issues, feature requests, and bugs. Take a look at the
ones labeled `"good first issue" <https://github.com/ray-project/ray/issues?utf8=%E2%9C%93&q=is%3Aissue+is%3Aopen+label%3A%22good+first+issue%22>`__ and `"help wanted" <https://github.com/ray-project/ray/issues?q=is%3Aopen+is%3Aissue+label%3A%22help+wanted%22>`__ for a place to start.
Look for issues with "[tune]" in the title.

.. note::

    If raising a new issue or PR related to Tune, be sure to include "[tune]" in the title and add a ``tune`` label.

For project organization, Tune maintains a relatively up-to-date organization of
issues on the `Tune Github Project Board <https://github.com/ray-project/ray/projects/4>`__.
Here, you can track and identify how issues are organized.


.. _tune-reproducible:

How can I make my Tune experiments reproducible?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Exact reproducibility of machine learning runs is hard to achieve. This
is even more true in a distributed setting, as more non-determinism is
introduced. For instance, if two trials finish at the same time, the
convergence of the search algorithm might be influenced by which trial
result is processed first. This depends on the searcher - for random search,
this shouldn't make a difference, but for most other searchers it will.

If you try to achieve some amount of reproducibility, there are two
places where you'll have to set random seeds:

1. On the driver program, e.g. for the search algorithm. This will ensure
   that at least the initial configurations suggested by the search
   algorithms are the same.

2. In the trainable (if required). Neural networks are usually initialized
   with random numbers, and many classical ML algorithms, like GBDTs, make use of
   randomness. Thus you'll want to make sure to set a seed here
   so that the initialization is always the same.

Here is an example that will always produce the same result (except for trial
runtimes).

.. literalinclude:: doc_code/faq.py
    :language: python
    :start-after: __reproducible_start__
    :end-before: __reproducible_end__


Some searchers use their own random states to sample new configurations.
These searchers usually accept a ``seed`` parameter that can be passed on
initialization. Other searchers use Numpy's ``np.random`` interface -
these seeds can be then set with ``np.random.seed()``. We don't offer an
interface to do this in the searcher classes as setting a random seed
globally could have side effects. For instance, it could influence the
way your dataset is split. Thus, we leave it up to the user to make
these global configuration changes.


How can I use large datasets in Tune?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

You often will want to compute a large object (e.g., training data, model weights) on the driver and use that
object within each trial.

Tune provides a wrapper function ``tune.with_parameters()`` that allows you to broadcast large objects to your trainable.
Objects passed with this wrapper will be stored on the :ref:`Ray object store <objects-in-ray>` and will
be automatically fetched and passed to your trainable as a parameter.


.. tip:: If the objects are small in size or already exist in the :ref:`Ray Object Store <objects-in-ray>`, there's no need to use ``tune.with_parameters()``. You can use `partials <https://docs.python.org/3/library/functools.html#functools.partial>`__ or pass in directly to ``config`` instead.

.. literalinclude:: doc_code/faq.py
    :language: python
    :start-after: __large_data_start__
    :end-before: __large_data_end__


How can I upload my Tune results to cloud storage?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

If an upload directory is provided, Tune will automatically sync results from the ``local_dir`` to the given directory,
natively supporting standard URIs for systems like S3, gsutil or HDFS.
Here is an example of uploading to S3, using a bucket called ``my-log-dir``:

.. literalinclude:: doc_code/faq.py
    :dedent:
    :language: python
    :start-after: __log_1_start__
    :end-before: __log_1_end__

You can customize this to specify arbitrary storages with the ``syncer`` argument in ``tune.SyncConfig``.
This argument supports either strings with the same replacement fields OR arbitrary functions.

.. literalinclude:: doc_code/faq.py
    :dedent:
    :language: python
    :start-after: __log_2_start__
    :end-before: __log_2_end__

If a string is provided, then it must include replacement fields ``{source}`` and ``{target}``, like
``s3 sync {source} {target}``. Alternatively, a function can be provided with the following signature:

.. literalinclude:: doc_code/faq.py
    :language: python
    :start-after: __sync_start__
    :end-before: __sync_end__

By default, syncing occurs every 300 seconds.
To change the frequency of syncing, set the ``sync_period`` attribute of the sync config to the desired syncing period.

Note that uploading only happens when global experiment state is collected, and the frequency of this is
determined by the sync period. So the true upload period is given by ``max(sync period, TUNE_GLOBAL_CHECKPOINT_S)``.

Make sure that worker nodes have the write access to the cloud storage.
Failing to do so would cause error messages like ``Error message (1): fatal error: Unable to locate credentials``.
For AWS set up, this involves adding an IamInstanceProfile configuration for worker nodes.
Please :ref:`see here for more tips <aws-cluster-s3>`.


.. _tune-docker:

How can I use Tune with Docker?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Tune automatically syncs files and checkpoints between different remote
containers as needed.

To make this work in your Docker cluster, e.g. when you are using the Ray autoscaler
with docker containers, you will need to pass a
``DockerSyncer`` to the ``syncer`` argument of ``tune.SyncConfig``.

.. literalinclude:: doc_code/faq.py
    :dedent:
    :language: python
    :start-after: __docker_start__
    :end-before: __docker_end__

.. _tune-kubernetes:

How can I use Tune with Kubernetes?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Ray Tune automatically synchronizes files and checkpoints between different remote nodes as needed.
This usually happens via SSH, but this can be a :ref:`performance bottleneck <tune-bottlenecks>`,
especially when running many trials in parallel.

Instead you should use shared storage for checkpoints so that no additional synchronization across nodes
is necessary. There are two main options.

First, you can use the :ref:`SyncConfig <tune-sync-config>` to store your
logs and checkpoints on cloud storage, such as AWS S3 or Google Cloud Storage:

.. literalinclude:: doc_code/faq.py
    :dedent:
    :language: python
    :start-after: __s3_start__
    :end-before: __s3_end__

Second, you can set up a shared file system like NFS. If you do this, disable automatic trial syncing:

.. literalinclude:: doc_code/faq.py
    :dedent:
    :language: python
    :start-after: __sync_config_start__
    :end-before: __sync_config_end__

Lastly, if you still want to use SSH for trial synchronization, but are not running
on the Ray cluster launcher, you might need to pass a
``KubernetesSyncer`` to the ``syncer`` argument of ``tune.SyncConfig``.
You have to specify your Kubernetes namespace explicitly:

.. literalinclude:: doc_code/faq.py
    :dedent:
    :language: python
    :start-after: __k8s_start__
    :end-before: __k8s_end__

Please note that we strongly encourage you to use one of the other two options instead, as they will
result in less overhead and don't require pods to SSH into each other.

.. _tune-default-search-space:

How do I configure search spaces?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

You can specify a grid search or sampling distribution via the dict passed into ``tune.run(config=...)``.

.. literalinclude:: doc_code/faq.py
    :dedent:
    :language: python
    :start-after: __grid_search_start__
    :end-before: __grid_search_end__

By default, each random variable and grid search point is sampled once.
To take multiple random samples, add ``num_samples: N`` to the experiment config.
If `grid_search` is provided as an argument, the grid will be repeated ``num_samples`` of times.

.. literalinclude:: doc_code/faq.py
    :emphasize-lines: 13
    :language: python
    :start-after: __grid_search_2_start__
    :end-before: __grid_search_2_end__

Note that search spaces may not be interoperable across different search algorithms.
For example, for many search algorithms, you will not be able to use a ``grid_search`` or ``sample_from`` parameters.
Read about this in the :ref:`Search Space API <tune-search-space>` page.
