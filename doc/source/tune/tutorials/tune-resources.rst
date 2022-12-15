.. _tune-parallelism:

A Guide To Parallelism and Resources
------------------------------------

Parallelism is determined by per trial resources (defaulting to 1 CPU, 0 GPU per trial)
and the resources available to Tune (``ray.cluster_resources()``).

By default, Tune automatically runs `N` concurrent trials, where `N` is the number
of CPUs (cores) on your machine.

.. code-block:: python

    # If you have 4 CPUs on your machine, this will run 4 concurrent trials at a time.
    tuner = tune.Tuner(
        trainable,
        tune_config=tune.TuneConfig(num_samples=10)
    )
    results = tuner.fit()

.. warning::

    ``tune.with_resources`` cannot be used with :ref:`Ray AIR Trainers <air-trainers>`. If you are passing a Trainer to a Tuner, specify the resource requirements in the Trainer instance using :class:`~ray.air.config.ScalingConfig`. The general principles outlined below still apply.

You can override this per trial resources with ``tune.with_resources``. Here you can
specify your resource requests using either a dictionary or a
:class:`PlacementGroupFactory <ray.tune.execution.placement_groups.PlacementGroupFactory>`
object. In any case, Ray Tune will try to start a placement group for each trial.

.. code-block:: python

    # If you have 4 CPUs on your machine, this will run 2 concurrent trials at a time.
    trainable_with_resources = tune.with_resources(trainable, {"cpu": 2})
    tuner = tune.Tuner(
        trainable_with_resources,
        tune_config=tune.TuneConfig(num_samples=10)
    )
    results = tuner.fit()

    # If you have 4 CPUs on your machine, this will run 1 trial at a time.
    trainable_with_resources = tune.with_resources(trainable, {"cpu": 4})
    tuner = tune.Tuner(
        trainable_with_resources,
        tune_config=tune.TuneConfig(num_samples=10)
    )
    results = tuner.fit()

    # Fractional values are also supported, (i.e., {"cpu": 0.5}).
    # If you have 4 CPUs on your machine, this will run 8 concurrent trials at a time.
    trainable_with_resources = tune.with_resources(trainable, {"cpu": 0.5})
    tuner = tune.Tuner(
        trainable_with_resources,
        tune_config=tune.TuneConfig(num_samples=10)
    )
    results = tuner.fit()

    # Custom resource allocation via lambda functions are also supported.
    # If you want to allocate gpu resources to trials based on a setting in your config
    trainable_with_resources = tune.with_resources(trainable,
        resources=lambda spec: {"gpu": 1} if spec.config.use_gpu else {"gpu": 0})
    tuner = tune.Tuner(
        trainable_with_resources,
        tune_config=tune.TuneConfig(num_samples=10)
    )
    results = tuner.fit()

Similarly, you can also use the ``ScalingConfig`` to specify trial resources. See :class:`~ray.air.config.ScalingConfig` for more information.

Tune will allocate the specified GPU and CPU as specified by ``tune.with_resources`` to each individual trial.
Even if the trial cannot be scheduled right now, Ray Tune will still try to start the respective placement group. If not enough resources are available, this will trigger
:ref:`autoscaling behavior <cluster-index>` if you're using the Ray cluster launcher.

It is also possible to specify memory (``"memory"``, in bytes) and custom resource requirements.

If your trainable function starts more remote workers, you will need to pass so-called placement group
factory objects to request these resources.
See the :class:`PlacementGroupFactory documentation <ray.tune.execution.placement_groups.PlacementGroupFactory>`
for further information.
This also applies if you are using other libraries making use of Ray, such as Modin.
Failure to set resources correctly may result in a deadlock, "hanging" the cluster.

.. note::
    The resources specified this way will only be allocated for scheduling Tune trials.
    These resources will not be enforced on your objective function (Tune trainable) automatically.
    You will have to make sure your trainable has enough resources to run (e.g. by setting ``n_jobs`` for a
    scikit-learn model accordingly).

How to leverage GPUs?
~~~~~~~~~~~~~~~~~~~~~

To leverage GPUs, you must set ``gpu`` in ``tune.with_resources(trainable, resources_per_trial)``.
This will automatically set ``CUDA_VISIBLE_DEVICES`` for each trial.

.. code-block:: python

    # If you have 8 GPUs, this will run 8 trials at once.
    trainable_with_gpu = tune.with_resources(trainable, {"gpu": 1})
    tuner = tune.Tuner(
        trainable_with_gpu,
        tune_config=tune.TuneConfig(num_samples=10)
    )
    results = tuner.fit()

    # If you have 4 CPUs and 1 GPU on your machine, this will run 1 trial at a time.
    trainable_with_cpu_gpu = tune.with_resources(trainable, {"cpu": 2, "gpu": 1})
    tuner = tune.Tuner(
        trainable_with_cpu_gpu,
        tune_config=tune.TuneConfig(num_samples=10)
    )
    results = tuner.fit()

You can find an example of this in the :doc:`Keras MNIST example </tune/examples/tune_mnist_keras>`.

.. warning:: If 'gpu' is not set, ``CUDA_VISIBLE_DEVICES`` environment variable will be set as empty, disallowing GPU access.

**Troubleshooting**: Occasionally, you may run into GPU memory issues when running a new trial. This may be
due to the previous trial not cleaning up its GPU state fast enough. To avoid this,
you can use ``tune.utils.wait_for_gpu`` - see :ref:`docstring <tune-util-ref>`.

How to run distributed tuning on a cluster?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

To attach to an existing Ray cluster, simply run ``ray.init`` before ``Tuner.fit()``.
See :ref:`start-ray-cli` for more information about ``ray.init``:

.. code-block:: python

    # Connect to an existing distributed Ray cluster
    ray.init(address=<ray_address>)
    # We choose to use a `PlacementGroupFactory` here to specify trial resources
    resource_group = tune.PlacementGroupFactory([{"CPU": 2, "GPU": 1}])
    trainable_with_resources = tune.with_resources(trainable, resource_group)
    tuner = tune.Tuner(
        trainable_with_resources,
        tune_config=tune.TuneConfig(num_samples=100)
    )

Read more in the Tune :ref:`distributed experiments guide <tune-distributed-ref>`.


.. _tune-dist-training:

How to run distributed training with Tune?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

To tune distributed training jobs, you should use :ref:`Ray AI Runtime (Ray AIR) <air>` to use Ray Tune and Ray Train in conjunction with
each other. Ray Tune will run multiple trials in parallel, with each trial running distributed training with Ray Train.

How to limit concurrency?
~~~~~~~~~~~~~~~~~~~~~~~~~

If using a :ref:`search algorithm <tune-search-alg>`, you may want to limit the number of trials that are being evaluated.
For example, you may want to serialize the evaluation of trials to do sequential optimization.

In this case, ``ray.tune.search.ConcurrencyLimiter`` to limit the amount of concurrency:

.. code-block:: python

    algo = BayesOptSearch(utility_kwargs={
        "kind": "ucb",
        "kappa": 2.5,
        "xi": 0.0
    })
    algo = ConcurrencyLimiter(algo, max_concurrent=4)
    scheduler = AsyncHyperBandScheduler()

.. note::

    It is also possible to directly use ``tune.TuneConfig(max_concurrent_trials=4, ...)``, which is taken in by ``Tuner``. This automatically wraps
    the underlying search algorithm in a ``ConcurrencyLimiter`` for you.

To understand concurrency limiting in depth, please see :ref:`limiter` for more details.
