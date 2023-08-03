.. _tune-parallelism:

A Guide To Parallelism and Resources for Ray Tune
-------------------------------------------------

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

You can override this per trial resources with :func:`tune.with_resources <ray.tune.with_resources>`. Here you can
specify your resource requests using either a dictionary, a :class:`~ray.air.config.ScalingConfig`, or a
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


Tune will allocate the specified GPU and CPU as specified by ``tune.with_resources`` to each individual trial.
Even if the trial cannot be scheduled right now, Ray Tune will still try to start the respective placement group. If not enough resources are available, this will trigger
:ref:`autoscaling behavior <cluster-index>` if you're using the Ray cluster launcher.

.. warning::
    ``tune.with_resources`` cannot be used with :ref:`Ray Train Trainers <train-docs>`. If you are passing a Trainer to a Tuner, specify the resource requirements in the Trainer instance using :class:`~ray.air.config.ScalingConfig`. The general principles outlined below still apply.

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

How to leverage GPUs in Tune?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

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

To tune distributed training jobs, you can use Ray Tune with Ray Train. Ray Tune will run multiple trials in parallel, with each trial running distributed training with Ray Train.

How to limit concurrency in Tune?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

To specifies the max number of trials to run concurrently, set `max_concurrent_trials` in :class:`TuneConfig <ray.tune.tune_config.TuneConfig>`

Note that actual parallelism can be less than `max_concurrent_trials` and will be determined by how many trials
can fit in the cluster at once (i.e., if you have a trial that requires 16 GPUs, your cluster has 32 GPUs,
and `max_concurrent_trials=10`, the `Tuner` can only run 2 trials concurrently).

.. code-block:: python 

    from ray.tune import TuneConfig

    config = TuneConfig(
        # ...
        num_samples=100,
        max_concurrent_trials=10,
    )