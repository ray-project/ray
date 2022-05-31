.. _tune-parallelism:

A Guide To Parallelism and Resources
------------------------------------

Parallelism is determined by ``resources_per_trial`` (defaulting to 1 CPU, 0 GPU per trial)
and the resources available to Tune (``ray.cluster_resources()``).

By default, Tune automatically runs N concurrent trials, where N is the number of CPUs (cores) on your machine.

.. code-block:: python

    # If you have 4 CPUs on your machine, this will run 4 concurrent trials at a time.
    tune.run(trainable, num_samples=10)

You can override this parallelism with ``resources_per_trial``. Here you can
specify your resource requests using either a dictionary or a
:class:`PlacementGroupFactory <ray.tune.utils.placement_groups.PlacementGroupFactory>`
object. In any case, Ray Tune will try to start a placement group for each trial.

.. code-block:: python

    # If you have 4 CPUs on your machine, this will run 2 concurrent trials at a time.
    tune.run(trainable, num_samples=10, resources_per_trial={"cpu": 2})

    # If you have 4 CPUs on your machine, this will run 1 trial at a time.
    tune.run(trainable, num_samples=10, resources_per_trial={"cpu": 4})

    # Fractional values are also supported, (i.e., {"cpu": 0.5}).
    tune.run(trainable, num_samples=10, resources_per_trial={"cpu": 0.5})


Tune will allocate the specified GPU and CPU from ``resources_per_trial`` to each individual trial.
Even if the trial cannot be scheduled right now, Ray Tune will still try to start
the respective placement group. If not enough resources are available, this will trigger
:ref:`autoscaling behavior<cluster-index>` if you're using the Ray cluster launcher.

It is also possible to specify memory (``"memory"``, in bytes) and custom resource requirements.

If your trainable function starts more remote workers, you will need to pass so-called placement group
factory objects to request these resources.
See the :class:`PlacementGroupFactory documentation <ray.tune.utils.placement_groups.PlacementGroupFactory>`
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

To leverage GPUs, you must set ``gpu`` in ``tune.run(resources_per_trial)``.
This will automatically set ``CUDA_VISIBLE_DEVICES`` for each trial.

.. code-block:: python

    # If you have 8 GPUs, this will run 8 trials at once.
    tune.run(trainable, num_samples=10, resources_per_trial={"gpu": 1})

    # If you have 4 CPUs on your machine and 1 GPU, this will run 1 trial at a time.
    tune.run(trainable, num_samples=10, resources_per_trial={"cpu": 2, "gpu": 1})

You can find an example of this in the :doc:`Keras MNIST example </tune/examples/tune_mnist_keras>`.

.. warning:: If 'gpu' is not set, ``CUDA_VISIBLE_DEVICES`` environment variable will be set as empty, disallowing GPU access.

**Troubleshooting**: Occasionally, you may run into GPU memory issues when running a new trial. This may be
due to the previous trial not cleaning up its GPU state fast enough. To avoid this,
you can use ``tune.utils.wait_for_gpu`` - see :ref:`docstring <tune-util-ref>`.

How to run distributed tuning on a cluster?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

To attach to an existing Ray cluster, simply run ``ray.init`` before ``tune.run``.
See :ref:`start-ray-cli` for more information about ``ray.init``:

.. code-block:: python

    # Connect to an existing distributed Ray cluster
    ray.init(address=<ray_address>)
    tune.run(trainable, num_samples=100, resources_per_trial=tune.PlacementGroupFactory([{"CPU": 2, "GPU": 1}]))

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

In this case, ``ray.tune.suggest.ConcurrencyLimiter`` to limit the amount of concurrency:

.. code-block:: python

    algo = BayesOptSearch(utility_kwargs={
        "kind": "ucb",
        "kappa": 2.5,
        "xi": 0.0
    })
    algo = ConcurrencyLimiter(algo, max_concurrent=4)
    scheduler = AsyncHyperBandScheduler()

.. note::

    It is also possible to directly use ``tune.run(max_concurrent_trials=4, ...)``, which automatically wraps
    the underlying search algorithm in a ``ConcurrencyLimiter`` for you.

To understand concurrency limiting in depth, please see :ref:`limiter` for more details.
