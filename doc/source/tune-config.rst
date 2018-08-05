Experiment Configuration
========================


Experiment Setup
----------------

There are two ways to setup an experiment - one via Python and one via JSON.

The first is to create an Experiment object. You can then pass in either
a single experiment or a list of experiments to `run_experiments`, as follows:

.. code-block:: python

    # Single experiment
    run_experiments(Experiment(...))

    # Multiple experiments
    run_experiments([Experiment(...), Experiment(...), ...])

.. autoclass:: ray.tune.Experiment

An example of this can be found in `hyperband_example.py <https://github.com/ray-project/ray/blob/master/python/ray/tune/examples/hyperband_example.py>`__.

Alternatively, you can pass in a Python dict. This uses the same fields as
the `ray.tune.Experiment`, except the experiment name is the key of the top level
dictionary.

.. code-block:: python

    run_experiments({
        "my_experiment_name": {
            "run": "my_func",
            "trial_resources": { "cpu": 1, "gpu": 0 },
            "stop": { "mean_accuracy": 100 },
            "config": {
                "alpha": grid_search([0.2, 0.4, 0.6]),
                "beta": grid_search([1, 2]),
            },
            "upload_dir": "s3://your_bucket/path",
            "local_dir": "~/ray_results",
            "max_failures": 2
        }
    })

An example of this can be found in `async_hyperband_example.py <https://github.com/ray-project/ray/blob/master/python/ray/tune/examples/async_hyperband_example.py>`__.


Trial Variant Generation
------------------------

In the above example, we specified a grid search over two parameters using the ``grid_search`` helper function. Ray Tune also supports sampling parameters from user-specified lambda functions, which can be used in combination with grid search.

The following shows grid search over two nested parameters combined with random sampling from two lambda functions. Note that the value of ``beta`` depends on the value of ``alpha``, which is represented by referencing ``spec.config.alpha`` in the lambda function. This lets you specify conditional parameter distributions.

.. code-block:: python

    "config": {
        "alpha": lambda spec: np.random.uniform(100),
        "beta": lambda spec: spec.config.alpha * np.random.normal(),
        "nn_layers": [
            grid_search([16, 64, 256]),
            grid_search([16, 64, 256]),
        ],
    },
    "repeat": 10,

By default, each random variable and grid search point is sampled once. To take multiple random samples or repeat grid search runs, add ``repeat: N`` to the experiment config. E.g. in the above, ``"repeat": 10`` repeats the 3x3 grid search 10 times, for a total of 90 trials, each with randomly sampled values of ``alpha`` and ``beta``.

For more information on variant generation, see `basic_variant.py <https://github.com/ray-project/ray/blob/master/python/ray/tune/suggest/basic_variant.py>`__.

Resource Allocation
-------------------

Ray Tune runs each trial as a Ray actor, allocating the specified GPU and CPU ``trial_resources`` to each actor (defaulting to 1 CPU per trial). A trial will not be scheduled unless at least that amount of resources is available in the cluster, preventing the cluster from being overloaded.

If GPU resources are not requested, the ``CUDA_VISIBLE_DEVICES`` environment variable will be set as empty, disallowing GPU access.
Otherwise, it will be set to the GPUs in the list (this is managed by Ray).

If your trainable function / class creates further Ray actors or tasks that also consume CPU / GPU resources, you will also want to set ``extra_cpu`` or ``extra_gpu`` to reserve extra resource slots for the actors you will create. For example, if a trainable class requires 1 GPU itself, but will launch 4 actors each using another GPU, then it should set ``"gpu": 1, "extra_gpu": 4``.
