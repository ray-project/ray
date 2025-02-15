.. _hyperparameter_optimization:

Hyperparameter Optimization
===========================

.. TODO: add figure

Tutorial
--------

This tutorial demonstrates how to integrate Ray Train with Ray Tune for hyperparameter optimization.

Reporting metrics and checkpoints from Train to Tune
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

To perform hyperparameter tuning with Ray Train and Tune, use a ``Trainable`` function within Tune instead of passing a ``Trainer`` instance to the ``Tuner``. This avoids config overwrites and improves user experience.

Example:

.. code-block:: python

    import ray
    from ray import tune
    from ray.train.torch import TorchTrainer
    from ray.train import ScalingConfig, RunConfig
    from ray.tune.schedulers import ASHAScheduler
    from ray.tune.integration.torch import TuneReportCallback

    def train_fn_per_worker(worker_config):
        # training code here...

    def tune_fn(config):
        num_workers = config["num_workers"]  # job-level hyperparameters
        context = ray.tune.get_context()
        storage_path = context.get_experiment_path()
        name = context.get_trial_name()

        trainer = TorchTrainer(
            train_fn_per_worker,
            train_loop_config=config["train_loop_config"],  # training hyperparams
            scaling_config=ScalingConfig(num_workers=num_workers),
            run_config=RunConfig(
                storage_path=storage_path,
                name=name,
                callbacks=[TuneReportCallback()],
            ),
        )
        result = trainer.fit()

    # Launch with Tune
    ray.init()
    tuner = tune.Tuner(
        tune_fn,
        param_space={
            "num_workers": tune.choice([2, 4]),
            "train_loop_config": {"lr": tune.grid_search([1e-3, 3e-4])},
        },
        run_config=tune.RunConfig(),
        tune_config=tune.TuneConfig(max_concurrent_trials=2),
    )
    results = tuner.fit()

How to set Tune Trainable resources?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

One major change in the Train + Tune integration is that Ray Tune no longer manages placement groups for Train workers. Instead, Ray Train handles its own resource allocation.

- Previously, Tune reserved placement groups for the entire trial, ensuring limited parallel runs based on available resources.
- Now, Tune only tracks the Trainable resources, while each Train driver manages its own worker allocation.

This means users should set ``max_concurrent_trials`` in ``TuneConfig`` to limit the number of trials spawned simultaneously, preventing excessive resource contention.

Example scenario:

1. Cluster has 4 CPUs and 4 GPUs.
2. Each trial requests 1 CPU (Tune Trainable) and 2 GPUs (Train workers).
3. Without ``max_concurrent_trials``, Tune would launch 4 trials, but only 2 could execute due to GPU limits.
4. Setting ``max_concurrent_trials=2`` ensures only 2 trials run concurrently.

How fault tolerance works with Train and Tune?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Ray Train manages its own fault tolerance mechanisms, making recovery from failures seamless.

- Each trial's Train driver will attempt to recover its state when restarted.
- Tune does not need to track detailed resource usage since Train handles placement groups dynamically.
- In case of a failure, trials can restart without disrupting the overall Tune experiment.

Appendix
--------

Advanced: Resource Allocation
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

By decoupling Tune from Train resource management, resource allocation becomes more flexible.

- Tune acts as a lightweight launcher, responsible for generating hyperparameter configurations.
- Train dynamically allocates resources as needed.
- This separation improves scheduling for Ray Data, which does not follow fixed placement group constraints.

Key implications:

- Users must manually configure concurrency limits via ``max_concurrent_trials``.
- Ray Data workloads can execute more flexibly without Tune interference.
- Future enhancements may introduce a centralized ``ResourceCoordinator`` for improved scheduling.

This new design makes hyperparameter tuning more intuitive while ensuring better resource utilization across Train and Tune.
