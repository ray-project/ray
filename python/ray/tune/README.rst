Parallel hyperparameter search with Ray
=======================================

Using ray.tune with existing training scripts
-----------------------------------------------

With only a couple changes, you can adapt any existing script for parallel
hyperparameter search with Ray.tune.

First, you must define a ``train(config, status_reporter)`` function in your
script. This will be the entry point which Ray will call into.

.. code:: python

    def train(config, status_reporter):
        pass

Second, you should periodically report training status by passing a
``TrainingResult`` tuple to ``status_reporter.report()``.

.. code:: python
    
    from ray.tune.result import TrainingResult

    def train(config, status_reporter):
        for step in range(1000):
            ...  # do an optimization step, etc.
            status_reporter.report(TrainingResult(
                timesteps_total=step,  # required
                mean_loss=train_loss,  # optional
                mean_accuracy=train_accuracy  # optional
            ))

You can then launch a hyperparameter tuning run by running ``tune.py``.
For example:

.. code:: bash

    cd python/ray/tune
    ./tune.py -f examples/tune_mnist_ray.yaml

The YAML or JSON file passed to ``tune.py`` specifies the configuration of the
trials to launch. You can also use ray.tune programmatically, e.g. the above
example also defines a main() using tune APIs that can be run directly:

.. code:: bash

    python examples/tune_mnist_ray.py

When run, ``./tune.py`` will schedule the trials on Ray, creating a new local
Ray cluster if an existing cluster address is not specified. Incremental
status will be reported on the command line, and you can also view the reported
metrics using Tensorboard:

.. code:: text

    == Status ==
    Resources used: 4/4 CPUs, 0/0 GPUs
    Tensorboard logdir: /tmp/ray/tune_mnist
     - script_custom_0_activation=relu:	RUNNING [pid=27708], 16 s, 20 ts, 0.46 acc
     - script_custom_1_activation=elu:	RUNNING [pid=27709], 16 s, 20 ts, 0.54 acc
     - script_custom_2_activation=tanh:	RUNNING [pid=27711], 18 s, 20 ts, 0.74 acc
     - script_custom_3_activation=relu:	RUNNING [pid=27713], 12 s, 10 ts, 0.22 acc
     - script_custom_4_activation=elu:	PENDING
     - script_custom_5_activation=tanh:	PENDING
     - script_custom_6_activation=relu:	PENDING
     - script_custom_7_activation=elu:	PENDING
     - script_custom_8_activation=tanh:	PENDING
     - script_custom_9_activation=relu:	PENDING

Note that if your script requires GPUs, you should specify the number of gpus
required per trial in the ``resources`` section. Additionally, Ray should be
initialized with the ``--num-gpus`` argument (you can also pass this argument
to ``tune.py``).

Specifying search parameters
----------------------------

To specify search parameters, variables in the ``config`` section may be set to
different values for each trial. You can either specify ``grid_search: <list>``
in place of a concrete value to specify a grid search across the list of
values, or ``eval: <str>`` for values to be sampled from the given Python
expression.

.. code:: yaml

    cartpole-ppo:
        env: CartPole-v0
        alg: PPO
        repeat: 2
        stop:
            episode_reward_mean: 200
            time_total_s: 180
        resources:
            cpu: 5
            driver_cpu_limit: 1  # of the 5 CPUs, only 1 is used by the driver
        config:
            num_workers: 4
            timesteps_per_batch:
                grid_search: [4000, 40000]
            sgd_batchsize:
                grid_search: [128, 256, 512]
            num_sgd_iter:
                eval: spec.config.sgd_batchsize * 2
            lr:
                eval: random.uniform(1e-4, 1e-3)

When using the Python API, the above is equivalent to the following program:

.. code:: python

    import random
    import ray
    from ray.tune.result import TrainingResult
    from ray.tune.trial_runner import TrialRunner
    from ray.tune.variant_generator import grid_search, generate_trials

    runner = TrialRunner()

    spec = {
        "env": "CartPole-v0",
        "alg": "PPO",
        "repeat": 2,
        "stop": {
            "episode_reward_mean": 200,
            "time_total_s": 180,
        },
        "resources": {
            "cpu": 4,
        },
        "config": {
            "num_workers": 4,
            "timesteps_per_batch": grid_search([4000, 40000]),
            "sgd_batchsize": grid_search([128, 256, 512]),
            "num_sgd_iter": lambda spec: spec.config.sgd_batchsize * 2,
            "lr": lambda spec: random.uniform(1e-4, 1e-3),
        },
    }

    for trial in generate_trials(spec):
        runner.add_trial(trial)

    ray.init()

    while not runner.is_finished():
        runner.step()
        print(runner.debug_string())

Note that conditional dependencies between variables can be expressed by
variable references, e.g. ``spec.config.sgd_batchsize`` in the above example.
It is also possible to combine grid search and lambda functions by having
a lambda function return a grid search object or vice versa.

Using ray.tune as a library
---------------------------

Ray.tune's Python API allows for finer-grained control over trial setup and
scheduling. Some more examples of calling ray.tune programmatically include:

- ``python/ray/tune/examples/tune_mnist_ray.py`` (see the main function)
- ``python/ray/rllib/train.py``
- ``python/ray/rllib/tune.py``

Using ray.tune with Ray RLlib
-----------------------------

Another way to use ray.tune is through RLlib's ``python/ray/rllib/train.py``
script. This script allows you to select between different RL algorithms with
the ``--alg`` option. For example, to train pong with the A3C algorithm, run:

- ``./train.py --env=PongDeterministic-v4 --alg=A3C --stop '{"time_total_s": 3200}' --resources '{"cpu": 8}' --config '{"num_workers": 8}'``

or

- ``./train.py -f tuned_examples/pong-a3c.yaml``

You can find more RLlib examples in ``python/ray/rllib/tuned_examples``.
