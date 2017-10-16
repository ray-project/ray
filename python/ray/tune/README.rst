Parallel hyperparameter evaluation with Ray
===========================================

Using ray.tune for deep neural network training
-----------------------------------------------

With only a couple changes, you can parallelize evaluation of any existing
Python script with Ray.tune.

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
            # do a training iteration
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
trials to launch. For example, the follow YAML describes a grid search over
activation functions.

.. code:: yaml

    tune_mnist:
        env: mnist
        alg: script
        num_trials: 10
        resources:
            cpu: 1
        stop:
            mean_accuracy: 0.99
            time_total_s: 600
        config:
            script_file_path: examples/tune_mnist_ray.py
            script_entrypoint: train
            script_min_iter_time_s: 1
            activation:
                grid_search: ['relu', 'elu', 'tanh']

When run, ``./tune.py`` will schedule the trials on Ray, creating a new local
Ray cluster if an existing cluster address is not specified. Incremental
status will be reported on the command line, and you can also view the reported
metrics using Tensorboard:

.. code:: text

    == Status ==
    Available: Resources(cpu=4, gpu=0)
    Committed: Resources(cpu=4, gpu=0)
    Tensorboard logdir: /tmp/ray/tune_mnist
     - script_mnist_0_activation=relu:	RUNNING, 24 itrs, 203 s, 240 ts, 0.92 acc
     - script_mnist_1_activation=elu:	RUNNING, 24 itrs, 205 s, 240 ts, 0.96 acc
     - script_mnist_2_activation=tanh:	RUNNING, 26 itrs, 210 s, 260 ts, 0.88 acc
     - script_mnist_3_activation=relu:	RUNNING, 25 itrs, 207 s, 250 ts, 0.84 acc
     - script_mnist_4_activation=elu:	PENDING
     - script_mnist_5_activation=tanh:	PENDING
     - script_mnist_6_activation=relu:	PENDING
     - script_mnist_7_activation=elu:	PENDING
     - script_mnist_8_activation=tanh:	PENDING

Note that if your script requires GPUs, you should specify the number of gpus
required per trial in the ``resources`` section. Additionally, Ray should be
initialized with the ``--num_gpus`` argument (you can also pass this argument
to ``tune.py``).

Using ray.tune as a library
---------------------------

Ray.tune can also be called programmatically from Python code. This allows for
finer-grained control over trial setup and scheduling. Some examples of
calling ray.tune programmatically include:

- python/ray/tune/examples/tune_mnist_ray.py
- python/ray/rllib/train.py

Using ray.tune with RLlib
-------------------------

Another way to use ray.tune is through RLlib's ``python/ray/rllib/train.py``
script. This script allows you to select between different RL algorithms with
the ``--alg`` option. For example, to train pong with the A3C algorithm, run:

- ``./train.py --env=PongDeterministic-v4 --alg=A3C --num_trials=8 --stop '{"time_total_s": 3200}' --resources '{"cpu": 8}' --config '{"num_workers": 8}'``

or

- ``./train.py -f tuned_examples/pong-a3c.yaml``

You can find more examples of using RLlib in ``python/ray/rllib/tuned_examples``.

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
        num_trials: 6
        stop:
            episode_reward_mean: 200
            time_total_s: 180
        resources:
            cpu: 4
        config:
            num_workers: 4
            num_sgd_iter:
                grid_search: [1, 4]
            sgd_batchsize:
                grid_search: [128, 256, 512]
            lr:
                eval: random.uniform(1e-4, 1e-3)
