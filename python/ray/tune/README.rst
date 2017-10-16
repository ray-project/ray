Distributed hyperparameter evaluation with Ray
==============================================

Using ray.tune for deep neural network training
-----------------------------------------------

TODO

.. code:: yaml

    tune_mnist:
        env: mnist
        alg: script
        num_trials: 9
        resources:
            cpu: 1
        stop:
            mean_accuracy: 0.99
            time_total_s: 600
        config:
            script_file_path: examples/tune_mnist_ray.py
            script_entrypoint: main
            script_min_iter_time_s: 1
            activation:
                grid_search: ['relu', 'elu', 'tanh']

When run, ``./tune.py`` will schedule the trials on Ray, creating a new local
Ray cluster if an existing cluster address is not specified. Incremental
status will be reported on the command line in the following form, and you can
also view the results using Tensorboard:

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

Using ray.tune as a library
---------------------------

TODO

Using ray.tune with RLlib
-------------------------

Another way to use ray.tune is through RLlib's `train.py` script. This script
supports two modes. For example, to tune Pong:

- Inline args: ``./train.py --env=Pong-v0 --alg=PPO --num_trials=8 --stop '{"time_total_s": 3200}' --resources '{"cpu": 8, "gpu": 2}' --config '{"num_workers": 8, "sgd_num_iter": 10}'``

- File-based: ``./train.py -f tune-pong.yaml``

Both delegate scheduling of trials to the ray.tune TrialRunner class.
Additionally, the file-based mode supports hyper-parameter tuning
(currently just grid and random search).

See ray/rllib/tuned_examples for some examples of RLlib configurations.

Specifying search parameters
----------------------------

To specify search parameters, variables in the `config` section may be set to
different values for each trial. You can either specify `grid_search: <list>`
in place of a concrete value to specify a grid search across the list of
values, or `eval: <str>` for values to be sampled from the given Python
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
