ray.tune: fast hyperparameter search
====================================

Using ray.tune with RLlib
-------------------------

You can use ray.tune through RLlib's train.py script. The train.py script
supports two modes:

- Inline args: ``./train.py --env=Pong-v0 --alg=PPO --num_trials=8 --stop '{"time_total_s": 3200}' --resources '{"cpu": 8, "gpu": 2}' --config '{"num_workers": 8, "sgd_num_iter": 10}'``

- File-based: ``./train.py -f tune-pong.yaml``

Both delegate scheduling of trials to the ray.tune TrialRunner class.
Additionally, the file-based mode supports hyper-parameter tuning
(currently just grid and random search).

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

See ray/rllib/tuned_examples for more examples of configs in YAML form.

Using ray.tune to run custom scripts
------------------------------------

TODO
