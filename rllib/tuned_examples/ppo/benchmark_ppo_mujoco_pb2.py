import time
from ray.rllib.algorithms.ppo.ppo import PPOConfig
from ray.rllib.utils.metrics import NUM_ENV_STEPS_SAMPLED_LIFETIME
from ray.tune.schedulers.pb2 import PB2
from ray import train, tune

# Needs the following packages to be installed on Ubuntu:
#   sudo apt-get libosmesa-dev
#   sudo apt-get install patchelf
#   python -m pip install "gymnasium[mujoco]"
# Might need to be added to bashsrc:
#   export MUJOCO_GL=osmesa"
#   export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$HOME/.mujoco/mujoco200/bin"

# See the following links for becnhmark results of other libraries:
#   Original paper: https://arxiv.org/abs/1812.05905
#   CleanRL: https://wandb.ai/cleanrl/cleanrl.benchmark/reports/Mujoco--VmlldzoxODE0NjE
#   AgileRL: https://github.com/AgileRL/AgileRL?tab=readme-ov-file#benchmarks
benchmark_envs = {
    "HalfCheetah-v4": {
        f"{NUM_ENV_STEPS_SAMPLED_LIFETIME}": 1000000,
    },
    "Hopper-v4": {
        f"{NUM_ENV_STEPS_SAMPLED_LIFETIME}": 1000000,
    },
    "InvertedPendulum-v4": {
        f"{NUM_ENV_STEPS_SAMPLED_LIFETIME}": 1000000,
    },
    "InvertedDoublePendulum-v4": {
        f"{NUM_ENV_STEPS_SAMPLED_LIFETIME}": 1000000,
    },
    "Reacher-v4": {f"{NUM_ENV_STEPS_SAMPLED_LIFETIME}": 1000000},
    "Swimmer-v4": {f"{NUM_ENV_STEPS_SAMPLED_LIFETIME}": 1000000},
    "Walker2d-v4": {
        f"{NUM_ENV_STEPS_SAMPLED_LIFETIME}": 1000000,
    },
}

pb2_scheduler = PB2(
    time_attr=f"{NUM_ENV_STEPS_SAMPLED_LIFETIME}",
    metric="env_runners/episode_return_mean",
    mode="max",
    perturbation_interval=50000,
    # Copy bottom % with top % weights.
    quantile_fraction=0.25,
    hyperparam_bounds={
        "lr": [1e-5, 1e-3],
        "gamma": [0.95, 0.99],
        "lambda": [0.97, 1.0],
        "entropy_coeff": [0.0, 0.01],
        "vf_loss_coeff": [0.01, 1.0],
        "clip_param": [0.1, 0.3],
        "kl_target": [0.01, 0.03],
        "minibatch_size": [512, 4096],
        "num_epochs": [6, 32],
        "vf_share_layers": [False, True],
        "use_kl_loss": [False, True],
        "kl_coeff": [0.1, 0.4],
        "vf_clip_param": [10.0, float("inf")],
        "grad_clip": [40, 200],
    },
)

experiment_start_time = time.time()
# Following the paper.
num_rollout_workers = 32
for env, stop_criteria in benchmark_envs.items():
    hp_trial_start_time = time.time()
    config = (
        PPOConfig()
        .environment(env=env)
        # Enable new API stack and use EnvRunner.
        .api_stack(
            enable_rl_module_and_learner=True,
            enable_env_runner_and_connector_v2=True,
        )
        .env_runners(
            rollout_fragment_length=1,
            num_env_runners=num_rollout_workers,
            # TODO (sven, simon): Add resources.
        )
        .learners(
            # Let's start with a small number of learner workers and
            # add later a tune grid search for these resources.
            # TODO (simon): Either add tune grid search here or make
            # an extra script to only test scalability.
            num_learners=1,
            num_gpus_per_learner=1,
        )
        # TODO (simon): Adjust to new model_config_dict.
        .training(
            lr=tune.uniform(1e-5, 1e-3),
            gamma=tune.uniform(0.95, 0.99),
            lambda_=tune.uniform(0.97, 1.0),
            entropy_coeff=tune.choice([0.0, 0.01]),
            vf_loss_coeff=tune.uniform(0.01, 1.0),
            clip_param=tune.uniform(0.1, 0.3),
            kl_target=tune.uniform(0.01, 0.03),
            minibatch_size=tune.choice([512, 1024, 2048, 4096]),
            num_epochs=tune.randint(6, 32),
            vf_share_layers=tune.choice([True, False]),
            use_kl_loss=tune.choice([True, False]),
            kl_coeff=tune.uniform(0.1, 0.4),
            vf_clip_param=tune.choice([10.0, 40.0, float("inf")]),
            grad_clip=tune.choice([None, 40, 100, 200]),
            train_batch_size=tune.sample_from(
                lambda spec: spec.config["minibatch_size"] * num_rollout_workers
            ),
            model={
                "fcnet_hiddens": [64, 64],
                "fcnet_activation": "tanh",
                "vf_share_layers": True,
            },
        )
        .reporting(
            metrics_num_episodes_for_smoothing=5,
            min_sample_timesteps_per_iteration=1000,
        )
        .evaluation(
            evaluation_duration="auto",
            evaluation_interval=1,
            evaluation_num_env_runners=1,
            evaluation_parallel_to_training=True,
            evaluation_config={
                # PPO learns stochastic policy.
                "explore": False,
            },
        )
    )

    tuner = tune.Tuner(
        "PPO",
        param_space=config,
        run_config=train.RunConfig(
            stop=stop_criteria,
            name="benchmark_ppo_mujoco_pb2_" + env,
        ),
        tune_config=tune.TuneConfig(
            scheduler=pb2_scheduler,
            num_samples=8,
        ),
    )
    result_grid = tuner.fit()
    best_result = result_grid.get_best_result()
    print(
        f"Finished running HP search for (env={env}) in "
        f"{time.time() - hp_trial_start_time} seconds."
    )
    print(f"Best result for {env}: {best_result}")
    print(f"Best config for {env}: {best_result['config']}")

    # Run again with the best config.
    best_trial_start_time = time.time()
    tuner = tune.Tuner(
        "PPO",
        param_space=best_result.config,
        run_config=train.RunConfig(
            stop=stop_criteria,
            name="benchmark_ppo_mujoco_pb2_" + env + "_best",
        ),
    )
    print(f"Running best config for (env={env})...")
    tuner.fit()
    print(
        f"Finished running best config for (env={env}) "
        f"in {time.time() - best_trial_start_time} seconds."
    )

print(
    f"Finished running HP search on all MuJoCo benchmarks in "
    f"{time.time() - experiment_start_time} seconds."
)
print(
    "Results from running the best configs can be found in the "
    "`benchmark_ppo_mujoco_pb2_<ENV-NAME>_best` directories."
)
