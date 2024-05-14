import random
import re
import time
from ray.air.integrations.wandb import WandbLoggerCallback
from ray.rllib.algorithms.ppo.ppo import PPOConfig
from ray.rllib.env.single_agent_env_runner import SingleAgentEnvRunner
from ray.rllib.utils.test_utils import add_rllib_example_script_args
from ray.tune.schedulers.pb2 import PB2
from ray import train, tune

# Needs the following packages to be installed on Ubuntu:
#   sudo apt-get libosmesa-dev
#   sudo apt-get install patchelf
#   python -m pip install "gymnasium[mujoco]"
# Might need to be added to bashsrc:s
#   export MUJOCO_GL=osmesa"
#   export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$HOME/.mujoco/mujoco200/bin"

parser = add_rllib_example_script_args(
    default_timesteps=5000000, default_reward=1000000.0, default_iters=100000
)

# See the following links for benchmark results of other libraries:
#   Original paper: https://arxiv.org/abs/1812.05905
#   CleanRL: https://wandb.ai/cleanrl/cleanrl.benchmark/reports/Mujoco--VmlldzoxODE0NjE
#   AgileRL: https://github.com/AgileRL/AgileRL?tab=readme-ov-file#benchmarks
benchmark_envs = {
    "HalfCheetah-v4": {
        "timesteps_total": 1000000,
    },
    "Hopper-v4": {
        "timesteps_total": 1000000,
    },
    "InvertedPendulum-v4": {
        "timesteps_total": 1000000,
    },
    "InvertedDoublePendulum-v4": {
        "timesteps_total": 1000000,
    },
    "Reacher-v4": {"timesteps_total": 1000000},
    "Swimmer-v4": {"timesteps_total": 1000000},
    "Walker2d-v4": {
        "timesteps_total": 1000000,
    },
}


if __name__ == "__main__":
    args = parser.parse_args()

    metric = "evaluation/sampler_results/episode_reward_mean"
    mode = "max"
    num_rollout_workers = 1
    pb2_scheduler = PB2(
        time_attr="timesteps_total",
        metric=metric,
        mode=mode,
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
            "mini_batch_size_per_learner": [128, 4096],
            "num_sgd_iter": [6, 32],
            "train_batch_size_per_learner": [
                128 * num_rollout_workers,
                4096 * num_rollout_workers * 8,
            ],
            "vf_share_layers": [False, True],
            "use_kl_loss": [False, True],
            "kl_coeff": [0.1, 0.4],
            "vf_clip_param": [10.0, 1e8],
            "grad_clip": [40, 200],
        },
    )

    experiment_start_time = time.time()
    # Following the paper.
    # TODO (simon): Change when running on large cluster.
    num_rollout_workers = 1
    for env, stop_criteria in benchmark_envs.items():
        hp_trial_start_time = time.time()
        config = (
            PPOConfig()
            .environment(env=env)
            # Enable new API stack and use EnvRunner.
            .experimental(_enable_new_api_stack=True)
            .rollouts(
                rollout_fragment_length=128,
                env_runner_cls=SingleAgentEnvRunner,
                num_rollout_workers=num_rollout_workers,
            )
            .resources(
                # Let's start with a small number of learner workers and
                # add later a tune grid search for these resources.
                # TODO (simon): Either add tune grid search here or make
                # an extra script to only test scalability.
                num_learner_workers=1,
                # TODO (simon): Change when running on large cluster.
                num_gpus_per_learner_worker=0.25,
            )
            .rl_module(
                model_config_dict={
                    "fcnet_hiddens": [64, 64],
                    "fcnet_activation": "tanh",
                    "vf_share_layers": True,
                },
            )
            .training(
                lr=tune.uniform(1e-5, 1e-3),
                gamma=tune.uniform(0.95, 0.99),
                lambda_=tune.uniform(0.97, 1.0),
                entropy_coeff=tune.choice([0.0, 0.01]),
                vf_loss_coeff=tune.uniform(0.01, 1.0),
                clip_param=tune.uniform(0.1, 0.3),
                kl_target=tune.uniform(0.01, 0.03),
                mini_batch_size_per_learner=tune.choice(
                    [128, 256, 512, 1024, 2048, 4096]
                ),
                num_sgd_iter=tune.sample_from(lambda spec: random.randint(6, 32)),
                vf_share_layers=tune.choice([True, False]),
                use_kl_loss=tune.choice([False, True]),
                kl_coeff=tune.uniform(0.1, 0.4),
                vf_clip_param=tune.choice([10.0, 40.0, 1e8]),
                grad_clip=tune.choice([None, 40, 100, 200]),
                train_batch_size_per_learner=tune.sample_from(
                    lambda spec: spec.config["mini_batch_size_per_learner"]
                    * num_rollout_workers
                    * random.choice([1, 2, 4, 8])
                ),
            )
            .reporting(
                metrics_num_episodes_for_smoothing=5,
                min_sample_timesteps_per_iteration=1000,
            )
            .evaluation(
                evaluation_duration="auto",
                evaluation_interval=1,
                evaluation_num_workers=1,
                evaluation_parallel_to_training=True,
                evaluation_config={
                    # PPO learns stochastic policy.
                    "explore": False,
                },
            )
        )

        # TODO (sven, simon): The WandB callback has to be fixed by the
        # tune team. The current implementation leads to an OOM.
        callbacks = None
        if hasattr(args, "wandb_key") and args.wandb_key is not None:
            project = args.wandb_project or (
                "ppo-benchmarks-mujoco-pb2"
                + "-"
                + re.sub("\\W+", "-", str(config.env).lower())
            )
            callbacks = [
                WandbLoggerCallback(
                    api_key=args.wandb_key,
                    project=project,
                    upload_checkpoints=True,
                    **({"name": args.wandb_run_name} if args.wandb_run_name else {}),
                )
            ]

        tuner = tune.Tuner(
            "PPO",
            param_space=config,
            run_config=train.RunConfig(
                stop={"timesteps_total": args.stop_timesteps},
                storage_path="~/default/ray/bm_results",
                name="benchmark_ppo_mujoco_pb2_" + env,
                callbacks=callbacks,
                checkpoint_config=train.CheckpointConfig(
                    checkpoint_frequency=args.checkpoint_freq,
                    checkpoint_at_end=args.checkpoint_at_end,
                ),
            ),
            tune_config=tune.TuneConfig(
                scheduler=pb2_scheduler,
                # TODO (simon): Change when running on large cluster.
                num_samples=args.num_samples,
            ),
        )
        result_grid = tuner.fit()
        # Get the best result for the current environment.
        best_result = result_grid.get_best_result(metric=metric, mode=mode)
        print(
            f"Finished running HP search for (env={env}) in "
            f"{time.time() - hp_trial_start_time} seconds."
        )
        print(f"Best result for {env}: {best_result}")
        print(f"Best config for {env}: {best_result.config}")

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
