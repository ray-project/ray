import argparse
from functools import partial

import gymnasium as gym

from ray.rllib.algorithms.ppo import PPOConfig
from ray.rllib.connectors.env_to_module.mean_std_filter import MeanStdFilter
from ray.rllib.env.single_agent_env_runner import SingleAgentEnvRunner
from ray.rllib.utils.test_utils import check_learning_achieved


parser = argparse.ArgumentParser()
parser.add_argument("--num-cpus", type=int, default=0)
parser.add_argument(
    "--framework",
    choices=["tf", "tf2", "torch"],
    default="torch",
    help="The DL framework specifier.",
)
parser.add_argument(
    "--num-gpus",
    type=int,
    default=0,
    help="The number of GPUs (Learner workers) to use.",
)
parser.add_argument(
    "--as-test",
    action="store_true",
    help="Whether this script should be run as a test: --stop-reward must "
    "be achieved within --stop-timesteps AND --stop-iters.",
)
parser.add_argument(
    "--stop-iters", type=int, default=2000, help="Number of iterations to train."
)
parser.add_argument(
    "--stop-timesteps", type=int, default=2000000, help="Number of timesteps to train."
)
parser.add_argument(
    "--stop-reward", type=float, default=20.0, help="Reward at which we stop training."
)


if __name__ == "__main__":
    import ray
    from ray import air, tune

    args = parser.parse_args()

    ray.init()

    config = (
        PPOConfig()
        .framework(args.framework)
        .environment("Pendulum-v1")
        # Use new API stack ...
        .experimental(_enable_new_api_stack=True)
        .rollouts(
            # ... new EnvRunner and our frame stacking env-to-module connector.
            env_runner_cls=SingleAgentEnvRunner,
            # Define our custom connector pipeline.
            # Returning a list of an `EnvToModulePipeline` directly would also be ok.
            # e.g. `lambda: [MeanStdFilter()]`
            env_to_module_connector=lambda: MeanStdFilter(),
        )
        .resources(
            num_learner_workers=args.num_gpus,
            num_gpus_per_learner_worker=1 if args.num_gpus else 0,
            num_cpus_for_local_worker=1,
        )
        .training(
            lambda_=0.95,
            kl_coeff=0.5,
            clip_param=0.1,
            vf_clip_param=10.0,
            entropy_coeff=0.01,
            num_sgd_iter=10,
            # Linearly adjust learning rate based on number of GPUs.
            lr=0.00015 * (args.num_gpus or 1),
            grad_clip=100.0,
            grad_clip_by="global_norm",
            model={
                "vf_share_layers": True,
                "conv_filters": [[16, 4, 2], [32, 4, 2], [64, 4, 2], [128, 4, 2]],
                "conv_activation": "relu",
                "post_fcnet_hiddens": [256],
            },
        )
    )

    stop = {
        "training_iteration": args.stop_iters,
        "timesteps_total": args.stop_timesteps,
        "episode_reward_mean": args.stop_reward,
    }

    tuner = tune.Tuner(
        config.algo_class,
        param_space=config,
        run_config=air.RunConfig(stop=stop),
        tune_config=tune.TuneConfig(num_samples=1),
    )
    results = tuner.fit()

    if args.as_test:
        check_learning_achieved(results, args.stop_reward)

    ray.shutdown()
