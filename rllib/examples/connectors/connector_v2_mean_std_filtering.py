import argparse

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
    "--stop-iters", type=int, default=500, help="Number of iterations to train."
)
parser.add_argument(
    "--stop-timesteps", type=int, default=500000, help="Number of timesteps to train."
)
parser.add_argument(
    "--stop-reward", type=float, default=-300.0, help="Reward at which we stop training."
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
            num_rollout_workers=0,
            num_envs_per_worker=20,
            # Define our custom connector pipeline.
            # Alternatively, return a list of n ConnectorV2 pieces (which will then be
            # included in an automatically generated EnvToModulePipeline or return a
            # EnvToModulePipeline directly.
            env_to_module_connector=lambda env: (
                MeanStdFilter(
                    input_observation_space=env.single_observation_space,
                    input_action_space=env.single_action_space,
                )
            ),
        )
        .resources(
            num_learner_workers=args.num_gpus,
            num_gpus_per_learner_worker=1 if args.num_gpus else 0,
            num_cpus_for_local_worker=1,
        )
        .training(
            train_batch_size_per_learner=512,
            mini_batch_size_per_learner=64,
            gamma=0.95,
            # Linearly adjust learning rate based on number of GPUs.
            lr=0.0003 * (args.num_gpus or 1),
            lambda_=0.1,
            vf_clip_param=10.0,
            #vf_loss_coeff=0.01,
            model={
                "fcnet_activation": "relu",
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
        run_config=air.RunConfig(stop=stop, verbose=2),
        tune_config=tune.TuneConfig(num_samples=1),
    )
    results = tuner.fit()

    if args.as_test:
        check_learning_achieved(results, args.stop_reward)

    ray.shutdown()
