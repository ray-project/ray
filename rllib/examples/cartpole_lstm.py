import argparse
import os

from ray.rllib.examples.env.stateless_cartpole import StatelessCartPole
from ray.rllib.utils.test_utils import check_learning_achieved
from ray.tune.registry import get_trainable_cls

parser = argparse.ArgumentParser()
parser.add_argument(
    "--run", type=str, default="PPO", help="The RLlib-registered algorithm to use."
)
parser.add_argument("--num-cpus", type=int, default=0)
parser.add_argument(
    "--framework",
    choices=["tf", "tf2", "torch"],
    default="torch",
    help="The DL framework specifier.",
)
parser.add_argument("--use-prev-action", action="store_true")
parser.add_argument("--use-prev-reward", action="store_true")
parser.add_argument(
    "--as-test",
    action="store_true",
    help="Whether this script should be run as a test: --stop-reward must "
    "be achieved within --stop-timesteps AND --stop-iters.",
)
parser.add_argument(
    "--stop-iters", type=int, default=200, help="Number of iterations to train."
)
parser.add_argument(
    "--stop-timesteps", type=int, default=100000, help="Number of timesteps to train."
)
parser.add_argument(
    "--stop-reward", type=float, default=150.0, help="Reward at which we stop training."
)

if __name__ == "__main__":
    import ray
    from ray import air, tune

    args = parser.parse_args()

    ray.init()

    algo_cls = get_trainable_cls(args.run)
    config = algo_cls.get_default_config()

    config.environment(env=StatelessCartPole).resources(
        num_gpus=int(os.environ.get("RLLIB_NUM_GPUS", "0"))
    ).framework(args.framework).reporting(min_time_s_per_iteration=0.1).training(
        model={
            "use_lstm": True,
            "lstm_cell_size": 32,
            "lstm_use_prev_action": args.use_prev_action,
            "lstm_use_prev_reward": args.use_prev_reward,
        }
    )

    if args.run == "PPO":
        config.training(num_sgd_iter=5, vf_loss_coeff=0.0001, train_batch_size=512)
        config.model["vf_share_layers"] = True
    elif args.run == "IMPALA":
        config.rollouts(num_rollout_workers=2)
        config.resources(num_gpus=0)
        config.training(vf_loss_coeff=0.01)

    stop = {
        "training_iteration": args.stop_iters,
        "timesteps_total": args.stop_timesteps,
        "episode_reward_mean": args.stop_reward,
    }

    tuner = tune.Tuner(
        args.run,
        param_space=config.to_dict(),
        run_config=air.RunConfig(
            stop=stop,
        ),
    )
    results = tuner.fit()

    if args.as_test:
        check_learning_achieved(results, args.stop_reward)
    ray.shutdown()
