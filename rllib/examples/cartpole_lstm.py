import argparse
import os

from ray.rllib.examples.env.stateless_cartpole import StatelessCartPole
from ray.rllib.utils.test_utils import check_learning_achieved

parser = argparse.ArgumentParser()
parser.add_argument("--run", type=str, default="PPO")
parser.add_argument("--num-cpus", type=int, default=0)
parser.add_argument(
    "--framework", choices=["tf2", "tf", "tfe", "torch"], default="tf")
parser.add_argument("--as-test", action="store_true")
parser.add_argument("--use-prev-action", action="store_true")
parser.add_argument("--use-prev-reward", action="store_true")
parser.add_argument("--stop-iters", type=int, default=200)
parser.add_argument("--stop-timesteps", type=int, default=100000)
parser.add_argument("--stop-reward", type=float, default=150.0)

if __name__ == "__main__":
    import ray
    from ray import tune

    args = parser.parse_args()

    ray.init(num_cpus=args.num_cpus or None)

    configs = {
        "PPO": {
            "num_sgd_iter": 5,
            "model": {
                "vf_share_layers": True,
            },
            "vf_loss_coeff": 0.0001,
        },
        "IMPALA": {
            "num_workers": 2,
            "num_gpus": 0,
            "vf_loss_coeff": 0.01,
        },
    }

    config = dict(
        configs[args.run],
        **{
            "env": StatelessCartPole,
            # Use GPUs iff `RLLIB_NUM_GPUS` env var set to > 0.
            "num_gpus": int(os.environ.get("RLLIB_NUM_GPUS", "0")),
            "model": {
                "use_lstm": True,
                "lstm_use_prev_action": args.use_prev_action,
                "lstm_use_prev_reward": args.use_prev_reward,
            },
            "framework": args.framework,
            # Run with tracing enabled for tfe/tf2.
            "eager_tracing": args.framework in ["tfe", "tf2"],
        })

    stop = {
        "training_iteration": args.stop_iters,
        "timesteps_total": args.stop_timesteps,
        "episode_reward_mean": args.stop_reward,
    }

    results = tune.run(args.run, config=config, stop=stop, verbose=2)

    if args.as_test:
        check_learning_achieved(results, args.stop_reward)
    ray.shutdown()
