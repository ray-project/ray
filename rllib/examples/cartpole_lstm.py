import argparse

from ray.rllib.examples.env.cartpole_stateless_env import CartPoleStatelessEnv

parser = argparse.ArgumentParser()
parser.add_argument("--stop", type=int, default=200)
parser.add_argument("--torch", action="store_true")
parser.add_argument("--use-prev-action-reward", action="store_true")
parser.add_argument("--run", type=str, default="PPO")
parser.add_argument("--num-cpus", type=int, default=0)

if __name__ == "__main__":
    import ray
    from ray import tune

    args = parser.parse_args()

    tune.register_env("cartpole_stateless", lambda _: CartPoleStatelessEnv())

    ray.init(num_cpus=args.num_cpus or None)

    configs = {
        "PPO": {
            "num_sgd_iter": 5,
            "vf_share_layers": True,
            "vf_loss_coeff": 0.0001,
        },
        "IMPALA": {
            "num_workers": 2,
            "num_gpus": 0,
            "vf_loss_coeff": 0.01,
        },
    }

    tune.run(
        args.run,
        stop={"episode_reward_mean": args.stop},
        config=dict(
            configs[args.run], **{
                "env": "cartpole_stateless",
                "model": {
                    "use_lstm": True,
                    "lstm_use_prev_action_reward": args.use_prev_action_reward,
                },
                "use_pytorch": args.torch,
            }),
    )
