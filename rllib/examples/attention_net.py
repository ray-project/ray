import argparse

import ray
from ray import tune
from ray.rllib.utils import try_import_tf
from ray.rllib.models.tf.attention_net import GTrXLNet
from ray.rllib.examples.env.look_and_push import LookAndPush, OneHot
from ray.rllib.examples.env.repeat_after_me_env import RepeatAfterMeEnv
from ray.rllib.examples.env.repeat_initial_obs_env import RepeatInitialObsEnv
from ray.rllib.examples.env.stateless_cartpole import StatelessCartPole
from ray.rllib.utils.test_utils import check_learning_achieved
from ray.tune import registry

tf = try_import_tf()

parser = argparse.ArgumentParser()
parser.add_argument("--run", type=str, default="PPO")
parser.add_argument("--env", type=str, default="RepeatAfterMeEnv")
parser.add_argument("--num-cpus", type=int, default=0)
parser.add_argument("--torch", action="store_true")
parser.add_argument("--as-test", action="store_true")
parser.add_argument("--stop-iters", type=int, default=200)
parser.add_argument("--stop-timesteps", type=int, default=500000)
parser.add_argument("--stop-reward", type=float, default=80)

if __name__ == "__main__":
    args = parser.parse_args()

    assert not args.torch, "PyTorch not supported for AttentionNets yet!"

    ray.init(num_cpus=args.num_cpus or None)

    registry.register_env("RepeatAfterMeEnv", lambda c: RepeatAfterMeEnv(c))
    registry.register_env("RepeatInitialObsEnv",
                          lambda _: RepeatInitialObsEnv())
    registry.register_env("LookAndPush", lambda _: OneHot(LookAndPush()))
    registry.register_env("StatelessCartPole", lambda _: StatelessCartPole())

    config = {
        "env": args.env,
        "env_config": {
            "repeat_delay": 2,
        },
        "gamma": 0.99,
        "num_workers": 0,
        "num_envs_per_worker": 20,
        "entropy_coeff": 0.001,
        "num_sgd_iter": 5,
        "vf_loss_coeff": 1e-5,
        "model": {
            "custom_model": GTrXLNet,
            "max_seq_len": 50,
            "custom_model_config": {
                "num_transformer_units": 1,
                "attn_dim": 64,
                "num_heads": 2,
                "memory_tau": 50,
                "head_dim": 32,
                "ff_hidden_dim": 32,
            },
        },
        "framework": "torch" if args.torch else "tf",
    }

    stop = {
        "training_iteration": args.stop_iters,
        "timesteps_total": args.stop_timesteps,
        "episode_reward_mean": args.stop_reward,
    }

    results = tune.run(args.run, config=config, stop=stop, verbose=1)

    if args.as_test:
        check_learning_achieved(results, args.stop_reward)
    ray.shutdown()
