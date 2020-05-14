import argparse

import ray
from ray import tune
from ray.rllib.utils import try_import_tf
#from ray.rllib.models.catalog import ModelCatalog
from ray.rllib.models.tf.attention_net import GTrXLNet
from ray.rllib.examples.env.look_and_push import LookAndPush, OneHot
from ray.rllib.examples.env.repeat_after_me_env import RepeatAfterMeEnv
from ray.rllib.examples.env.repeat_initial_obs_env import RepeatInitialObsEnv
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
parser.add_argument("--stop-timesteps", type=int, default=100000)
parser.add_argument("--stop-reward", type=float, default=200)


if __name__ == "__main__":
    args = parser.parse_args()

    ray.init(num_cpus=args.num_cpus or None, local_mode=True)

    #ModelCatalog.register_custom_model(
    #    "trxl", TorchGTrXLNet if args.torch else GTrXLNet)
    registry.register_env("RepeatAfterMeEnv", lambda c: RepeatAfterMeEnv(c))
    registry.register_env(
        "RepeatInitialObsEnv", lambda _: RepeatInitialObsEnv())
    registry.register_env("LookAndPush", lambda _: OneHot(LookAndPush()))

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
            "custom_model": TorchGTrXLNet if args.torch else GTrXLNet,
            "max_seq_len": 10,
            "custom_options": {
                "num_transformer_units": 1,
                "attn_dim": 11,
                "num_heads": 1,
                "memory_tau": 3,
                "head_dim": 10,
                "ff_hidden_dim": 20,
            },
        },
        "use_pytorch": args.torch,
    }

    stop = {
        "training_iteration": args.stop_iters,
        "timesteps_total": args.stop_timesteps,
        "episode_reward_mean": args.stop_reward,
    }

    results = tune.run(args.run, config=config, stop=stop)

    if args.as_test:
        check_learning_achieved(results, args.stop_reward)
    ray.shutdown()
