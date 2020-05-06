import argparse

import ray
from ray.rllib.examples.env.repeat_initial_obs_env import RepeatInitialObsEnv
from ray.rllib.examples.env.repeat_after_me_env import RepeatAfterMeEnv
from ray.rllib.examples.env.stateless_cartpole import StatelessCartPole
from ray.rllib.utils import try_import_torch
from ray.rllib.models import ModelCatalog
import ray.tune as tune

torch, nn = try_import_torch()

parser = argparse.ArgumentParser()
parser.add_argument("--run", type=str, default="PPO")
parser.add_argument("--env", type=str, default="repeat_initial")
parser.add_argument("--stop", type=int, default=90)
parser.add_argument("--num-cpus", type=int, default=0)
parser.add_argument("--fc-size", type=int, default=64)
parser.add_argument("--lstm-cell-size", type=int, default=256)


if __name__ == "__main__":
    args = parser.parse_args()

    ray.init(num_cpus=args.num_cpus or None)
    ModelCatalog.register_custom_model("rnn", RNNModel)
    tune.register_env(
        "repeat_initial", lambda _: RepeatInitialObsEnv(episode_len=100))
    tune.register_env(
        "repeat_after_me", lambda _: RepeatAfterMeEnv({"repeat_delay": 1}))
    tune.register_env("stateless_cartpole", lambda _: StatelessCartPole())

    config = {
        "env": args.env,
        "use_pytorch": True,
        "num_workers": 0,
        "num_envs_per_worker": 20,
        "gamma": 0.9,
        "entropy_coeff": 0.0001,
        "model": {
            "custom_model": "rnn",
            "max_seq_len": 20,
            "lstm_use_prev_action_reward": "store_true",
            "custom_options": {
                "fc_size": args.fc_size,
                "lstm_state_size": args.lstm_cell_size,
            }
        },
        "lr": 3e-4,
        "num_sgd_iter": 5,
        "vf_loss_coeff": 0.0003,
    }

    tune.run(
        args.run,
        stop={
            "episode_reward_mean": args.stop,
            "timesteps_total": 100000
        },
        config=config,
    )
