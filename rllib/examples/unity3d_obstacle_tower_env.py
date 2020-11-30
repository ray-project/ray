"""
Example of running an RLlib Trainer against Unity3D's Obstacle Tower
environment.
"""

import argparse
from obstacle_tower_env import ObstacleTowerEnv
import os

import ray
from ray import tune
from ray.rllib.utils.test_utils import check_learning_achieved

parser = argparse.ArgumentParser()
parser.add_argument(
    "--from-checkpoint",
    type=str,
    default=None,
    help="Full path to a checkpoint file for restoring a previously saved "
    "Trainer state.")
parser.add_argument("--num-workers", type=int, default=0)
parser.add_argument("--as-test", action="store_true")
parser.add_argument("--stop-iters", type=int, default=9999)
parser.add_argument("--stop-reward", type=float, default=9999.0)
parser.add_argument("--stop-timesteps", type=int, default=10000000)
parser.add_argument("--torch", action="store_true")

if __name__ == "__main__":
    ray.init(local_mode=True)#TODO

    args = parser.parse_args()

    tune.register_env("obstacle_tower", lambda c: ObstacleTowerEnv(**c))

    config = {
        "env": "obstacle_tower",
        # See here for more env options.
        # https://github.com/Unity-Technologies/obstacle-tower-env/
        # blob/master/reset-parameters.md
        "env_config": {
            "retro": False,
            "realtime_mode": False,
        },
        # For running in editor, force to use just one Worker (we only have
        # one Unity running)!
        "num_workers": args.num_workers,
        # Other settings.
        "lr": 0.0003,
        "lambda": 0.95,
        "gamma": 0.99,
        "sgd_minibatch_size": 500,
        "train_batch_size": 5000,
        # Use GPUs iff `RLLIB_NUM_GPUS` env var set to > 0.
        "num_gpus": int(os.environ.get("RLLIB_NUM_GPUS", "0")),
        "num_sgd_iter": 20,
        "rollout_fragment_length": 200,
        "clip_param": 0.1,
        "model": {
            "max_seq_len": 100,
            "use_lstm": True,
            "lstm_use_prev_action": True,
            "lstm_use_prev_reward": True,
            "lstm_cell_size": 256,
        },
        "framework": "torch" if args.torch else "tf",
    }

    stop = {
        "training_iteration": args.stop_iters,
        "timesteps_total": args.stop_timesteps,
        "episode_reward_mean": args.stop_reward,
    }

    # Run the experiment.
    results = tune.run(
        "PPO",
        config=config,
        stop=stop,
        verbose=1,
        checkpoint_freq=20,
        restore=args.from_checkpoint)

    # And check the results.
    if args.as_test:
        check_learning_achieved(results, args.stop_reward)

    ray.shutdown()
