import argparse

import ray
from ray import tune
from ray.rllib.examples.models.trajectory_view_utilizing_models import \
    FrameStackingCartPoleModel, TorchFrameStackingCartPoleModel
from ray.rllib.models.catalog import ModelCatalog
from ray.rllib.utils.framework import try_import_tf
from ray.rllib.utils.test_utils import check_learning_achieved

tf1, tf, tfv = try_import_tf()

parser = argparse.ArgumentParser()
parser.add_argument("--run", type=str, default="PPO")
parser.add_argument(
    "--framework", choices=["tf2", "tf", "tfe", "torch"], default="tf")
parser.add_argument("--as-test", action="store_true")
parser.add_argument("--stop-iters", type=int, default=50)
parser.add_argument("--stop-timesteps", type=int, default=100000)
parser.add_argument("--stop-reward", type=float, default=150.0)

if __name__ == "__main__":
    args = parser.parse_args()
    ray.init(num_cpus=3)

    ModelCatalog.register_custom_model(
        "frame_stack_model", FrameStackingCartPoleModel
        if args.framework != "torch" else TorchFrameStackingCartPoleModel)

    config = {
        "env": "CartPole-v0",
        "model": {
            "custom_model": "frame_stack_model",
            "custom_model_config": {
                "num_frames": 4,
            }
        },
        "framework": args.framework,
    }

    stop = {
        "training_iteration": args.stop_iters,
        "timesteps_total": args.stop_timesteps,
        "episode_reward_mean": args.stop_reward,
    }
    results = tune.run(args.run, config=config, stop=stop, verbose=2)

    if args.as_test:
        check_learning_achieved(results, args.stop_reward)
    ray.shutdown()
