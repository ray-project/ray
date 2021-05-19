import argparse

import ray
from ray import tune
from ray.rllib.examples.env.stateless_cartpole import StatelessCartPole
from ray.rllib.examples.models.trajectory_view_utilizing_models import \
    FrameStackingCartPoleModel, TorchFrameStackingCartPoleModel
from ray.rllib.models.catalog import ModelCatalog
from ray.rllib.utils.framework import try_import_tf
from ray.rllib.utils.test_utils import check_learning_achieved

tf1, tf, tfv = try_import_tf()

parser = argparse.ArgumentParser()
parser.add_argument(
    "--run",
    type=str,
    default="PPO",
    help="The RLlib-registered algorithm to use.")
parser.add_argument(
    "--framework",
    choices=["tf", "tf2", "tfe", "torch"],
    default="tf",
    help="The DL framework specifier.")
parser.add_argument(
    "--as-test",
    action="store_true",
    help="Whether this script should be run as a test: --stop-reward must "
    "be achieved within --stop-timesteps AND --stop-iters.")
parser.add_argument(
    "--stop-iters",
    type=int,
    default=50,
    help="Number of iterations to train.")
parser.add_argument(
    "--stop-timesteps",
    type=int,
    default=200000,
    help="Number of timesteps to train.")
parser.add_argument(
    "--stop-reward",
    type=float,
    default=150.0,
    help="Reward at which we stop training.")

if __name__ == "__main__":
    args = parser.parse_args()
    ray.init(num_cpus=3)

    ModelCatalog.register_custom_model(
        "frame_stack_model", FrameStackingCartPoleModel
        if args.framework != "torch" else TorchFrameStackingCartPoleModel)
    tune.register_env("stateless_cartpole", lambda c: StatelessCartPole())

    config = {
        "env": "stateless_cartpole",
        "model": {
            "vf_share_layers": True,
            "custom_model": "frame_stack_model",
            "custom_model_config": {
                "num_frames": 16,
            },

            # To compare against a simple LSTM:
            # "use_lstm": True,
            # "lstm_use_prev_action": True,
            # "lstm_use_prev_reward": True,

            # To compare against a simple attention net:
            # "use_attention": True,
            # "attention_use_n_prev_actions": 1,
            # "attention_use_n_prev_rewards": 1,
        },
        "num_sgd_iter": 5,
        "vf_loss_coeff": 0.0001,
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
