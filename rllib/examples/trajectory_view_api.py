import argparse
import numpy as np

import ray
from ray import air, tune
from ray.rllib.algorithms.algorithm import Algorithm
from ray.rllib.examples.env.stateless_cartpole import StatelessCartPole
from ray.rllib.examples.models.trajectory_view_utilizing_models import (
    FrameStackingCartPoleModel,
    TorchFrameStackingCartPoleModel,
)
from ray.rllib.models.catalog import ModelCatalog
from ray.rllib.utils.framework import try_import_tf
from ray.rllib.utils.test_utils import check_learning_achieved
from ray.tune.registry import get_trainable_cls

tf1, tf, tfv = try_import_tf()

parser = argparse.ArgumentParser()
parser.add_argument(
    "--run", type=str, default="PPO", help="The RLlib-registered algorithm to use."
)
parser.add_argument(
    "--framework",
    choices=["tf", "tf2", "torch"],
    default="torch",
    help="The DL framework specifier.",
)
parser.add_argument(
    "--as-test",
    action="store_true",
    help="Whether this script should be run as a test: --stop-reward must "
    "be achieved within --stop-timesteps AND --stop-iters.",
)
parser.add_argument(
    "--stop-iters", type=int, default=50, help="Number of iterations to train."
)
parser.add_argument(
    "--stop-timesteps", type=int, default=200000, help="Number of timesteps to train."
)
parser.add_argument(
    "--stop-reward", type=float, default=150.0, help="Reward at which we stop training."
)

if __name__ == "__main__":
    args = parser.parse_args()
    ray.init(num_cpus=3)

    num_frames = 16

    ModelCatalog.register_custom_model(
        "frame_stack_model",
        FrameStackingCartPoleModel
        if args.framework != "torch"
        else TorchFrameStackingCartPoleModel,
    )

    config = (
        get_trainable_cls(args.run)
        .get_default_config()
        .environment(StatelessCartPole)
        .framework(args.framework)
        .training(
            model={
                "vf_share_layers": True,
                "custom_model": "frame_stack_model",
                "custom_model_config": {
                    "num_frames": num_frames,
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
            # TODO (Kourosh): This example needs to be migrated to the new RLModule API
            # stack.
            _enable_learner_api=False,
        )
        .rl_module(_enable_rl_module_api=False)
    )
    if args.run == "PPO":
        config.training(
            num_sgd_iter=5,
            vf_loss_coeff=0.0001,
        )

    stop = {
        "training_iteration": args.stop_iters,
        "timesteps_total": args.stop_timesteps,
        "episode_reward_mean": args.stop_reward,
    }
    results = tune.Tuner(
        args.run,
        param_space=config,
        run_config=air.RunConfig(
            stop=stop,
            verbose=2,
            checkpoint_config=air.CheckpointConfig(checkpoint_at_end=True),
        ),
    ).fit()

    if args.as_test:
        check_learning_achieved(results, args.stop_reward)

    ckpt = results.get_best_result(metric="episode_reward_mean", mode="max").checkpoint

    algo = Algorithm.from_checkpoint(ckpt)

    # Inference loop.
    env = StatelessCartPole()

    # Run manual inference loop for n episodes.
    for _ in range(10):
        episode_reward = 0.0
        reward = 0.0
        action = 0
        terminated = truncated = False
        obs, info = env.reset()
        while not terminated and not truncated:
            # Create a dummy action using the same observation n times,
            # as well as dummy prev-n-actions and prev-n-rewards.
            action, state, logits = algo.compute_single_action(
                input_dict={
                    "obs": obs,
                    "prev_n_obs": np.stack([obs for _ in range(num_frames)]),
                    "prev_n_actions": np.stack([0 for _ in range(num_frames)]),
                    "prev_n_rewards": np.stack([1.0 for _ in range(num_frames)]),
                },
                full_fetch=True,
            )
            obs, reward, terminated, truncated, info = env.step(action)
            episode_reward += reward

        print(f"Episode reward={episode_reward}")

    algo.stop()

    ray.shutdown()
