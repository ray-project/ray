import argparse


import ray
from ray import air, tune
from ray.rllib.algorithms.ppo import PPOConfig
from ray.rllib.core.rl_module.rl_module import SingleAgentRLModuleSpec
from ray.rllib.examples.env.stateless_cartpole import StatelessCartPole
from ray.rllib.examples.rl_module.frame_stacking_rlm import (
    TorchFrameStackingCartPoleRLM,
    TfFrameStackingCartPoleRLM,
)
from ray.rllib.utils.framework import try_import_tf
from ray.rllib.utils.test_utils import check_learning_achieved

tf1, tf, tfv = try_import_tf()

parser = argparse.ArgumentParser()
parser.add_argument(
    "--framework",
    choices=["tf2", "torch"],
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
parser.add_argument(
    "--local-mode",
    action="store_true",
    help="Run ray in local mode for easier debugging.",
)


if __name__ == "__main__":
    args = parser.parse_args()
    ray.init(local_mode=args.local_mode)

    module = (
        TorchFrameStackingCartPoleRLM
        if args.framework == "torch"
        else TfFrameStackingCartPoleRLM
    )

    # We only need to fill in FrameStackingCartPoleRLM here, RLlib will fill in the
    # defaults for us.
    frame_stacking_rlm_spec = SingleAgentRLModuleSpec(
        module_class=module,
    )

    config = (
        PPOConfig()
        .environment(StatelessCartPole)
        .framework(args.framework)
        .training(num_sgd_iter=5, vf_loss_coeff=0.0001)
        .rl_module(rl_module_spec=frame_stacking_rlm_spec)
    )

    stop = {
        "training_iteration": args.stop_iters,
        "timesteps_total": args.stop_timesteps,
        "episode_reward_mean": args.stop_reward,
    }
    results = tune.Tuner(
        "PPO",
        param_space=config.to_dict(),
        run_config=air.RunConfig(
            stop=stop,
            verbose=2,
            checkpoint_config=air.CheckpointConfig(checkpoint_at_end=True),
        ),
    ).fit()

    # TODO (Artur): Add inference code here and include in docs

    if args.as_test:
        check_learning_achieved(results, args.stop_reward)

    ray.shutdown()
