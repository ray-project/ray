"""Example of using a custom RNN keras model."""

import argparse
import os

import ray
from ray import air, tune
from ray.tune.registry import register_env
from ray.rllib.examples.env.repeat_after_me_env import RepeatAfterMeEnv
from ray.rllib.examples.env.repeat_initial_obs_env import RepeatInitialObsEnv
from ray.rllib.examples.models.rnn_model import RNNModel, TorchRNNModel
from ray.rllib.models import ModelCatalog
from ray.rllib.utils.test_utils import check_learning_achieved
from ray.tune.registry import get_trainable_cls

parser = argparse.ArgumentParser()
parser.add_argument(
    "--run", type=str, default="PPO", help="The RLlib-registered algorithm to use."
)
parser.add_argument("--env", type=str, default="RepeatAfterMeEnv")
parser.add_argument("--num-cpus", type=int, default=0)
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
    "--stop-iters", type=int, default=100, help="Number of iterations to train."
)
parser.add_argument(
    "--stop-timesteps", type=int, default=100000, help="Number of timesteps to train."
)
parser.add_argument(
    "--stop-reward", type=float, default=90.0, help="Reward at which we stop training."
)
parser.add_argument(
    "--local-mode",
    action="store_true",
    help="Init Ray in local mode for easier debugging.",
)

if __name__ == "__main__":
    args = parser.parse_args()

    ray.init(num_cpus=args.num_cpus or None, local_mode=args.local_mode)

    ModelCatalog.register_custom_model(
        "rnn", TorchRNNModel if args.framework == "torch" else RNNModel
    )
    register_env("RepeatAfterMeEnv", lambda c: RepeatAfterMeEnv(c))
    register_env("RepeatInitialObsEnv", lambda _: RepeatInitialObsEnv())

    config = (
        get_trainable_cls(args.run)
        .get_default_config()
        .environment(args.env, env_config={"repeat_delay": 2})
        .framework(args.framework)
        .rollouts(num_rollout_workers=0, num_envs_per_worker=20)
        .training(
            model={
                "custom_model": "rnn",
                "max_seq_len": 20,
                "custom_model_config": {
                    "cell_size": 32,
                },
            },
            gamma=0.9,
            # TODO (Kourosh): Enable when LSTMs are supported.
            _enable_learner_api=False,
        )
        # Use GPUs iff `RLLIB_NUM_GPUS` env var set to > 0.
        .resources(num_gpus=int(os.environ.get("RLLIB_NUM_GPUS", "0")))
        .rl_module(_enable_rl_module_api=False)
    )

    if args.run == "PPO":
        config.training(entropy_coeff=0.001, num_sgd_iter=5, vf_loss_coeff=1e-5)

    stop = {
        "training_iteration": args.stop_iters,
        "timesteps_total": args.stop_timesteps,
        "episode_reward_mean": args.stop_reward,
    }

    # To run the Algorithm without ``Tuner.fit()``, using our RNN model and
    # manual state-in handling, do the following:

    # Example (use `config` from the above code):
    # >> import numpy as np
    # >> from ray.rllib.algorithms.ppo import PPO
    # >>
    # >> algo = config.build()
    # >> lstm_cell_size = config.model["custom_model_config"]["cell_size"]
    # >> env = RepeatAfterMeEnv({})
    # >> obs, info = env.reset()
    # >>
    # >> # range(2) b/c h- and c-states of the LSTM.
    # >> init_state = state = [
    # ..     np.zeros([lstm_cell_size], np.float32) for _ in range(2)
    # .. ]
    # >>
    # >> while True:
    # >>     a, state_out, _ = algo.compute_single_action(obs, state)
    # >>     obs, reward, done, _, _ = env.step(a)
    # >>     if done:
    # >>         obs, info = env.reset()
    # >>         state = init_state
    # >>     else:
    # >>         state = state_out

    tuner = tune.Tuner(
        args.run,
        param_space=config.to_dict(),
        run_config=air.RunConfig(stop=stop, verbose=1),
    )
    results = tuner.fit()

    if args.as_test:
        check_learning_achieved(results, args.stop_reward)
    ray.shutdown()
