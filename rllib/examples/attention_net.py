import argparse
import os

import ray
from ray import tune
from ray.rllib.examples.env.look_and_push import LookAndPush, OneHot
from ray.rllib.examples.env.repeat_after_me_env import RepeatAfterMeEnv
from ray.rllib.examples.env.repeat_initial_obs_env import RepeatInitialObsEnv
from ray.rllib.examples.env.stateless_cartpole import StatelessCartPole
from ray.rllib.utils.framework import try_import_tf
from ray.rllib.utils.test_utils import check_learning_achieved
from ray.tune import registry

tf1, tf, tfv = try_import_tf()

parser = argparse.ArgumentParser()
parser.add_argument(
    "--run",
    type=str,
    default="PPO",
    help="The RLlib-registered algorithm to use.")
parser.add_argument("--env", type=str, default="RepeatAfterMeEnv")
parser.add_argument("--num-cpus", type=int, default=3)
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
    default=200,
    help="Number of iterations to train.")
parser.add_argument(
    "--stop-timesteps",
    type=int,
    default=500000,
    help="Number of timesteps to train.")
parser.add_argument(
    "--stop-reward",
    type=float,
    default=80.0,
    help="Reward at which we stop training.")

if __name__ == "__main__":
    args = parser.parse_args()

    ray.init(num_cpus=args.num_cpus or None)

    registry.register_env("RepeatAfterMeEnv", lambda c: RepeatAfterMeEnv(c))
    registry.register_env("RepeatInitialObsEnv",
                          lambda _: RepeatInitialObsEnv())
    registry.register_env("LookAndPush", lambda _: OneHot(LookAndPush()))
    registry.register_env("StatelessCartPole", lambda _: StatelessCartPole())

    config = {
        "env": args.env,
        # This env_config is only used for the RepeatAfterMeEnv env.
        "env_config": {
            "repeat_delay": 2,
        },
        "gamma": 0.99,
        # Use GPUs iff `RLLIB_NUM_GPUS` env var set to > 0.
        "num_gpus": int(os.environ.get("RLLIB_NUM_GPUS", 0)),
        "num_envs_per_worker": 20,
        "entropy_coeff": 0.001,
        "num_sgd_iter": 10,
        "vf_loss_coeff": 1e-5,
        "model": {
            # Attention net wrapping (for tf) can already use the native keras
            # model versions. For torch, this will have no effect.
            "_use_default_native_models": True,
            "use_attention": True,
            "max_seq_len": 10,
            "attention_num_transformer_units": 1,
            "attention_dim": 32,
            "attention_memory_inference": 10,
            "attention_memory_training": 10,
            "attention_num_heads": 1,
            "attention_head_dim": 32,
            "attention_position_wise_mlp_dim": 32,
        },
        "framework": args.framework,
    }

    stop = {
        "training_iteration": args.stop_iters,
        "timesteps_total": args.stop_timesteps,
        "episode_reward_mean": args.stop_reward,
    }

    # To run the Trainer without tune.run, using the attention net and
    # manual state-in handling, do the following:

    # Example (use `config` from the above code):
    # >> import numpy as np
    # >> from ray.rllib.agents.ppo import PPOTrainer
    # >>
    # >> trainer = PPOTrainer(config)
    # >> num_transformers = config["model"]["attention_num_transformer_units"]
    # >> env = RepeatAfterMeEnv({})
    # >> obs = env.reset()
    # >> init_state = state = [
    # ..     np.zeros([100, 32], np.float32) for _ in range(num_transformers)
    # .. ]
    # >> while True:
    # >>     a, state_out, _ = trainer.compute_single_action(obs, state)
    # >>     obs, reward, done, _ = env.step(a)
    # >>     if done:
    # >>         obs = env.reset()
    # >>         state = init_state
    # >>     else:
    # >>         state = [
    # ..             np.concatenate([state[i], [state_out[i]]], axis=0)[1:]
    # ..             for i in range(num_transformers)
    # ..         ]

    # We use tune here, which handles env and trainer creation for us.
    results = tune.run(args.run, config=config, stop=stop, verbose=2)

    if args.as_test:
        check_learning_achieved(results, args.stop_reward)
    ray.shutdown()
