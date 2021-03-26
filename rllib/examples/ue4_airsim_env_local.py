"""
Example of running an RLlib Trainer against a locally running UE4 editor
instance (available as UnrealEngine4AirSimCarEnv inside RLlib).


TODO: instructions how to setup.
TODO: Client/Server example #For a distributed cloud setup example with Unity,
#see `examples/serving/unity3d_[server|client].py`

"""

import argparse
import os

import ray
from ray import tune
from ray.rllib.env.wrappers.ue4_airsim_env import UnrealEngine4AirSimCarEnv
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
parser.add_argument(
    "--horizon",
    type=int,
    default=3000,
    help="The max. number of `step()`s for any episode (per agent) before "
    "it'll be reset again automatically.")
parser.add_argument("--torch", action="store_true")

if __name__ == "__main__":
    ray.init()

    args = parser.parse_args()

    tune.register_env(
        "ue4_airsim_car",
        lambda c: UnrealEngine4AirSimCarEnv(
            episode_horizon=c["episode_horizon"],
        ))

    # Get policies (different agent types; "behaviors" in MLAgents) and
    # the mappings from individual agents to Policies.
    policies, policy_mapping_fn = \
        UnrealEngine4AirSimCarEnv.get_policy_configs_for_game()

    config = {
        "env": "ue4_airsim_car",
        "env_config": {
            "file_name": args.file_name,
            "episode_horizon": args.horizon,
        },
        # For running in editor, force to use just one Worker (we only have
        # one Unity running)!
        "num_workers": args.num_workers if args.file_name else 0,
        # Other settings.
        "lr": 0.0003,
        "lambda": 0.95,
        "gamma": 0.99,
        "sgd_minibatch_size": 256,
        "train_batch_size": 4000,
        # Use GPUs iff `RLLIB_NUM_GPUS` env var set to > 0.
        "num_gpus": int(os.environ.get("RLLIB_NUM_GPUS", "0")),
        "num_sgd_iter": 20,
        "rollout_fragment_length": 200,
        "clip_param": 0.2,
        # Multi-agent setup for the particular env.
        "multiagent": {
            "policies": policies,
            "policy_mapping_fn": policy_mapping_fn,
        },
        "model": {
            "fcnet_hiddens": [512, 512],
        },
        "framework": "tf" if args.env != "Pyramids" else "torch",
        "no_done_at_end": True,
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
        checkpoint_freq=5,
        checkpoint_at_end=True,
        restore=args.from_checkpoint)

    # And check the results.
    if args.as_test:
        check_learning_achieved(results, args.stop_reward)

    ray.shutdown()
