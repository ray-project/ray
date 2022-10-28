"""Example showing how to use "action masking" in RLlib.

"Action masking" allows the agent to select actions based on the current
observation. This is useful in many practical scenarios, where different
actions are available in different time steps.
Blog post explaining action masking: https://boring-guy.sh/posts/masking-rl/

RLlib supports action masking, i.e., disallowing these actions based on the
observation, by slightly adjusting the environment and the model as shown in
this example.

Here, the ActionMaskEnv wraps an underlying environment (here, RandomEnv),
defining only a subset of all actions as valid based on the environment's
observations. If an invalid action is selected, the environment raises an error
- this must not happen!

The environment constructs Dict observations, where obs["observations"] holds
the original observations and obs["action_mask"] holds the valid actions.
To avoid selection invalid actions, the ActionMaskModel is used. This model
takes the original observations, computes the logits of the corresponding
actions and then sets the logits of all invalid actions to zero, thus disabling
them. This only works with discrete actions.

---
Run this example with defaults (using Tune and action masking):

  $ python action_masking.py

Then run again without action masking, which will likely lead to errors due to
invalid actions being selected (ValueError "Invalid action sent to env!"):

  $ python action_masking.py --no-masking

Other options for running this example:

  $ python action_masking.py --help
"""

import argparse
import os

from gym.spaces import Box, Discrete
import ray
from ray import air, tune
from ray.rllib.algorithms import ppo
from ray.rllib.examples.env.action_mask_env import ActionMaskEnv
from ray.rllib.examples.models.action_mask_model import (
    ActionMaskModel,
    TorchActionMaskModel,
)
from ray.tune.logger import pretty_print


def get_cli_args():
    """Create CLI parser and return parsed arguments"""
    parser = argparse.ArgumentParser()

    # example-specific args
    parser.add_argument(
        "--no-masking",
        action="store_true",
        help="Do NOT mask invalid actions. This will likely lead to errors.",
    )

    # general args
    parser.add_argument(
        "--run", type=str, default="APPO", help="The RLlib-registered algorithm to use."
    )
    parser.add_argument("--num-cpus", type=int, default=0)
    parser.add_argument(
        "--framework",
        choices=["tf", "tf2", "torch"],
        default="tf",
        help="The DL framework specifier.",
    )
    parser.add_argument("--eager-tracing", action="store_true")
    parser.add_argument(
        "--stop-iters", type=int, default=10, help="Number of iterations to train."
    )
    parser.add_argument(
        "--stop-timesteps",
        type=int,
        default=10000,
        help="Number of timesteps to train.",
    )
    parser.add_argument(
        "--stop-reward",
        type=float,
        default=80.0,
        help="Reward at which we stop training.",
    )
    parser.add_argument(
        "--no-tune",
        action="store_true",
        help="Run without Tune using a manual train loop instead. Here,"
        "there is no TensorBoard support.",
    )
    parser.add_argument(
        "--local-mode",
        action="store_true",
        help="Init Ray in local mode for easier debugging.",
    )

    args = parser.parse_args()
    print(f"Running with following CLI args: {args}")
    return args


if __name__ == "__main__":
    args = get_cli_args()

    ray.init(num_cpus=args.num_cpus or None, local_mode=args.local_mode)

    # main part: configure the ActionMaskEnv and ActionMaskModel
    config = {
        # random env with 100 discrete actions and 5x [-1,1] observations
        # some actions are declared invalid and lead to errors
        "env": ActionMaskEnv,
        "env_config": {
            "action_space": Discrete(100),
            "observation_space": Box(-1.0, 1.0, (5,)),
        },
        # the ActionMaskModel retrieves the invalid actions and avoids them
        "model": {
            "custom_model": ActionMaskModel
            if args.framework != "torch"
            else TorchActionMaskModel,
            # disable action masking according to CLI
            "custom_model_config": {"no_masking": args.no_masking},
        },
        # Use GPUs iff `RLLIB_NUM_GPUS` env var set to > 0.
        "num_gpus": int(os.environ.get("RLLIB_NUM_GPUS", "0")),
        "framework": args.framework,
        # Run with tracing enabled for tf2?
        "eager_tracing": args.eager_tracing,
    }

    stop = {
        "training_iteration": args.stop_iters,
        "timesteps_total": args.stop_timesteps,
        "episode_reward_mean": args.stop_reward,
    }

    # manual training loop (no Ray tune)
    if args.no_tune:
        if args.run not in {"APPO", "PPO"}:
            raise ValueError("This example only supports APPO and PPO.")
        ppo_config = ppo.DEFAULT_CONFIG.copy()
        ppo_config.update(config)
        trainer = ppo.PPO(config=ppo_config, env=ActionMaskEnv)
        # run manual training loop and print results after each iteration
        for _ in range(args.stop_iters):
            result = trainer.train()
            print(pretty_print(result))
            # stop training if the target train steps or reward are reached
            if (
                result["timesteps_total"] >= args.stop_timesteps
                or result["episode_reward_mean"] >= args.stop_reward
            ):
                break

        # manual test loop
        print("Finished training. Running manual test/inference loop.")
        # prepare environment with max 10 steps
        config["env_config"]["max_episode_len"] = 10
        env = ActionMaskEnv(config["env_config"])
        obs = env.reset()
        done = False
        # run one iteration until done
        print(f"ActionMaskEnv with {config['env_config']}")
        while not done:
            action = trainer.compute_single_action(obs)
            next_obs, reward, done, _ = env.step(action)
            # observations contain original observations and the action mask
            # reward is random and irrelevant here and therefore not printed
            print(f"Obs: {obs}, Action: {action}")
            obs = next_obs

    # run with tune for auto trainer creation, stopping, TensorBoard, etc.
    else:
        tuner = tune.Tuner(
            args.run, param_space=config, run_config=air.RunConfig(stop=stop, verbose=2)
        )
        tuner.fit()

    print("Finished successfully without selecting invalid actions.")
    ray.shutdown()
