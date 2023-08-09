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

from gymnasium.spaces import Box, Discrete
import ray
from ray.rllib.algorithms import ppo
from ray.rllib.examples.env.action_mask_env import ActionMaskEnv
from ray.rllib.examples.rl_module.action_masking_rlm import (
    TorchActionMaskRLM,
    TFActionMaskRLM,
)
from ray.rllib.core.rl_module.rl_module import SingleAgentRLModuleSpec

from ray.tune.logger import pretty_print


def get_cli_args():
    """Create CLI parser and return parsed arguments"""
    parser = argparse.ArgumentParser()

    parser.add_argument("--num-cpus", type=int, default=0)
    parser.add_argument(
        "--framework",
        choices=["tf", "tf2", "torch"],
        default="torch",
        help="The DL framework specifier.",
    )
    parser.add_argument(
        "--stop-iters", type=int, default=10, help="Number of iterations to train."
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

    if args.framework == "torch":
        rlm_class = TorchActionMaskRLM
    elif args.framework == "tf2":
        rlm_class = TFActionMaskRLM
    else:
        raise ValueError(f"Unsupported framework: {args.framework}")

    rlm_spec = SingleAgentRLModuleSpec(module_class=rlm_class)

    # main part: configure the ActionMaskEnv and ActionMaskModel
    config = (
        ppo.PPOConfig()
        .environment(
            # random env with 100 discrete actions and 5x [-1,1] observations
            # some actions are declared invalid and lead to errors
            ActionMaskEnv,
            env_config={
                "action_space": Discrete(100),
                # This is not going to be the observation space that our RLModule sees.
                # It's only the configuration provided to the environment.
                # The environment will instead create Dict observations with
                # the keys "observations" and "action_mask".
                "observation_space": Box(-1.0, 1.0, (5,)),
            },
        )
        # We need to disable preprocessing of observations, because preprocessing
        # would flatten the observation dict of the environment.
        .experimental(_disable_preprocessor_api=True)
        .framework(args.framework)
        .resources(
            # Use GPUs iff `RLLIB_NUM_GPUS` env var set to > 0.
            num_gpus=int(os.environ.get("RLLIB_NUM_GPUS", "0"))
        )
        .rl_module(rl_module_spec=rlm_spec)
    )

    algo = config.build()

    # run manual training loop and print results after each iteration
    for _ in range(args.stop_iters):
        result = algo.train()
        print(pretty_print(result))

    # manual test loop
    print("Finished training. Running manual test/inference loop.")
    # prepare environment with max 10 steps
    config["env_config"]["max_episode_len"] = 10
    env = ActionMaskEnv(config["env_config"])
    obs, info = env.reset()
    done = False
    # run one iteration until done
    print(f"ActionMaskEnv with {config['env_config']}")
    while not done:
        action = algo.compute_single_action(obs)
        next_obs, reward, done, truncated, _ = env.step(action)
        # observations contain original observations and the action mask
        # reward is random and irrelevant here and therefore not printed
        print(f"Obs: {obs}, Action: {action}")
        obs = next_obs

    print("Finished successfully without selecting invalid actions.")
    ray.shutdown()
