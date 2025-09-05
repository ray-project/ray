"""
Multi-agent RLlib Footsies Simplified Example (PPO)

About:
    - This example as a simplified version of "rllib/tuned_examples/ppo/multi_agent_footsies_ppo.py",
      which has more detailed comments and instructions. Please refer to that example for more information.
    - This example is created to test the self-play training progression with footsies.
    - Simplified version runs with single learner (cpu), single env runner, and single eval env runner.
"""
from pathlib import Path

from ray.rllib.tuned_examples.ppo.multi_agent_footsies_ppo import (
    config,
    env_creator,
    stop,
)
from ray.rllib.utils.test_utils import (
    add_rllib_example_script_args,
)
from ray.tune.registry import register_env

parser = add_rllib_example_script_args(
    default_iters=500,
    default_timesteps=5_000_000,
)
parser.add_argument(
    "--train-start-port",
    type=int,
    default=45001,
    help="First port number for the Footsies training environment server (default: 45001). Each server gets its own port.",
)
parser.add_argument(
    "--eval-start-port",
    type=int,
    default=55001,
    help="First port number for the Footsies evaluation environment server (default: 55001) Each server gets its own port.",
)
parser.add_argument(
    "--binary-download-dir",
    type=Path,
    default="/tmp/ray/binaries/footsies",
    help="Directory to download Footsies binaries (default: /tmp/ray/binaries/footsies)",
)
parser.add_argument(
    "--binary-extract-dir",
    type=Path,
    default="/tmp/ray/binaries/footsies",
    help="Directory to extract Footsies binaries (default: /tmp/ray/binaries/footsies)",
)
parser.add_argument(
    "--binary-to-download",
    type=str,
    choices=["linux_server", "linux_windowed", "mac_headless", "mac_windowed"],
    default="linux_server",
    help="Target binary for Footsies environment (default: linux_server). Linux and Mac machines are supported. "
    "'linux_server' and 'mac_headless' choices are the default options for the training. Game will run in the batchmode, without initializing the graphics. "
    "'linux_windowed' and 'mac_windowed' choices are for the local run only, because "
    "game will be rendered in the OS window. To use this option effectively, set up: "
    "--no-tune --num-env-runners 0 --evaluation-num-env-runners 0",
)
parser.add_argument(
    "--win-rate-threshold",
    type=float,
    default=0.55,
    help="The main policy should have at least 'win-rate-threshold' win rate against the "
    "other policy to advance to the next level. Moving to the next level "
    "means adding a new policy to the mix.",
)
parser.add_argument(
    "--target-mix-size",
    type=int,
    default=4,
    help="Target number of policies (RLModules) in the mix to consider the test passed. "
    "The initial mix size is 2: 'main policy' vs. 'other'. "
    "`--target-mix-size=4` means that 2 new policies will be added to the mix. "
    "Whether to add new policy is decided by checking the '--win-rate-threshold' condition. ",
)
parser.add_argument(
    "--rollout-fragment-length",
    type=int,
    default=256,
    help="The length of each rollout fragment to be collected by the EnvRunners when sampling.",
)

args = parser.parse_args()
register_env(name="FootsiesEnv", env_creator=env_creator)
stop["mix_size"] = args.target_mix_size

config.environment(
    env="FootsiesEnv",
    env_config={
        "train_start_port": args.train_start_port,
        "eval_start_port": args.eval_start_port,
        "binary_download_dir": args.binary_download_dir,
        "binary_extract_dir": args.binary_extract_dir,
        "binary_to_download": args.binary_to_download,
    },
).training(
    train_batch_size_per_learner=args.rollout_fragment_length
    * (args.num_env_runners or 1),
)


if __name__ == "__main__":
    from ray.rllib.utils.test_utils import run_rllib_example_script_experiment

    results = run_rllib_example_script_experiment(
        base_config=config,
        args=args,
        stop=stop,
        success_metric={"mix_size": args.target_mix_size},
    )
