"""
Multi-agent RLlib Footsies Simplified Example (PPO)

About:
    - This example as a simplified version of "rllib/examples/ppo/multi_agent_footsies_ppo.py",
      which has more detailed comments and instructions. Please refer to that example for more information.
    - This example is created to test the self-play training progression with footsies.
    - Simplified version runs with single learner (cpu), single env runner, and single eval env runner.
"""
import platform
from pathlib import Path

from ray.rllib.examples.algorithms.ppo.multi_agent_footsies_ppo import (
    config,
    env_creator,
    stop,
)
from ray.rllib.examples.utils import (
    add_rllib_example_script_args,
    run_rllib_example_script_experiment,
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
    "--render",
    action="store_true",
    default=False,
    help="Whether to render the Footsies environment. Default is False.",
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
parser.add_argument(
    "--log-unity-output",
    action="store_true",
    help="Whether to log Unity output (from the game engine). Default is False.",
    default=False,
)

args = parser.parse_args()
register_env(name="FootsiesEnv", env_creator=env_creator)
stop["mix_size"] = args.target_mix_size

# Detect platform and choose appropriate binary
if platform.system() == "Darwin":
    if args.render:
        binary_to_download = "mac_windowed"
    else:
        binary_to_download = "mac_headless"
elif platform.system() == "Linux":
    if args.render:
        binary_to_download = "linux_windowed"
    else:
        binary_to_download = "linux_server"
else:
    raise RuntimeError(f"Unsupported platform: {platform.system()}")


config.environment(
    env="FootsiesEnv",
    env_config={
        "train_start_port": args.train_start_port,
        "eval_start_port": args.eval_start_port,
        "binary_download_dir": args.binary_download_dir,
        "binary_extract_dir": args.binary_extract_dir,
        "binary_to_download": binary_to_download,
        "log_unity_output": args.log_unity_output,
    },
).training(
    train_batch_size_per_learner=args.rollout_fragment_length
    * (args.num_env_runners or 1),
)


if __name__ == "__main__":
    results = run_rllib_example_script_experiment(
        base_config=config,
        args=args,
        stop=stop,
        success_metric={"mix_size": args.target_mix_size},
    )
