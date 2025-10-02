"""Script to execute RLlib's official PPO Atari benchmarks.

How to run this script
----------------------
`python [script-name].py --stop-timesteps 12000000
--num-learners=4 --num-gpus-per-learner --num-env-runners=95`

In order to only run individual or lists of envs, you can provide a list of env-strings
under the `--env` arg, such as `--env=ale_py:ALE/Pong-v5,ale_py:ALE/Breakout-v5`.

For logging to your WandB account, use:
`--wandb-key=[your WandB API key] --wandb-project=[some project name]
--wandb-run-name=[optional: WandB run name (within the defined project)]`


Results to expect
-----------------
TODO (sven): Link to RLlib's to-be-created benchmark page.
"""
import subprocess

from ray.rllib.utils.test_utils import add_rllib_example_script_args

parser = add_rllib_example_script_args()

# Might need `gymnasium[atari, other]` to be installed.

# See the following links for benchmark results of other libraries:
#   Original paper: https://arxiv.org/abs/1812.05905
#   CleanRL: https://wandb.ai/cleanrl/cleanrl.benchmark/reports/Mujoco--VmlldzoxODE0NjE
#   AgileRL: https://github.com/AgileRL/AgileRL?tab=readme-ov-file#benchmarks
# [0] = reward to expect for DQN rainbow [1] = timesteps to run (always 200M for DQN
# rainbow).
# Note that for PPO, we simply run everything for 6M ts.
benchmark_envs = {
    "ale_py:ALE/Alien-v5": (6022.9, 200000000),
    "ale_py:ALE/Amidar-v5": (202.8, 200000000),
    "ale_py:ALE/Assault-v5": (14491.7, 200000000),
    "ale_py:ALE/Asterix-v5": (280114.0, 200000000),
    "ale_py:ALE/Asteroids-v5": (2249.4, 200000000),
    "ale_py:ALE/Atlantis-v5": (814684.0, 200000000),
    "ale_py:ALE/BankHeist-v5": (826.0, 200000000),
    "ale_py:ALE/BattleZone-v5": (52040.0, 200000000),
    "ale_py:ALE/BeamRider-v5": (21768.5, 200000000),
    "ale_py:ALE/Berzerk-v5": (1793.4, 200000000),
    "ale_py:ALE/Bowling-v5": (39.4, 200000000),
    "ale_py:ALE/Boxing-v5": (54.9, 200000000),
    "ale_py:ALE/Breakout-v5": (379.5, 200000000),
    "ale_py:ALE/Centipede-v5": (7160.9, 200000000),
    "ale_py:ALE/ChopperCommand-v5": (10916.0, 200000000),
    "ale_py:ALE/CrazyClimber-v5": (143962.0, 200000000),
    "ale_py:ALE/Defender-v5": (47671.3, 200000000),
    "ale_py:ALE/DemonAttack-v5": (109670.7, 200000000),
    "ale_py:ALE/DoubleDunk-v5": (-0.6, 200000000),
    "ale_py:ALE/Enduro-v5": (2061.1, 200000000),
    "ale_py:ALE/FishingDerby-v5": (22.6, 200000000),
    "ale_py:ALE/Freeway-v5": (29.1, 200000000),
    "ale_py:ALE/Frostbite-v5": (4141.1, 200000000),
    "ale_py:ALE/Gopher-v5": (72595.7, 200000000),
    "ale_py:ALE/Gravitar-v5": (567.5, 200000000),
    "ale_py:ALE/Hero-v5": (50496.8, 200000000),
    "ale_py:ALE/IceHockey-v5": (-11685.8, 200000000),
    "ale_py:ALE/Kangaroo-v5": (10841.0, 200000000),
    "ale_py:ALE/Krull-v5": (6715.5, 200000000),
    "ale_py:ALE/KungFuMaster-v5": (28999.8, 200000000),
    "ale_py:ALE/MontezumaRevenge-v5": (154.0, 200000000),
    "ale_py:ALE/MsPacman-v5": (2570.2, 200000000),
    "ale_py:ALE/NameThisGame-v5": (11686.5, 200000000),
    "ale_py:ALE/Phoenix-v5": (103061.6, 200000000),
    "ale_py:ALE/Pitfall-v5": (-37.6, 200000000),
    "ale_py:ALE/Pong-v5": (19.0, 200000000),
    "ale_py:ALE/PrivateEye-v5": (1704.4, 200000000),
    "ale_py:ALE/Qbert-v5": (18397.6, 200000000),
    "ale_py:ALE/RoadRunner-v5": (54261.0, 200000000),
    "ale_py:ALE/Robotank-v5": (55.2, 200000000),
    "ale_py:ALE/Seaquest-v5": (19176.0, 200000000),
    "ale_py:ALE/Skiing-v5": (-11685.8, 200000000),
    "ale_py:ALE/Solaris-v5": (2860.7, 200000000),
    "ale_py:ALE/SpaceInvaders-v5": (12629.0, 200000000),
    "ale_py:ALE/StarGunner-v5": (123853.0, 200000000),
    "ale_py:ALE/Surround-v5": (7.0, 200000000),
    "ale_py:ALE/Tennis-v5": (-2.2, 200000000),
    "ale_py:ALE/TimePilot-v5": (11190.5, 200000000),
    "ale_py:ALE/Tutankham-v5": (126.9, 200000000),
    "ale_py:ALE/Venture-v5": (45.0, 200000000),
    "ale_py:ALE/VideoPinball-v5": (506817.2, 200000000),
    "ale_py:ALE/WizardOfWor-v5": (14631.5, 200000000),
    "ale_py:ALE/YarsRevenge-v5": (93007.9, 200000000),
    "ale_py:ALE/Zaxxon-v5": (19658.0, 200000000),
}


if __name__ == "__main__":
    args = parser.parse_args()

    # Compile the base command running the actual `tuned_example` script.
    base_commands = [
        "python",
        "../../tuned_examples/ppo/atari_ppo.py",
        f"--num-env-runners={args.num_env_runners}" if args.num_env_runners else "",
        f"--num-learners={args.num_learners}",
        f"--num-gpus-per-learner={args.num_gpus_per_learner}",
        f"--wandb-key={args.wandb_key}" if args.wandb_key else "",
        f"--wandb-project={args.wandb_project}" if args.wandb_project else "",
        f"--wandb-run-name={args.wandb_run_name}" if args.wandb_run_name else "",
        f"--stop-timesteps={args.stop_timesteps}",
        f"--checkpoint-freq={args.checkpoint_freq}",
        "--checkpoint-at-end" if args.checkpoint_at_end else "",
    ]

    # Loop through all envs (given on command line or found in `benchmark_envs` and
    # run the `tuned_example` script for each of them.
    for env_name in args.env.split(",") if args.env else benchmark_envs.keys():
        # Remove missing commands.
        commands = []
        for c in base_commands:
            if c != "":
                commands.append(c)
        commands.append(f"--env={env_name}")
        commands.append(f"--wandb-run-name={env_name}")
        print(f"Running {env_name} through command line=`{commands}`")
        subprocess.run(commands)
