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
    "ALE/Alien-v5": (6022.9, 200000000),
    "ALE/Amidar-v5": (202.8, 200000000),
    "ALE/Assault-v5": (14491.7, 200000000),
    "ALE/Asterix-v5": (280114.0, 200000000),
    "ALE/Asteroids-v5": (2249.4, 200000000),
    "ALE/Atlantis-v5": (814684.0, 200000000),
    "ALE/BankHeist-v5": (826.0, 200000000),
    "ALE/BattleZone-v5": (52040.0, 200000000),
    "ALE/BeamRider-v5": (21768.5, 200000000),
    "ALE/Berzerk-v5": (1793.4, 200000000),
    "ALE/Bowling-v5": (39.4, 200000000),
    "ALE/Boxing-v5": (54.9, 200000000),
    "ALE/Breakout-v5": (379.5, 200000000),
    "ALE/Centipede-v5": (7160.9, 200000000),
    "ALE/ChopperCommand-v5": (10916.0, 200000000),
    "ALE/CrazyClimber-v5": (143962.0, 200000000),
    "ALE/Defender-v5": (47671.3, 200000000),
    "ALE/DemonAttack-v5": (109670.7, 200000000),
    "ALE/DoubleDunk-v5": (-0.6, 200000000),
    "ALE/Enduro-v5": (2061.1, 200000000),
    "ALE/FishingDerby-v5": (22.6, 200000000),
    "ALE/Freeway-v5": (29.1, 200000000),
    "ALE/Frostbite-v5": (4141.1, 200000000),
    "ALE/Gopher-v5": (72595.7, 200000000),
    "ALE/Gravitar-v5": (567.5, 200000000),
    "ALE/Hero-v5": (50496.8, 200000000),
    "ALE/IceHockey-v5": (-11685.8, 200000000),
    "ALE/Kangaroo-v5": (10841.0, 200000000),
    "ALE/Krull-v5": (6715.5, 200000000),
    "ALE/KungFuMaster-v5": (28999.8, 200000000),
    "ALE/MontezumaRevenge-v5": (154.0, 200000000),
    "ALE/MsPacman-v5": (2570.2, 200000000),
    "ALE/NameThisGame-v5": (11686.5, 200000000),
    "ALE/Phoenix-v5": (103061.6, 200000000),
    "ALE/Pitfall-v5": (-37.6, 200000000),
    "ALE/Pong-v5": (19.0, 200000000),
    "ALE/PrivateEye-v5": (1704.4, 200000000),
    "ALE/Qbert-v5": (18397.6, 200000000),
    "ALE/RoadRunner-v5": (54261.0, 200000000),
    "ALE/Robotank-v5": (55.2, 200000000),
    "ALE/Seaquest-v5": (19176.0, 200000000),
    "ALE/Skiing-v5": (-11685.8, 200000000),
    "ALE/Solaris-v5": (2860.7, 200000000),
    "ALE/SpaceInvaders-v5": (12629.0, 200000000),
    "ALE/StarGunner-v5": (123853.0, 200000000),
    "ALE/Surround-v5": (7.0, 200000000),
    "ALE/Tennis-v5": (-2.2, 200000000),
    "ALE/TimePilot-v5": (11190.5, 200000000),
    "ALE/Tutankham-v5": (126.9, 200000000),
    "ALE/Venture-v5": (45.0, 200000000),
    "ALE/VideoPinball-v5": (506817.2, 200000000),
    "ALE/WizardOfWor-v5": (14631.5, 200000000),
    "ALE/YarsRevenge-v5": (93007.9, 200000000),
    "ALE/Zaxxon-v5": (19658.0, 200000000),
}


if __name__ == "__main__":
    args = parser.parse_args()

    base_commands = [
        "python",
        "../../tuned_examples/ppo/atari_ppo.py",
        "--enable-new-api-stack",
        f"--num-env-runners={args.num_env_runners}",
        f"--num-gpus={args.num_gpus}",
        f"--wandb-key={args.wandb_key}" if args.wandb_key else "",
        f"--wandb-project={args.wandb_project}" if args.wandb_project else "",
        f"--wandb-run-name={args.wandb_run_name}" if args.wandb_run_name else "",
        f"--stop-timesteps={args.stop_timesteps}",
        f"--checkpoint-freq={args.checkpoint_freq}",
        f"--checkpoint-at-end={args.checkpoint_at_end}",
    ]

    envs = args.env.split(",") if args.env else benchmark_envs.keys()

    for env_name in envs:
        setup = benchmark_envs[env_name]
        commands = base_commands.copy()
        commands.extend(["--env", env_name])
        subprocess.run(commands)
