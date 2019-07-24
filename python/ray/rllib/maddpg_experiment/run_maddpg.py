import ray
from ray.tune import run_experiments
from ray.tune.registry import register_trainable, register_env
from env import MultiAgentParticleEnv
import ray.rllib.agents.maddpg.maddpg as maddpg
import argparse

import os

os.environ['TF_CPP_MIN_LOG_LEVEL'] = '3'


def parse_args():
    parser = argparse.ArgumentParser("MADDPG with OpenAI MPE")
    # Slurm task id
    parser.add_argument("--slurm-task-id", type=int, default=None,
                        help="task id of slurm sbatch array")
    parser.add_argument("--run-id", type=int, default=None,
                        help="run id for multiple runs, e.g., seed")
    parser.add_argument("--exp-name", type=str, default="MADDPG_MPE_RLlib",
                        help="name of the experiment")
    parser.add_argument("--exp-id", type=str, default="test",
                        help="experiment id (version)")

    # Environment
    parser.add_argument("--scenario", type=str, default="simple_spread",
                        choices=['simple', 'simple_speaker_listener',
                                 'simple_crypto', 'simple_push',
                                 'simple_tag', 'simple_spread', 'simple_adversary'],
                        help="name of the scenario script")
    parser.add_argument("--max-episode-len", type=int, default=25,
                        help="maximum episode length")
    parser.add_argument("--num-episodes", type=int, default=60000,
                        help="number of episodes")
    parser.add_argument("--num-adversaries", type=int, default=0,
                        help="number of adversaries")
    parser.add_argument("--good-policy", type=str, default="maddpg",
                        help="policy for good agents")
    parser.add_argument("--adv-policy", type=str, default="maddpg",
                        help="policy of adversaries")

    # Core training parameters
    parser.add_argument("--lr", type=float, default=1e-2,
                        help="learning rate for Adam optimizer")
    parser.add_argument("--gamma", type=float, default=0.95,
                        help="discount factor")
    # NOTE: 1 iteration = sample_batch_size * num_workers timesteps
    parser.add_argument("--sample-batch-size", type=int, default=25,
                        help="number of data points sampled /update /worker")
    parser.add_argument("--train-batch-size", type=int, default=1024,
                        help="number of data points /update")
    parser.add_argument("--n-step", type=int, default=1,
                        help="length of multistep value backup")
    parser.add_argument("--num-units", type=int, default=64,
                        help="number of units in the mlp")

    # Checkpoint
    parser.add_argument("--checkpoint-freq", type=int, default=7500,
                        help="save model once every time this many iterations are completed")
    parser.add_argument("--local-dir", type=str, default="./ray_results",
                        help="path to save checkpoints")
    parser.add_argument("--restore", type=str, default=None,
                        help="directory in which training state and model are loaded")

    # Parallelism
    parser.add_argument("--num-workers", type=int, default=1)
    parser.add_argument("--num-envs-per-worker", type=int, default=4)
    parser.add_argument("--num-gpus", type=int, default=0)

    return parser.parse_args()


def main(args):
    ray.init(redis_max_memory=int(1e10), object_store_memory=int(3e9))
    register_trainable("MADDPG", maddpg.MADDPGTrainer)

    def env_creater(mpe_args):
        return MultiAgentParticleEnv(**mpe_args)

    register_env("mpe", env_creater)

    env = env_creater({
        "scenario_name": args.scenario,
    })

    def gen_policy(i):
        use_local_critic = [
            args.adv_policy == "ddpg" if i < args.num_adversaries else
            args.good_policy == "ddpg" for i in range(env.num_agents)
        ]
        return (
            None,
            env.observation_space_dict[i],
            env.action_space_dict[i],
            {
                "agent_id": i,
                "use_local_critic": use_local_critic[i],
            }
        )

    policies = {"policy_%d" %i: gen_policy(i) for i in range(len(env.observation_space_dict))}
    policy_ids = list(policies.keys())

    args.exp_name += "-{}".format(args.exp_id)
    if args.slurm_task_id is not None:
        args.exp_name += "/{}".format(str(args.slurm_task_id))

    run_experiments({
        args.exp_name: {
            "run": "MADDPG",
            "env": "mpe",
            "stop": {
                "episodes_total": args.num_episodes,
            },
            "checkpoint_freq": args.checkpoint_freq,
            "local_dir": args.local_dir,
            "restore": args.restore,
            "config": {
                "good_policy": args.good_policy,
                "adv_policy": args.adv_policy,
                "run_id": args.run_id,

                # === Log ===
                "log_level": "ERROR",

                # === Environment ===
                "env_config": {
                    "scenario_name": args.scenario,
                },
                "num_envs_per_worker": args.num_envs_per_worker,
                "horizon": args.max_episode_len,
                "obs_space_dict": env.observation_space_dict,
                "act_space_dict": env.action_space_dict,

                # === Policy Config ===
                # --- Model ---
                "actor_hiddens": [args.num_units] * 2,
                "actor_hidden_activation": "relu",
                "critic_hiddens": [args.num_units] * 2,
                "critic_hidden_activation": "relu",
                "n_step": args.n_step,
                "gamma": args.gamma,

                # --- Exploration ---
                "tau": 0.01,

                # --- Replay buffer ---
                "buffer_size": int(1e6),

                # --- Optimization ---
                "actor_lr": args.lr,
                "critic_lr": args.lr,
                "learning_starts": args.train_batch_size * args.max_episode_len,
                "sample_batch_size": args.sample_batch_size,
                "train_batch_size": args.train_batch_size,
                "batch_mode": "truncate_episodes",

                # --- Parallelism ---
                "num_workers": args.num_workers,
                "num_gpus": args.num_gpus,
                "num_gpus_per_worker": 0,

                # === Multi-agent setting ===
                "multiagent": {
                    "policies": policies,
                    "policy_mapping_fn": ray.tune.function(
                        lambda i: policy_ids[i]
                    )
                },
            },
        },
    }, verbose=0)


if __name__ == '__main__':
    args = parse_args()
    main(args)
