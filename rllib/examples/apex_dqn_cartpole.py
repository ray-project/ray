"""
Example script on how to train, save, load, and test an RLlib agent.
Equivalent script with stable baselines: sb2rllib_sb_example.py.
Demonstrates transition from stable_baselines to Ray RLlib.

Run example: python sb2rllib_rllib_example.py
"""
import ray
import ray.rllib.agents.ppo as ppo

# settings used for both stable baselines and rllib
env_name = "CartPole-v1"
train_steps = 250000
episode_reward_mean = 150.0
learning_rate = 1e-3

ray.init()

# training and saving
analysis = ray.tune.run(
    "APEX",
    verbose=2,
    stop={"timesteps_total": train_steps, "episode_reward_mean": episode_reward_mean},
    config={"env": env_name,
            "lr": learning_rate,
            "min_time_s_per_reporting": 5,
            "target_network_update_freq": 500,
            "learning_starts": 1000,
            "timesteps_per_iteration": 1000,
            "buffer_size": 20000,
            "optimizer": {"num_replay_buffer_shards": 2},
            "framework": "tf",
            "num_gpus": 0,
            "num_workers": 2,
            "training_intensity": 16,
            "_disable_execution_plan_api": True,
            },

    )