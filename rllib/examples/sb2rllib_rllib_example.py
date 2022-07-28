"""
Example script on how to train, save, load, and test an RLlib agent.
Equivalent script with stable baselines: sb2rllib_sb_example.py.
Demonstrates transition from stable_baselines to Ray RLlib.

Run example: python sb2rllib_rllib_example.py
"""
import gym
from ray import tune, air
import ray.rllib.algorithms.ppo as ppo

# settings used for both stable baselines and rllib
env_name = "CartPole-v1"
train_steps = 10000
learning_rate = 1e-3
save_dir = "saved_models"

# training and saving
analysis = tune.Tuner(
    "PPO",
    run_config=air.RunConfig(
        stop={"timesteps_total": train_steps},
        local_dir=save_dir,
        checkpoint_config=air.CheckpointConfig(
            checkpoint_at_end=True,
        ),
    ),
    param_space={"env": env_name, "lr": learning_rate},
).fit()
# retrieve the checkpoint path
analysis.default_metric = "episode_reward_mean"
analysis.default_mode = "max"
checkpoint_path = analysis.get_best_checkpoint(trial=analysis.get_best_trial())
print(f"Trained model saved at {checkpoint_path}")

# load and restore model
agent = ppo.PPO(env=env_name)
agent.restore(checkpoint_path)
print(f"Agent loaded from saved model at {checkpoint_path}")

# inference
env = gym.make(env_name)
obs = env.reset()
for i in range(1000):
    action = agent.compute_single_action(obs)
    obs, reward, done, info = env.step(action)
    env.render()
    if done:
        print(f"Cart pole dropped after {i} steps.")
        break
