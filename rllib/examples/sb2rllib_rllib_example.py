"""
Example script on how to train, save, load, and test an RLlib agent.
Equivalent script with stable baselines: sb2rllib_sb_example.py.
Demonstrates transition from stable_baselines to Ray RLlib.

Run example: python sb2rllib_rllib_example.py
"""
import gym
import ray
import ray.rllib.agents.ppo as ppo

# settings used for both stable baselines and rllib
env_name = "CartPole-v1"
train_steps = 10000
learning_rate = 1e-3
save_dir = "saved_models"

# training and saving
analysis = ray.tune.run(
    "PPO",
    stop={"timesteps_total": train_steps},
    config={"env": env_name, "lr": learning_rate},
    checkpoint_at_end=True,
    local_dir=save_dir,
)
# retrieve the checkpoint path
analysis.default_metric = "episode_reward_mean"
analysis.default_mode = "max"
checkpoint_path = analysis.get_best_checkpoint(trial=analysis.get_best_trial())
print(f"Trained model saved at {checkpoint_path}")

# load and restore model
agent = ppo.PPOTrainer(env=env_name)
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
