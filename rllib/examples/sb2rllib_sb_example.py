"""
Example script on how to train, save, load, and test a stable baselines 2 agent
Code taken and adjusted from SB2 docs:
https://stable-baselines.readthedocs.io/en/master/guide/quickstart.html
Equivalent script with RLlib: sb2rllib_rllib_example.py
"""
import gymnasium as gym

from stable_baselines.common.policies import MlpPolicy
from stable_baselines import PPO2

# settings used for both stable baselines and rllib
env_name = "CartPole-v1"
train_steps = 10000
learning_rate = 1e-3
save_dir = "saved_models"

save_path = f"{save_dir}/sb_model_{train_steps}steps"
env = gym.make(env_name)

# training and saving
model = PPO2(MlpPolicy, env, learning_rate=learning_rate, verbose=1)
model.learn(total_timesteps=train_steps)
model.save(save_path)
print(f"Trained model saved at {save_path}")

# delete and load model (just for illustration)
del model
model = PPO2.load(save_path)
print(f"Agent loaded from saved model at {save_path}")

# inference
obs, info = env.reset()
for i in range(1000):
    action, _states = model.predict(obs)
    obs, reward, terminated, truncated, info = env.step(action)
    env.render()
    if terminated or truncated:
        print(f"Cart pole ended after {i} steps.")
        break
