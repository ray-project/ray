import tensorflow as tf
tf1 = tf.compat.v1
import numpy as np
from scipy.stats import sem

from recsim.agents.slate_decomp_q_agent import create_agent
from recsim.environments.interest_evolution import create_environment

env = create_environment(env_config={
    "slate_size": 2,
    "num_candidates": 50,
    "resample_documents": True,
    "seed": 0,
})

# Run n episodes for random baseline.
num_episodes = 0
rewards = []
episode_lengths = []
steps = 0
reward = 0.0
while num_episodes < 200:
    env.reset()
    done = False
    while not done:
        _, r, done, _ = env.step(action=env.action_space.sample())
        steps += 1
        reward += r
    rewards.append(reward)
    episode_lengths.append(steps)
    reward = 0.0
    steps = 0
    num_episodes += 1
print(f"Random baseline = {np.mean(rewards)} +/- {sem(rewards)}")
print(f"Average episode lengths: {np.mean(episode_lengths)} +/- {sem(episode_lengths)}")


with tf1.Session() as sess:
    agent = create_agent(
        agent_name="slate_optimal_optimal_q",
        sess=sess,
        observation_space=env.observation_space,
        action_space=env.action_space,
        epsilon_decay_period=50000,
        target_update_period=800,
    )
    sess.run(tf1.global_variables_initializer())

    num_episodes = 0
    num_timesteps = 0
    episode_rewards = []
    episode_lengths = []

    while num_episodes < 100000:
        obs = env.reset()
        action = agent.begin_episode(observation=obs)
        done = False
        episode_reward = 0.0
        num_episode_steps = 0

        while not done:
            obs, reward, done, _ = env.step(action=action)
            num_timesteps += 1
            num_episode_steps += 1
            episode_reward += reward
            if not done:
                action = agent.step(reward=reward, observation=obs)

        num_episodes += 1
        episode_lengths.append(num_episode_steps)
        episode_rewards.append(episode_reward)
        agent.end_episode(reward=reward, observation=obs)

        print(f"Avg. reward last 100 episodes (of {num_episodes}; {num_timesteps} ts; episode len={np.mean(episode_lengths[-500:])}): {np.mean(episode_rewards[-500:])}")
