# __quick_start_begin__
import gym
from ray.rllib.agents.ppo import PPOTrainer


# Define your problem using python and openAI's gym API:
class SimpleCorridor(gym.Env):
    def __init__(self, config):
        self.end_pos = config["corridor_length"]
        self.cur_pos = 0
        self.action_space = gym.spaces.Discrete(2)
        self.observation_space = gym.spaces.Box(0.0, self.end_pos, shape=(1,))

    def reset(self):
        self.cur_pos = 0
        return [self.cur_pos]

    def step(self, action):
        if action == 0 and self.cur_pos > 0:
            self.cur_pos -= 1
        elif action == 1:
            self.cur_pos += 1
        # Set `done` flag when end of corridor (goal) reached.
        done = self.cur_pos >= self.end_pos
        # +1 when goal reached, otherwise -1.
        reward = 1.0 if done else -0.1
        return [self.cur_pos], reward, done, {}


# Create an RLlib Trainer instance.
trainer = PPOTrainer(env=SimpleCorridor, config={
    "env": SimpleCorridor,
    "env_config": {"corridor_length": 20},  # customize the environment
    "num_workers": 3,  # parallelize environment rollouts
})

# Train for n iterations.
for i in range(5):
    results = trainer.train()
    print(f"Iter: {i}; avg. reward={results['episode_reward_mean']}")
# __quick_start_end__