import gym
from ray.tune import register_env

class DeterministicCartPole(gym.Env):

    def __init__(self, seed=0):
        self.env = gym.make("CartPole-v0")
        self.env.seed(seed)
        self.action_space = self.env.action_space
        self.observation_space = self.env.observation_space
    
    def reset(self):
        return self.env.reset()
    
    def step(self, action):
        return self.env.step(action)
    
register_env(
    "DeterministicCartPole-v0", 
    lambda config: DeterministicCartPole(config["seed"])
)