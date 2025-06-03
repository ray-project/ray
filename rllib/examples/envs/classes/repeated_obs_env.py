import gymnasium as gym
import numpy as np
from gymnasium.spaces import Discrete, Box, Dict
from ray.rllib.utils.spaces.repeated import Repeated


class RepeatedObsEnv(gym.Env):
    '''
      An environment to test the handling of Repeated observation spaces. Multiple types of entity must be related to each other in varying quantities.

      Each voter has a set of values, which determine their vote by some hidden linear function. Given a list of voters' values and the set of weights that map their values to their voting decision, predict whether a specific (non-voting) citizen will be happy with the outcome of a referendum.
    '''
    def __init__(self, config={}):
        super().__init__()
        np.random.seed(config['random_seed'] if 'random_seed' in config else 0)
        self.max_voter_pairs = config['max_voter_pairs'] if 'max_voter_pairs' in config else 5
        self.num_values = config['num_values'] if 'num_values' in config else 5
        self.observation_space = Dict({
            'voters': Repeated(Box(-1,1,shape=(self.num_values,)), self.max_voter_pairs*2+1),
            'value_mapping': Box(-1,1,shape=(self.num_values,)),
            'citizen': Box(-1,1,shape=(self.num_values,))
        })
        self.action_space = Discrete(2)

    def new_obs(self):
      self.voters = []
      for i in range(self.num_voters):
        self.voters.append(np.random.rand(self.num_values).astype(np.float32) * 2 - 1)
      return {
          'voters': self.voters,
          'value_mapping': self.value_mapping,
          'citizen': self.citizen
      }

    def step(self, action):
        outcome = (np.array(self.voters) @ self.value_mapping.T > 0).sum() > self.max_voter_pairs
        citizen_outcome = (self.citizen @ self.value_mapping.T) > 0
        reward = 1 if ((outcome==citizen_outcome) == (action==1)) else 0
        return self.new_obs(), reward, True, False, {}

    def reset(self, *, seed=None, options=None):
        np.random.seed(seed)
        self.num_voters = np.random.randint(0, self.max_voter_pairs+1) * 2 + 1
        self.value_mapping = (np.random.rand(self.num_values).astype(np.float32) - 0.5) * 2
        self.citizen = (np.random.rand(self.num_values).astype(np.float32) - 0.5) * 2
        return self.new_obs(), {}
