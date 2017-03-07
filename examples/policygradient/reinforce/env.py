import gym
import numpy as np

def atari_preprocessor(observation):
  "Convert images from (210, 160, 3) to (3, 80, 80) by downsampling."
  return (observation[25:-25:2,::2,:][None] - 128.0) / 128.8

def ram_preprocessor(observation):
  return (observation - 128.0) / 128.0

class BatchedEnv(object):
  "A BatchedEnv holds multiple gym enviroments and performs steps on all of them."

  def __init__(self, name, batchsize, preprocessor=None):
    self.envs = [gym.make(name) for _ in range(batchsize)]
    self.observation_space = self.envs[0].observation_space
    self.action_space = self.envs[0].action_space
    self.batchsize = batchsize
    self.preprocessor = preprocessor if preprocessor else lambda obs: obs[None]

  def reset(self):
    observations = [self.preprocessor(env.reset()) for env in self.envs]
    self.shape = observations[0].shape
    self.dones = [False for _ in range(self.batchsize)]
    return np.vstack(observations)

  def step(self, actions, render=False):
    observations = []
    rewards = []
    for i, action in enumerate(actions):
      if self.dones[i]:
        observations.append(np.zeros(self.shape))
        rewards.append(0.0)
        continue
      observation, reward, done, info = self.envs[i].step(action)
      if render:
        self.envs[0].render()
      observations.append(self.preprocessor(observation))
      rewards.append(reward)
      self.dones[i] = done
    return np.vstack(observations), np.array(rewards, dtype="float32"), np.array(self.dones)
