import gym
import numpy as np

from ray.rllib.env.vector_env import VectorEnv
from ray.rllib.utils.annotations import override


class MockEnv(gym.Env):
    """Mock environment for testing purposes.

    Observation=0, reward=1.0, episode-len is configurable.
    Actions are ignored.
    """

    def __init__(self, episode_length, config=None):
        self.episode_length = episode_length
        self.config = config
        self.i = 0
        self.observation_space = gym.spaces.Discrete(1)
        self.action_space = gym.spaces.Discrete(2)

    def reset(self):
        self.i = 0
        return 0

    def step(self, action):
        self.i += 1
        return 0, 1.0, self.i >= self.episode_length, {}


class MockEnv2(gym.Env):
    """Mock environment for testing purposes.

    Observation=ts (discrete space!), reward=100.0, episode-len is
    configurable. Actions are ignored.
    """

    metadata = {
        "render.modes": ["rgb_array"],
    }

    def __init__(self, episode_length):
        self.episode_length = episode_length
        self.i = 0
        self.observation_space = gym.spaces.Discrete(self.episode_length + 1)
        self.action_space = gym.spaces.Discrete(2)
        self.rng_seed = None

    def reset(self):
        self.i = 0
        return self.i

    def step(self, action):
        self.i += 1
        return self.i, 100.0, self.i >= self.episode_length, {}

    def seed(self, rng_seed):
        self.rng_seed = rng_seed

    def render(self, mode="rgb_array"):
        # Just generate a random image here for demonstration purposes.
        # Also see `gym/envs/classic_control/cartpole.py` for
        # an example on how to use a Viewer object.
        return np.random.randint(0, 256, size=(300, 400, 3), dtype=np.uint8)


class MockEnv3(gym.Env):
    """Mock environment for testing purposes.

    Observation=ts (discrete space!), reward=100.0, episode-len is
    configurable. Actions are ignored.
    """

    def __init__(self, episode_length):
        self.episode_length = episode_length
        self.i = 0
        self.observation_space = gym.spaces.Discrete(100)
        self.action_space = gym.spaces.Discrete(2)

    def reset(self):
        self.i = 0
        return self.i

    def step(self, action):
        self.i += 1
        return self.i, self.i, self.i >= self.episode_length, {"timestep": self.i}


class VectorizedMockEnv(VectorEnv):
    """Vectorized version of the MockEnv.

    Contains `num_envs` MockEnv instances, each one having its own
    `episode_length` horizon.
    """

    def __init__(self, episode_length, num_envs):
        super().__init__(
            observation_space=gym.spaces.Discrete(1),
            action_space=gym.spaces.Discrete(2),
            num_envs=num_envs,
        )
        self.envs = [MockEnv(episode_length) for _ in range(num_envs)]

    @override(VectorEnv)
    def vector_reset(self):
        return [e.reset() for e in self.envs]

    @override(VectorEnv)
    def reset_at(self, index):
        return self.envs[index].reset()

    @override(VectorEnv)
    def vector_step(self, actions):
        obs_batch, rew_batch, done_batch, info_batch = [], [], [], []
        for i in range(len(self.envs)):
            obs, rew, done, info = self.envs[i].step(actions[i])
            obs_batch.append(obs)
            rew_batch.append(rew)
            done_batch.append(done)
            info_batch.append(info)
        return obs_batch, rew_batch, done_batch, info_batch

    @override(VectorEnv)
    def get_sub_environments(self):
        return self.envs


class MockVectorEnv(VectorEnv):
    """A custom vector env that uses a single(!) CartPole sub-env.

    However, this env pretends to be a vectorized one to illustrate how one
    could create custom VectorEnvs w/o the need for actual vectorizations of
    sub-envs under the hood.
    """

    def __init__(self, episode_length, mocked_num_envs):
        self.env = gym.make("CartPole-v0")
        super().__init__(
            observation_space=self.env.observation_space,
            action_space=self.env.action_space,
            num_envs=mocked_num_envs,
        )
        self.episode_len = episode_length
        self.ts = 0

    @override(VectorEnv)
    def vector_reset(self):
        obs = self.env.reset()
        return [obs for _ in range(self.num_envs)]

    @override(VectorEnv)
    def reset_at(self, index):
        self.ts = 0
        return self.env.reset()

    @override(VectorEnv)
    def vector_step(self, actions):
        self.ts += 1
        # Apply all actions sequentially to the same env.
        # Whether this would make a lot of sense is debatable.
        obs_batch, rew_batch, done_batch, info_batch = [], [], [], []
        for i in range(self.num_envs):
            obs, rew, done, info = self.env.step(actions[i])
            # Artificially terminate once time step limit has been reached.
            # Note: Also terminate, when underlying CartPole is terminated.
            if self.ts >= self.episode_len:
                done = True
            obs_batch.append(obs)
            rew_batch.append(rew)
            done_batch.append(done)
            info_batch.append(info)
            if done:
                remaining = self.num_envs - (i + 1)
                obs_batch.extend([obs for _ in range(remaining)])
                rew_batch.extend([rew for _ in range(remaining)])
                done_batch.extend([done for _ in range(remaining)])
                info_batch.extend([info for _ in range(remaining)])
                break
        return obs_batch, rew_batch, done_batch, info_batch

    @override(VectorEnv)
    def get_sub_environments(self):
        # You may also leave this method as-is, in which case, it would
        # return an empty list.
        return [self.env for _ in range(self.num_envs)]
