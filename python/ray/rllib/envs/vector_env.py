from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import queue
import threading


class VectorEnv(object):
    """An environment that supports batch evaluation.

    Subclasses must define the following attributes:

    Attributes:
        action_space (gym.Space): Action space of individual envs.
        observation_space (gym.Space): Observation space of individual envs.
        num_envs (int): Number of envs in this vector env.
    """

    @staticmethod
    def wrap(make_env=None, existing_envs=None, num_envs=1):
        return _VectorizedGymEnv(make_env, existing_envs or [], num_envs)

    def vector_reset(self):
        """Resets all environments.

        Returns:
            obs (list): Vector of observations from each environment.
        """
        raise NotImplementedError

    def reset_at(self, index):
        """Resets a single environment.

        Returns:
            obs (obj): Observations from the resetted environment.
        """
        raise NotImplementedError

    def vector_step(self, actions):
        """Vectorized step.

        Arguments:
            actions (list): Actions for each env.

        Returns:
            obs (list): New observations for each env.
            rewards (list): Reward values for each env.
            dones (list): Done values for each env.
            infos (list): Info values for each env.
        """
        raise NotImplementedError

    def get_unwrapped(self):
        """Returns a single instance of the underlying env."""
        raise NotImplementedError


class _VectorizedGymEnv(VectorEnv):
    """Internal wrapper for gym envs to implement VectorEnv.

    Arguments:
        make_env (func|None): Factory that produces a new gym env. Must be
            defined if the number of existing envs is less than num_envs.
        existing_envs (list): List of existing gym envs.
        num_envs (int): Desired num gym envs to keep total.
    """

    def __init__(self, make_env, existing_envs, num_envs):
        self.make_env = make_env
        self.envs = existing_envs
        self.num_envs = num_envs
        if make_env and num_envs > 1:
            self.resetter = _AsyncResetter(
                make_env, int(self.num_envs ** 0.5))
        else:
            self.resetter = _SimpleResetter(make_env)
        while len(self.envs) < self.num_envs:
            self.envs.append(self.make_env())

    def vector_reset(self):
        return [e.reset() for e in self.envs]

    def reset_at(self, index):
        new_obs, new_env = self.resetter.trade_for_resetted(self.envs[index])
        self.envs[index] = new_env
        return new_obs

    def vector_step(self, actions):
        obs_batch, rew_batch, done_batch, info_batch = [], [], [], []
        for i in range(self.num_envs):
            obs, rew, done, info = self.envs[i].step(actions[i])
            obs_batch.append(obs)
            rew_batch.append(rew)
            done_batch.append(done)
            info_batch.append(info)
        return obs_batch, rew_batch, done_batch, info_batch

    def get_unwrapped(self):
        return self.envs[0]


class _AsyncResetter(threading.Thread):
    """Does env reset asynchronously in the background.

    This is useful since resetting an env can be 100x slower than stepping."""

    def __init__(self, make_env, pool_size):
        threading.Thread.__init__(self)
        self.make_env = make_env
        self.pool_size = 0
        self.to_reset = queue.Queue()
        self.resetted = queue.Queue()
        self.daemon = True
        self.pool_size = pool_size
        while self.resetted.qsize() < self.pool_size:
            env = self.make_env()
            obs = env.reset()
            self.resetted.put((obs, env))
        self.start()

    def run(self):
        while True:
            env = self.to_reset.get()
            obs = env.reset()
            self.resetted.put((obs, env))

    def trade_for_resetted(self, env):
        self.to_reset.put(env)
        new_obs, new_env = self.resetted.get(timeout=30)
        return new_obs, new_env


class _SimpleResetter(object):
    def __init__(self, make_env):
        pass

    def trade_for_resetted(self, env):
        return env.reset(), env
