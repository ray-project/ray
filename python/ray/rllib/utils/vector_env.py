from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import queue
import threading


class VectorEnv(object):
    @classmethod
    def wrap(self, make_env=None, existing_envs=[]):
        return _VectorizedGymEnv(make_env, existing_envs)

    def vector_reset(self, vector_width):
        raise NotImplementedError

    def reset_at(self, index):
        raise NotImplementedError

    def vector_step(self, actions):
        raise NotImplementedError

    def first_env(self):
        raise NotImplementedError


class _VectorizedGymEnv(VectorEnv):
    def __init__(self, make_env, existing_envs):
        self.make_env = make_env
        self.envs = existing_envs
        if make_env:
            self.resetter = _AsyncResetter(make_env)
        else:
            self.resetter = _SimpleResetter(make_env)

    def vector_reset(self, vector_width):
        self.resetter.set_pool_size(int(vector_width ** 0.5))
        self.vector_width = vector_width
        while len(self.envs) < vector_width:
            self.envs.append(self.make_env())
        self.envs = self.envs[:self.vector_width]
        return [e.reset() for e in self.envs]

    def reset_at(self, index):
        new_obs, new_env = self.resetter.trade_for_resetted(self.envs[index])
        self.envs[index] = new_env
        return new_obs

    def vector_step(self, actions):
        obs_batch, rew_batch, done_batch, info_batch = [], [], [], []
        for i in range(self.vector_width):
            obs, rew, done, info = self.envs[i].step(actions[i])
            obs_batch.append(obs)
            rew_batch.append(rew)
            done_batch.append(done)
            info_batch.append(info)
        return obs_batch, rew_batch, done_batch, info_batch

    def first_env(self):
        return self.envs[0]


class _AsyncResetter(threading.Thread):
    """Does env reset asynchronously in the background.

    This is useful since resetting an env can be 100x slower than stepping."""

    def __init__(self, make_env):
        threading.Thread.__init__(self)
        self.make_env = make_env
        self.pool_size = 0
        self.to_reset = queue.Queue()
        self.resetted = queue.Queue()
        self.daemon = True
        self.start()

    def set_pool_size(self, size):
        self.pool_size = size
        while self.resetted.qsize() < self.pool_size:
            env = self.make_env()
            obs = env.reset()
            self.resetted.put((obs, env))

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

    def set_pool_size(self, size):
        pass

    def trade_for_resetted(self, env):
        return env.reset(), env
