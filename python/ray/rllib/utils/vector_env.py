from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import queue
import threading

from ray.rllib.utils.async_vector_env import AsyncVectorEnv


class VectorEnv(object):
    """An environment that supports batch evaluation.

    Subclasses must define the following attributes:

    Attributes:
        action_space (gym.Space): Action space of individual envs.
        observation_space (gym.Space): Observation space of individual envs.
        num_envs (int): Number of envs to batch over.
    """

    @staticmethod
    def wrap(make_env=None, existing_envs=None, num_envs=1):
        return _VectorizedGymEnv(make_env, existing_envs or [], num_envs)

    def vector_reset(self):
        raise NotImplementedError

    def reset_at(self, index):
        raise NotImplementedError

    def vector_step(self, actions):
        raise NotImplementedError

    def get_unwrapped(self):
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


# Fixed agent identifier for the single agent in the env
_DUMMY_AGENT_ID = "single_agent"


class _VectorEnvToAsync(AsyncVectorEnv):
    """Wraps VectorEnv to implement AsyncVectorEnv.

    We assume the caller will always send the full vector of actions in each
    call to send_actions(), and that they call reset_at() on all completed
    environments before calling send_actions().
    """

    def __init__(self, vector_env):
        self.vector_env = vector_env
        self.num_envs = vector_env.num_envs
        self.new_obs = self.vector_env.vector_reset()
        self.cur_rewards = [None for _ in range(self.num_envs)]
        self.cur_dones = [False for _ in range(self.num_envs)]
        self.cur_infos = [None for _ in range(self.num_envs)]

    def poll(self):
        new_obs = dict(enumerate(self.new_obs))
        rewards = dict(enumerate(self.cur_rewards))
        dones = dict(enumerate(self.cur_dones))
        infos = dict(enumerate(self.cur_infos))
        self.new_obs = []
        self.cur_rewards = []
        self.cur_dones = []
        self.cur_infos = []
        return _with_dummy_agent_id(new_obs), \
            _with_dummy_agent_id(rewards), \
            _with_dummy_agent_id(dones), \
            _with_dummy_agent_id(infos), {}

    def send_actions(self, action_dict):
        action_vector = [None] * self.num_envs
        for i in range(self.num_envs):
            action_vector[i] = action_dict[i][_DUMMY_AGENT_ID]
        self.new_obs, self.cur_rewards, self.cur_dones, self.cur_infos = \
            self.vector_env.vector_step(action_vector)

    def try_reset(self, env_id):
        return self.vector_env.reset_at(env_id)

    def get_unwrapped(self):
        return self.vector_env.get_unwrapped()


def _with_dummy_agent_id(env_id_to_values):
    return {k: {_DUMMY_AGENT_ID: v} for (k, v) in env_id_to_values.items()}
