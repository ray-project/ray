import logging
import multiprocessing
import numpy as np
import traceback

import gym
from gym import spaces
import core

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

class Error(Exception):
    pass

def display_name(exception):
    prefix = ''
    # AttributeError has no __module__; RuntimeError has module of
    # exceptions
    if hasattr(exception, '__module__') and exception.__module__ != 'exceptions':
        prefix = exception.__module__ + '.'
    return prefix + type(exception).__name__

def render_dict(error):
    return {
        'type': display_name(error),
        'message': error.message,
        'traceback': traceback.format_exc(error)
    }

class Worker(object):
    def __init__(self, env_m, worker_idx):
        # These are instantiated in the *parent* process
        # currently. Probably will want to change this. The parent
        # does need to obtain the relevant Spaces at some stage, but
        # that's doable.
        self.worker_idx = worker_idx
        self.env_m = env_m
        self.m = len(env_m)
        self.parent_conn, self.child_conn = multiprocessing.Pipe()
        self.joiner = multiprocessing.Process(target=self.run)
        self._clear_state()

        self.start()

        # Parent only!
        self.child_conn.close()

    def _clear_state(self):
        self.mask = [True] * self.m

    # Control methods

    def start(self):
        self.joiner.start()

    def _parent_recv(self):
        rendered, res = self.parent_conn.recv()
        if rendered is not None:
            raise Error('[Worker {}] Error: {} ({})\n\n{}'.format(self.worker_idx, rendered['message'], rendered['type'], rendered['traceback']))
        return res

    def _child_send(self, msg):
        self.child_conn.send((None, msg))

    def _parent_send(self, msg):
        try:
            self.parent_conn.send(msg)
        except IOError: # the worker is now dead
            try:
                res = self._parent_recv()
            except EOFError:
                raise Error('[Worker {}] Child died unexpectedly'.format(self.worker_idx))
            else:
                raise Error('[Worker {}] Child returned unexpected result: {}'.format(self.worker_idx, res))

    def close_start(self):
        self._parent_send(('close', None))

    def close_finish(self):
        self.joiner.join()

    def reset_start(self):
        self._parent_send(('reset', None))

    def reset_finish(self):
        return self._parent_recv()

    def step_start(self, action_m):
        """action_m: the batch of actions for this worker"""
        self._parent_send(('step', action_m))

    def step_finish(self):
        return self._parent_recv()

    def mask_start(self, i):
        self._parent_send(('mask', i))

    def seed_start(self, seed_m):
        self._parent_send(('seed', seed_m))

    def render_start(self, mode, close):
        self._parent_send(('render', (mode, close)))

    def render_finish(self):
        return self._parent_recv()

    def run(self):
        try:
            self.do_run()
        except Exception as e:
            rendered = render_dict(e)
            self.child_conn.send((rendered, None))
            return

    def do_run(self):
        # Child only!
        self.parent_conn.close()

        while True:
            method, body = self.child_conn.recv()
            logger.debug('[%d] Received: method=%s body=%s', self.worker_idx, method, body)
            if method == 'close':
                logger.info('Closing envs')
                # TODO: close envs?
                return
            elif method == 'reset':
                self._clear_state()
                observation_m = [env.reset() for env in self.env_m]
                self._child_send(observation_m)
            elif method == 'step':
                action_m = body
                observation_m, reward_m, done_m, info = self.step_m(action_m)
                self._child_send((observation_m, reward_m, done_m, info))
            elif method == 'mask':
                i = body
                assert 0 <= i < self.m, 'Bad value for mask: {} (should be >= 0 and < {})'.format(i, self.m)

                self.mask[i] = False
                logger.debug('[%d] Applying mask: i=%d', self.worker_idx, i)
            elif method == 'seed':
                seeds = body
                [env.seed(seed) for env, seed in zip(self.env_m, seeds)]
            elif method == 'render':
                mode, close = body
                if mode == 'human':
                    self.env_m[0].render(mode=mode, close=close)
                    result = [None]
                else:
                    result = [env.render(mode=mode, close=close) for env in self.env_m]
                self._child_send(result)
            else:
                raise Error('Bad method: {}'.format(method))

    def step_m(self, action_m):
        observation_m = []
        reward_m = []
        done_m = []
        info = {'m': []}

        for env, enabled, action in zip(self.env_m, self.mask, action_m):
            if enabled:
                observation, reward, done, info_i = env.step(action)
                if done:
                    observation = env.reset()
            else:
                observation = None
                reward = 0
                done = False
                info_i = {}
            observation_m.append(observation)
            reward_m.append(reward)
            done_m.append(done)
            info['m'].append(info_i)
        return observation_m, reward_m, done_m, info


def step_n(worker_n, action_n):
    accumulated = 0
    for worker in worker_n:
        action_m = action_n[accumulated:accumulated+worker.m]
        worker.step_start(action_m)
        accumulated += worker.m

    observation_n = []
    reward_n = []
    done_n = []
    info = {'n': []}

    for worker in worker_n:
        observation_m, reward_m, done_m, info_i = worker.step_finish()
        observation_n += observation_m
        reward_n += reward_m
        done_n += done_m
        info['n'] += info_i['m']
    return observation_n, reward_n, done_n, info


def reset_n(worker_n):
    for worker in worker_n:
        worker.reset_start()

    observation_n = []
    for worker in worker_n:
        observation_n += worker.reset_finish()

    return observation_n


def seed_n(worker_n, seed_n):
    accumulated = 0
    for worker in worker_n:
        action_m = seed_n[accumulated:accumulated+worker.m]
        worker.seed_start(seed_n)
        accumulated += worker.m


def mask(worker_n, i):
    accumulated = 0
    for k, worker in enumerate(worker_n):
        if accumulated + worker.m <= i:
            accumulated += worker.m
        else:
            worker.mask_start(i - accumulated)
            return

def render_n(worker_n, mode, close):
    if mode == 'human':
        # Only render 1 worker
        worker_n = worker_n[0:]

    for worker in worker_n:
        worker.render_start(mode, close)
    res = []
    for worker in worker_n:
        res += worker.render_finish()
    if mode != 'human':
        return res
    else:
        return None

def close_n(worker_n):
    if worker_n is None:
        return

    # TODO: better error handling: workers should die when we go away
    # anyway. Also technically should wait for these processes if
    # we're not crashing.
    for worker in worker_n:
        try:
            worker.close_start()
        except Error:
            pass

    # for worker in worker_n:
    #     try:
    #         worker.close_finish()
    #     except Error:
    #         pass

class MultiprocessingEnv(core.Env):
    metadata = {
        'runtime.vectorized': True,
    }

    def __init__(self, env_id):
        self.worker_n = None

        # Pull the relevant info from a transient env instance
        self.spec = gym.spec(env_id)
        env = self.spec.make()

        current_metadata = self.metadata
        self.metadata = env.metadata.copy()
        self.metadata.update(current_metadata)

        self.action_space = env.action_space
        self.observation_space = env.observation_space
        self.reward_range = env.reward_range

    def _configure(self, n=1, pool_size=None, episode_limit=None):
        super(MultiprocessingEnv, self)._configure()
        self.n = n
        self.envs = [self.spec.make() for _ in range(self.n)]

        if pool_size is None:
            pool_size = min(len(self.envs), multiprocessing.cpu_count() - 1)
            pool_size = max(1, pool_size)

        self.worker_n = []
        m = int((self.n + pool_size - 1) / pool_size)
        for i in range(0, self.n, m):
            envs = self.envs[i:i+m]
            self.worker_n.append(Worker(envs, i))

        if episode_limit is not None:
            self._episode_id.episode_limit = episode_limit

    def _seed(self, seed):
        seed_n(self.worker_n, seed)
        return [[seed_i] for seed_i in seed]

    def _reset(self):
        return reset_n(self.worker_n)

    def _step(self, action_n):
        return step_n(self.worker_n, action_n)

    def _render(self, mode='human', close=False):
        return render_n(self.worker_n, mode=mode, close=close)

    def mask(self, i):
        mask(self.worker_n, i)

    def _close(self):
        close_n(self.worker_n)

if __name__ == '__main__':
    env_n = make('Pong-v3')
    env_n.configure()
    env_n.reset()
    print(env_n.step([0] * 10))
