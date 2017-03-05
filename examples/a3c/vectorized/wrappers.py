import gym
import weakref

import vectorized.vectorize_core as core

class Vectorize(gym.Wrapper):
    """
Given an unvectorized environment (where, e.g., the output of .step() is an observation
rather than a list of observations), turn it into a vectorized environment with a batch of size
1.
"""

    metadata = {'runtime.vectorized': True}

    def __init__(self, env):
        super(Vectorize, self).__init__(env)
        assert not env.metadata.get('runtime.vectorized')
        assert self.metadata.get('runtime.vectorized')
        self.n = 1

    def _reset(self):
        observation = self.env.reset()
        return [observation]

    def _step(self, action):
        observation, reward, done, info = self.env.step(action[0])
        return [observation], [reward], [done], {'n': [info]}

    def _seed(self, seed):
        return [self.env.seed(seed[0])]

class Unvectorize(core.Wrapper):
    """
Take a vectorized environment with a batch of size 1 and turn it into an unvectorized environment.
""" 
    autovectorize = False
    metadata = {'runtime.vectorized': False}

    def _configure(self, **kwargs):
        import ipdb; ipdb.set_trace()
        super(Unvectorize, self)._configure(**kwargs)
        if self.n != 1:
            raise Exception('Can only disable vectorization with n=1, not n={}'.format(self.n))

    def _reset(self):
        observation_n = self.env.reset()
        return observation_n[0]

    def _step(self, action):
        action_n = [action]
        observation_n, reward_n, done_n, info = self.env.step(action_n)
        return observation_n[0], reward_n[0], done_n[0], info['n'][0]

    def _seed(self, seed):
        return self.env.seed([seed])[0]

class WeakUnvectorize(Unvectorize):
    def __init__(self, env):
        self._env_ref = weakref.ref(env)
        super(WeakUnvectorize, self).__init__(env)
        # WeakUnvectorize won't get configure called on it

    @property
    def env(self):
        # Called upon instantiation
        if not hasattr(self, '_env_ref'):
            return

        env = self._env_ref()
        if env is None:
            raise Exception("env has been garbage collected. To keep using WeakUnvectorize, you must keep around a reference to the env object. (HINT: try assigning the env to a variable in your code.)")
        return env

    @env.setter
    def env(self, value):
        # We'll maintain our own weakref, thank you very much.
        pass

    def _seed(self, seed):
        # We handle the seeding ourselves in the vectorized Monitor
        return [seed]

    def close(self):
        # Don't want to close through this wrapper
        pass
