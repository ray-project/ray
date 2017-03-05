import vectorized.vectorize_core as core

class Filter(object):
    def _after_reset(self, observation):
        return observation

    def _after_step(self, observation, reward, done, info):
        return observation, reward, done, info

class VectorizeFilter(core.Wrapper):
    """Vectorizes a Filter written for the non-vectorized case."""

    autovectorize = False

    def __init__(self, env, filter_factory, *args, **kwargs):
        super(VectorizeFilter, self).__init__(env)
        self.filter_factory = filter_factory
        self._args = args
        self._kwargs = kwargs

    def _configure(self, **kwargs):
        super(VectorizeFilter, self)._configure(**kwargs)
        self.filter_n = [self.filter_factory(*self._args, **self._kwargs) for _ in range(self.n)]

    def _reset(self):
        observation_n = self.env.reset()
        observation_n = [filter._after_reset(observation) for filter, observation in zip(self.filter_n, observation_n)]
        return observation_n

    def _step(self, action_n):
        o_n, r_n, d_n, i = self.env.step(action_n)

        observation_n = []
        reward_n = []
        done_n = []
        info = i.copy()
        info['n'] = []
        for filter, observation, reward, done, info_i in zip(self.filter_n, o_n, r_n, d_n, i['n']):
            observation, reward, done, info_i = filter._after_step(observation, reward, done, info_i)
            observation_n.append(observation)
            reward_n.append(reward)
            done_n.append(done)
            info['n'].append(info_i)
        return observation_n, reward_n, done_n, info

    def __str__(self):
        return '<{}[{}]{}>'.format(type(self).__name__, self.filter_factory, self.env)
