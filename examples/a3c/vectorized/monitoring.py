from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import weakref

from gym import monitoring

class Monitor(object):
    def __init__(self, env_n):
        """env_n is a collection of unvectorized envs"""
        self.monitor_n = [monitoring.Monitor(env) for env in env_n]

    @property
    def env(self):
        # The real env is the first unwrapped env. Maybe we should
        # maintain our own weakref rather than doing this.
        return self.monitor_n[0].env.env

    def start(self, directory, video_callable=None, seed_n=None, force=False,
              resume=False, write_upon_reset=False, uid=None):
        if seed_n is None:
            seed_n = [None] * len(self.monitor_n)
        # There's way to seed just one of the vectorized environments,
        # so we have to do the seeding ourselves outside of the
        # underlying monitor instances.
        #
        # The monitor will call the .seed method on the
        # WeakUnvectorized env, which just returns rather than
        # actually re-seeding the env.
        self.env.seed(seed_n)

        for i, monitor in enumerate(self.monitor_n):
            # Only allow recording of video in first monitor
            if i > 0:
                video_callable = False
            # Seed gets passed in but just recorded, not used.
            monitor.start(directory=directory, video_callable=video_callable,
                          force=force, resume=resume, write_upon_reset=write_upon_reset, uid=uid)

    def close(self, *args, **kwargs):
        [monitor.close(*args, **kwargs) for monitor in self.monitor_n]

    def _before_reset(self):
        return [monitor._before_reset() for monitor in self.monitor_n]

    def _after_reset(self, observation_n):
        assert len(observation_n) == len(self.monitor_n)
        return [monitor._after_reset(observation) for monitor, observation in zip(self.monitor_n, observation_n)]

    def _before_step(self, action_n):
        assert len(action_n) == len(self.monitor_n)
        return [monitor._before_step(action) for monitor, action in zip(self.monitor_n, action_n)]

    def _after_step(self, observation_n, reward_n, done_n, info):
        return [monitor._after_step(o, r, d, i) for monitor, o, r, d, i in zip(self.monitor_n, observation_n, reward_n, done_n, info['n'])]
