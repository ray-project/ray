import time
from gym import Wrapper, logger

class TimeLimit(Wrapper):
    def __init__(self, env, max_episode_seconds=None, max_episode_steps=None):
        super(TimeLimit, self).__init__(env)
        self._max_episode_seconds = max_episode_seconds
        self._max_episode_steps = max_episode_steps

        self._elapsed_steps = 0
        self._episode_started_at = None

    @property
    def _elapsed_seconds(self):
        return time.time() - self._episode_started_at

    def _past_limit(self):
        """Return true if we are past our limit"""
        if self._max_episode_steps is not None and self._max_episode_steps <= self._elapsed_steps:
            logger.debug("Env has passed the step limit defined by TimeLimit.")
            return True

        if self._max_episode_seconds is not None and self._max_episode_seconds <= self._elapsed_seconds:
            logger.debug("Env has passed the seconds limit defined by TimeLimit.")
            return True

        return False

    def step(self, action):
        assert self._episode_started_at is not None, "Cannot call env.step() before calling reset()"
        observation, reward, done, info = self.env.step(action)
        self._elapsed_steps += 1

        if self._past_limit():
            if self.metadata.get('semantics.autoreset'):
                _ = self.reset() # automatically reset the env
            done = True 

        return observation, reward, done, info

    def reset(self, **kwargs):
        self._episode_started_at = time.time()
        self._elapsed_steps = 0
        return self.env.reset(**kwargs)
