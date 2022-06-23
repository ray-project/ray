import logging
import traceback

import gym

logger = logging.getLogger(__name__)


class TooManyResetAttemptsException(Exception):
    def __init__(self, max_attempts: int):
        super().__init__(
            f"Reached the maximum number of attempts ({max_attempts}) "
            f"to reset an environment."
        )


class ResetOnExceptionWrapper(gym.Wrapper):
    def __init__(self, env: gym.Env, max_reset_attempts: int = 5):
        super().__init__(env)
        self.max_reset_attempts = max_reset_attempts

    def reset(self, **kwargs):
        attempt = 0
        while attempt < self.max_reset_attempts:
            try:
                return self.env.reset(**kwargs)
            except Exception:
                logger.error(traceback.format_exc())
                attempt += 1
        else:
            raise TooManyResetAttemptsException(self.max_reset_attempts)

    def step(self, action):
        try:
            return self.env.step(action)
        except Exception:
            logger.error(traceback.format_exc())
            return self.reset(), 0.0, False, {"__terminated__": True}
