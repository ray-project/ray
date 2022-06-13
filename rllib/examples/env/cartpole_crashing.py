import logging
from gym.envs.classic_control import CartPoleEnv
import numpy as np
import time

from ray.rllib.utils.annotations import override
from ray.rllib.utils.error import EnvError

logger = logging.getLogger(__name__)


class CartPoleCrashing(CartPoleEnv):
    """A CartPole env that crashes from time to time.

    Useful for testing faulty sub-env (within a vectorized env) handling by
    RolloutWorkers.

    After crashing, the env expects a `reset()` call next (calling `step()` will
    result in yet another error), which may or may not take a very long time to
    complete. This simulates the env having to reinitialize some sub-processes, e.g.
    an external connection.
    """

    def __init__(self, config=None):
        super().__init__()

        config = config or {}

        # Crash probability (in each `step()`).
        self.p_crash = config.get("p_crash", 0.005)
        self.crash_after_n_steps = config.get("crash_after_n_steps")
        # Only crash (with prob=p_crash) if on certain worker indices.
        faulty_indices = config.get("crash_on_worker_indices", None)
        if faulty_indices and config.worker_index not in faulty_indices:
            self.p_crash = 0.0
            self.crash_after_n_steps = None
        # Timestep counter for the ongoing episode.
        self.timesteps = 0

        # Time in seconds to initialize (in this c'tor).
        init_time_s = config.get("init_time_s", 0)
        time.sleep(init_time_s)

        # Time in seconds to re-initialize, while `reset()` is called after a crash.
        self.re_init_time_s = config.get("re_init_time_s", 10)

        # Are we currently in crashed state and expect `reset()` call next?
        self.in_crashed_state = False

        # No env pre-checking?
        self._skip_env_checking = config.get("skip_env_checking", False)

    @override(CartPoleEnv)
    def reset(self):
        # Reset timestep counter for the new episode.
        self.timesteps = 0
        if self.in_crashed_state:
            logger.info(
                f"Env had crashed ... resetting (for {self.re_init_time_s} sec)"
            )
            time.sleep(self.re_init_time_s)
            self.in_crashed_state = False
        return super().reset()

    @override(CartPoleEnv)
    def step(self, action):
        # Increase timestep counter for the ongoing episode.
        self.timesteps += 1
        # Still in crashed state -> Must call `reset()` first.
        if self.in_crashed_state:
            raise EnvError("Env had crashed before! Must call `reset()` first.")
        # Should we crash?
        elif np.random.random() < self.p_crash or (
            self.crash_after_n_steps and self.crash_after_n_steps == self.timesteps
        ):
            self.in_crashed_state = True
            raise EnvError(
                "Simulated env crash! Feel free to use any "
                "other exception type here instead."
            )
        # No crash.
        return super().step(action)
