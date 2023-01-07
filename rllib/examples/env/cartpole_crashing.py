import logging
from gymnasium.envs.classic_control import CartPoleEnv
import numpy as np
import time

from ray.rllib.examples.env.multi_agent import make_multi_agent
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
        self.p_crash_reset = config.get("p_crash_reset", 0.0)
        self.crash_after_n_steps = config.get("crash_after_n_steps")
        # Only crash (with prob=p_crash) if on certain worker indices.
        faulty_indices = config.get("crash_on_worker_indices", None)
        if faulty_indices and config.worker_index not in faulty_indices:
            self.p_crash = 0.0
            self.p_crash_reset = 0.0
            self.crash_after_n_steps = None
        # Timestep counter for the ongoing episode.
        self.timesteps = 0

        # Time in seconds to initialize (in this c'tor).
        if "init_time_s" in config:
            init_time_s = config.get("init_time_s", 0)
        else:
            init_time_s = np.random.randint(
                config.get("init_time_s_min", 0),
                config.get("init_time_s_max", 1),
            )
        print(f"Initializing crashing env with init-delay of {init_time_s}sec ...")
        time.sleep(init_time_s)

        # No env pre-checking?
        self._skip_env_checking = config.get("skip_env_checking", False)

        # Make sure envs don't crash at the same time.
        self._rng = np.random.RandomState()

    @override(CartPoleEnv)
    def reset(self, *, seed=None, options=None):
        # Reset timestep counter for the new episode.
        self.timesteps = 0
        # Should we crash?
        if self._rng.rand() < self.p_crash_reset or (
            self.crash_after_n_steps is not None and self.crash_after_n_steps == 0
        ):
            raise EnvError(
                "Simulated env crash in `reset()`! Feel free to use any "
                "other exception type here instead."
            )
        return super().reset()

    @override(CartPoleEnv)
    def step(self, action):
        # Increase timestep counter for the ongoing episode.
        self.timesteps += 1
        # Should we crash?
        if self._rng.rand() < self.p_crash or (
            self.crash_after_n_steps and self.crash_after_n_steps == self.timesteps
        ):
            raise EnvError(
                "Simulated env crash in `step()`! Feel free to use any "
                "other exception type here instead."
            )
        # No crash.
        return super().step(action)


MultiAgentCartPoleCrashing = make_multi_agent(lambda config: CartPoleCrashing(config))
