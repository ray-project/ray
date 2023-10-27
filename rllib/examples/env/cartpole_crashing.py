import logging
from gymnasium.envs.classic_control import CartPoleEnv
import numpy as np
import time

from ray.rllib.examples.env.multi_agent import make_multi_agent
from ray.rllib.utils.annotations import override
from ray.rllib.utils.error import EnvError

logger = logging.getLogger(__name__)


class CartPoleCrashing(CartPoleEnv):
    """A CartPole env that crashes (or hangs) from time to time.

    Useful for testing faulty sub-env (within a vectorized env) handling by
    EnvRunners.

    After crashing, the env expects a `reset()` call next (calling `step()` will
    result in yet another error), which may or may not take a very long time to
    complete. This simulates the env having to reinitialize some sub-processes, e.g.
    an external connection.

    The env can also be configured to hang (and do nothing during a call to `step()`)
    from time to time for a configurable amount of time.
    """

    def __init__(self, config=None):
        super().__init__()

        self.config = config or {}

        # Crash probability (in each `step()`).
        self.p_crash = config.get("p_crash", 0.005)
        # Crash probability when `reset()` is called.
        self.p_crash_reset = config.get("p_crash_reset", 0.0)
        # Crash exactly after every n steps. If a 2-tuple, will uniformly sample
        # crash timesteps from in between the two given values.
        self.crash_after_n_steps = config.get("crash_after_n_steps")
        self._crash_after_n_steps = None
        assert (
            self.crash_after_n_steps is None
            or isinstance(self.crash_after_n_steps, int)
            or (
                isinstance(self.crash_after_n_steps, tuple)
                and len(self.crash_after_n_steps) == 2
            )
        )
        # Only ever crash, if on certain worker indices.
        faulty_indices = config.get("crash_on_worker_indices", None)
        if faulty_indices and config.worker_index not in faulty_indices:
            self.p_crash = 0.0
            self.p_crash_reset = 0.0
            self.crash_after_n_steps = None

        # Hang probability (in each `step()`).
        self.p_hang = config.get("p_hang", 0.0)
        # Hang probability when `reset()` is called.
        self.p_hang_reset = config.get("p_hang_reset", 0.0)
        # Hang exactly after every n steps.
        self.hang_after_n_steps = config.get("hang_after_n_steps")
        self._hang_after_n_steps = None
        # Amount of time to hang. If a 2-tuple, will uniformly sample from in between
        # the two given values.
        self.hang_time_sec = config.get("hang_time_sec")
        assert (
            self.hang_time_sec is None
            or isinstance(self.hang_time_sec, (int, float))
            or (
                isinstance(self.hang_time_sec, tuple)
                and len(self.hang_time_sec) == 2
            )
        )

        # Timestep counter for the ongoing episode.
        self.timesteps = 0

        # Time in seconds to initialize (in this c'tor).
        sample = 0.0
        if "init_time_s" in config:
            sample = (
                config["init_time_s"]
                if not isinstance(config["init_time_s"], tuple)
                else np.random.uniform(
                    config["init_time_s"][0], config["init_time_s"][1]
                )
            )

        print(f"Initializing crashing env (with init-delay of {sample}sec) ...")
        time.sleep(sample)

        # No env pre-checking?
        self._skip_env_checking = config.get("skip_env_checking", False)

        # Make sure envs don't crash at the same time.
        self._rng = np.random.RandomState()

    @override(CartPoleEnv)
    def reset(self, *, seed=None, options=None):
        # Reset timestep counter for the new episode.
        self.timesteps = 0
        self._crash_after_n_steps = None

        # Should we crash?
        if self._should_crash(p=self.p_crash_reset):
            raise EnvError(
                f"Simulated env crash on worker={self.config.worker_index} "
                f"env-idx={self.config.vector_index} during `reset()`! "
                "Feel free to use any other exception type here instead."
            )
        # Should we hang for a while?
        self._hang_if_necessary(p=self.p_hang_reset)

        return super().reset()

    @override(CartPoleEnv)
    def step(self, action):
        # Increase timestep counter for the ongoing episode.
        self.timesteps += 1

        # Should we crash?
        if self._should_crash(p=self.p_crash):
            raise EnvError(
                f"Simulated env crash on worker={self.config.worker_index} "
                f"env-idx={self.config.vector_index} during `step()`! "
                "Feel free to use any other exception type here instead."
            )
        # Should we hang for a while?
        self._hang_if_necessary(p=self.p_hang)

        return super().step(action)

    def _should_crash(self, p):
        print(f"inside _should_crash() p={p}")
        rnd = self._rng.rand()
        if rnd < p:
            print(f" -> return True ({rnd} < {p})")
            return True
        elif self.crash_after_n_steps is not None:
            if self._crash_after_n_steps is None:
                self._crash_after_n_steps = (
                    self.crash_after_n_steps
                    if not isinstance(self.crash_after_n_steps, tuple)
                    else np.random.randint(
                        self.crash_after_n_steps[0], self.crash_after_n_steps[1]
                    )
                )
            if self._crash_after_n_steps == self.timesteps:
                print(f" -> return True (n steps)")
                return True

        print(f" -> return False")
        return False

    def _hang_if_necessary(self, p):
        hang = False
        if self._rng.rand() < p:
            hang = True
        elif self.hang_after_n_steps is not None:
            if self._hang_after_n_steps is None:
                self._hang_after_n_steps = (
                    self.hang_after_n_steps
                    if not isinstance(self.hang_after_n_steps, tuple)
                    else np.random.randint(
                        self.hang_after_n_steps[0], self.hang_after_n_steps[1]
                    )
                )
            if self._hang_after_n_steps == self.timesteps:
                hang = True

        if hang:
            print("hanging ...")
            time.sleep(
                self.hang_time_sec if not isinstance(self.hang_time_sec, tuple)
                else np.random.uniform(self.hang_time_sec[0], self.hang_time_sec[1])
            )


MultiAgentCartPoleCrashing = make_multi_agent(lambda config: CartPoleCrashing(config))
