from typing import Optional, Tuple

from ray.rllib.env.multi_agent_env import MultiAgentEnv
from ray.rllib.utils.typing import MultiAgentDict


class MultiAgentEnvCompatibility(MultiAgentEnv):
    """A wrapper converting MultiAgentEnv from old gym API to the new one.

    "Old API" refers to step() method returning (observation, reward, done, info),
    and reset() only retuning the observation.
    "New API" refers to step() method returning (observation, reward, terminated,
    truncated, info) and reset() returning (observation, info).

    Known limitations:
    - Environments that use `self.np_random` might not work as expected.
    """

    def __init__(self, old_env, render_mode: Optional[str] = None):
        """A wrapper which converts old-style envs to valid modern envs.

        Some information may be lost in the conversion, so we recommend updating your
        environment.

        Args:
            old_env: The old MultiAgentEnv to wrap. Implemented with the old API.
            render_mode: The render mode to use when rendering the environment,
                passed automatically to `env.render()`.
        """
        super().__init__()

        self.metadata = getattr(old_env, "metadata", {"render_modes": []})
        self.render_mode = render_mode
        self.reward_range = getattr(old_env, "reward_range", None)
        self.spec = getattr(old_env, "spec", None)
        self.env = old_env

        self.observation_space = old_env.observation_space
        self.action_space = old_env.action_space

    def reset(
        self, *, seed: Optional[int] = None, options: Optional[dict] = None
    ) -> Tuple[MultiAgentDict, MultiAgentDict]:
        # Use old `seed()` method.
        if seed is not None:
            self.env.seed(seed)
        # Options are ignored

        if self.render_mode == "human":
            self.render()

        obs = self.env.reset()
        infos = {k: {} for k in obs.keys()}
        return obs, infos

    def step(
        self, action
    ) -> Tuple[
        MultiAgentDict, MultiAgentDict, MultiAgentDict, MultiAgentDict, MultiAgentDict
    ]:
        obs, rewards, terminateds, infos = self.env.step(action)

        # Truncated should always be False by default.
        truncateds = {k: False for k in terminateds.keys()}

        return obs, rewards, terminateds, truncateds, infos

    def render(self):
        # Use the old `render()` API, where we have to pass in the mode to each call.
        return self.env.render(mode=self.render_mode)

    def close(self):
        self.env.close()
