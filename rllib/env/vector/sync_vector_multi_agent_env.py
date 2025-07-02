import gymnasium as gym
import numpy as np

from copy import deepcopy
from gymnasium.core import ActType, RenderFrame
from typing import Any, Callable, Dict, Iterator, List, Optional, Sequence, Tuple, Union

from ray.rllib.env.multi_agent_env import MultiAgentEnv
from ray.rllib.env.vector.vector_multi_agent_env import ArrayType, VectorMultiAgentEnv
from ray.rllib.utils.typing import AgentID


class SyncVectorMultiAgentEnv(VectorMultiAgentEnv):
    def __init__(
        self,
        env_fns: Union[
            Iterator[Callable[[], MultiAgentEnv]], Sequence[Callable[[], MultiAgentEnv]]
        ],
        copy: bool = True,
        autoreset_mode: str = "next_step",
    ):
        super().__init__()

        self.env_fns = env_fns
        self.copy = copy
        self.autoreset_mode = autoreset_mode

        # Initialize all sub-environments.
        self.envs = [env_fn() for env_fn in self.env_fns]

        self.num_envs = len(self.envs)
        self.metadata = self.envs[0].metadata
        self.metadata["autoreset_mode"] = self.autoreset_mode
        self.render_mode = self.envs[0].render_mode

        if self.envs[0].unwrapped.observation_spaces is not None:
            self.single_observation_spaces = self.envs[0].unwrapped.observation_spaces
        elif self.envs[0].unwrapped.observation_space is not None:
            self.single_observation_spaces = dict(
                self.envs[0].unwrapped.observation_space
            )
        else:
            self.single_observation_spaces = {
                aid: self.envs[0].unwrapped.get_observation_space(aid)
                for aid in self.envs[0].unwrapped.possible_agents
            }
        self.single_observation_space = gym.spaces.Dict(self.single_observation_spaces)

        if self.envs[0].unwrapped.action_spaces is not None:
            self.single_action_spaces = self.envs[0].unwrapped.action_spaces
        elif self.envs[0].unwrapped.action_space is not None:
            self.single_action_spaces = dict(self.envs[0].unwrapped.action_space)
        else:
            self.single_action_spaces = {
                aid: self.envs[0].unwrapped.get_action_space(aid)
                for aid in self.envs[0].unwrapped.possible_agents
            }
        self.single_action_space = gym.spaces.Dict(self.single_action_spaces)

        # TODO (simon): Decide if we want to include a spaces check here.

        # Note, if `single_observation_spaces` are not defined, this will
        # raise an error.
        self._observations = self.create_empty_array(
            self.single_observation_spaces,
            n=self.num_envs,
        )
        self._rewards = [{} for _ in range(self.num_envs)]
        self._terminations = [{} for _ in range(self.num_envs)]
        self._truncations = [{} for _ in range(self.num_envs)]
        self._infos = [{} for _ in range(self.num_envs)]

    def reset(
        self, *, seed: Optional[int] = None, options: Optional[Dict[str, Any]] = None
    ) -> Tuple[ArrayType, ArrayType]:

        # Check the `seed`.
        if seed is None:
            seed = [None for _ in range(self.num_envs)]
        elif isinstance(seed, int):
            seed = [seed + i for i in range(self.num_envs)]
        # If `seed` is a sequence, check if length is correct.
        else:
            assert len(seed) == self.num_envs, (
                f"If seeds are passed as a list the length must match num_envs={self.num_envs} "
                f"but got length={len(seed)}.",
            )

        # Set auto-reset to `False` for all sub-environments. This will be
        # set to `True` during `VectorMultiAgentEnv.step`.
        self._autoreset_envs = np.zeros((self.num_envs,), dtype=np.bool_)

        # Force reset on all sub-environments.
        for i, (env, single_seed) in enumerate(zip(self.envs, seed)):
            self._observations[i], self._infos[i] = env.reset(
                seed=single_seed, options=options
            )

        return (
            deepcopy(self._observations) if self.copy else self._observations,
            self._infos,
        )

    def step(
        self, actions: List[Dict[AgentID, ActType]]
    ) -> Tuple[ArrayType, ArrayType, ArrayType, ArrayType, ArrayType]:

        for i, action in enumerate(actions):
            # Note, this will be a feature coming in `gymnasium`s next release.
            if self.autoreset_mode == "next_step":
                # Auto-reset environments that terminated or truncated.
                if self._autoreset_envs[i]:
                    self._observations[i], self._infos[i] = self.envs[i].reset()

                    self._rewards[i] = {
                        aid: 0.0 for aid in self.envs[i].unwrapped.possible_agents
                    }
                    self._terminations[i] = {
                        aid: False for aid in self.envs[i].unwrapped.possible_agents
                    }
                    self._truncations[i] = {
                        aid: False for aid in self.envs[i].unwrapped.possible_agents
                    }
                    # Note, this needs to be added for `MultiAgentEpisode`s to work.
                    self._terminations[i].update({"__all__": False})
                    self._truncations[i].update({"__all__": False})
                # Otherwise make a regular step.
                else:
                    (
                        self._observations[i],
                        self._rewards[i],
                        self._terminations[i],
                        self._truncations[i],
                        self._infos[i],
                    ) = self.envs[i].step(action)
            # Other autoreset modes are not implemented, yet.
            else:
                raise ValueError(f"Unexpected autoreset mode: {self.autoreset_mode}")

        # Is any sub-environment terminated or truncated?
        self._autoreset_envs = np.logical_or(
            np.array([t["__all__"] for t in self._terminations]),
            # Note, some environments just return `terminated`.
            np.array([t.get("__all__", False) for t in self._truncations]),
        )

        return (
            deepcopy(self._observations) if self.copy else self._observations,
            np.copy(self._rewards),
            np.copy(self._terminations),
            np.copy(self._truncations),
            np.copy(self._infos),
        )

    def render(self) -> Optional[Tuple[RenderFrame, ...]]:
        """Returns the rendered frames from all environments."""
        return tuple(env.render() for env in self.envs)

    def call(self, name: str, *args: Any, **kwargs: Any) -> tuple[Any, ...]:
        """Calls a sub-environment method with name and applies args and kwargs.

        Args:
            name: The method name
            *args: The method args
            **kwargs: The method kwargs

        Returns:
            Tuple of results
        """
        results = []
        for env in self.envs:
            function = env.get_wrapper_attr(name)

            if callable(function):
                results.append(function(*args, **kwargs))
            else:
                results.append(function)

        return tuple(results)

    def get_attr(self, name: str) -> tuple[Any, ...]:
        """Get a property from each parallel environment.

        Args:
            name: Name of the property to get from each individual environment.

        Returns:
            The property with name
        """
        return self.call(name)

    def close_extras(self, **kwargs):
        """Close all environments."""
        if hasattr(self, "envs"):
            [env.close() for env in self.envs]

    @property
    def np_random_seed(self) -> Tuple[int, ...]:
        """Returns a tuple of random seeds for the wrapped envs."""
        return self.get_attr("np_random_seed")

    @property
    def np_random(self) -> Tuple[np.random.Generator, ...]:
        """Returns a tuple of numpy random generators for the wrapped envs."""
        return self.get_attr("np_random")

    # TODO (simon): Move to space_utils.
    def create_empty_array(
        self, space: Dict[str, gym.Space], n: int = 1
    ) -> List[Dict[str, gym.Space]]:

        return [
            {aid: agent_space.sample() for aid, agent_space in space.items()}
            for _ in range(n)
        ]
