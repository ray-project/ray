import gymnasium as gym
import numpy as np
import tree

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

        # TODO (simon): Check, if we need to define `get_observation_space(agent)`.
        self.single_action_spaces = self.envs[0].unwrapped.action_spaces
        self.single_observation_spaces = self.envs[0].unwrapped.observation_spaces

        # TODO (simon): Decide if we want to include a spaces check here.

        self._observations = self.create_empty_array(
            self.single_observation_spaces, n=self.num_envs, fn=np.zeros
        )
        self._rewards = [{}] * self.num_envs
        self._terminations = [{}] * self.num_envs
        self._truncations = [{}] * self.num_envs
        self._infos = [{}] * self.num_envs

    def reset(
        self, *, seed: Optional[int] = None, options: Optional[Dict[str, Any]] = None
    ) -> Tuple[ArrayType, ArrayType]:

        if seed is None:
            seed = [None for _ in range(self.num_envs)]
        elif isinstance(seed, int):
            seed = [seed + i for i in range(self.num_envs)]
        else:
            assert (
                len(seed) == self.num_envs,
                f"If seeds are passed as a list the length must match num_envs={self.num_envs} "
                f"but got length={len(seed)}.",
            )

        if options is not None and "reset_mask" in options:
            reset_mask = options.pop("reset_mask")
            assert isinstance(
                reset_mask, np.ndarray
            ), f"`options['reset_mask': mask]` must be a numpy array, got {type(reset_mask)}"
            assert reset_mask.shape == (
                self.num_envs,
            ), f"`options['reset_mask': mask]` must have shape `({self.num_envs},)`, got {reset_mask.shape}"
            assert (
                reset_mask.dtype == np.bool_
            ), f"`options['reset_mask': mask]` must have `dtype=np.bool_`, got {reset_mask.dtype}"
            assert np.any(
                reset_mask
            ), f"`options['reset_mask': mask]` must contain a boolean array, got reset_mask={reset_mask}"

            # self._terminations[reset_mask] = False
            # self._truncations[reset_mask] = False
            # self._autoreset_envs[reset_mask] = False

            for i, (env, single_seed, env_mask) in enumerate(
                zip(self.envs, seed, reset_mask)
            ):
                if env_mask:
                    self._observations[i], self._infos[i] = env.reset(
                        seed=single_seed,
                        options=options,
                    )

        else:

            # self._terminations = np.zeros((self.num_envs,), dtype=np.bool_)
            # self._truncations = np.zeros((self.num_envs,), dtype=np.bool_)
            self._autoreset_envs = np.zeros((self.num_envs,), dtype=np.bool_)

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

        infos = []
        for i, action in enumerate(actions):
            if self.autoreset_mode == "next_step":
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
                    self._terminations[i].update({"__all__": False})
                    self._truncations[i].update({"__all__": False})
                else:
                    (
                        self._observations[i],
                        self._rewards[i],
                        self._terminations[i],
                        self._truncations[i],
                        self._infos[i],
                    ) = self.envs[i].step(action)
            else:
                raise ValueError(f"Unexpected autoreset mode: {self.autoreset_mode}")

        self._autoreset_envs = np.logical_or(
            np.array([t["__all__"] for t in self._terminations]),
            np.array([t["__all__"] for t in self._truncations]),
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

    # TODO (simon): Move to space_utils.
    def create_empty_array(
        self, space: Dict[str, gym.Space], n: int = 1, fn=np.zeros
    ) -> List[Dict[str, gym.Space]]:

        return [
            tree.map_structure(lambda x: x.sample(), self.single_observation_spaces)
            for _ in range(n)
        ]
