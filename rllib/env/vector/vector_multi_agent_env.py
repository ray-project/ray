from typing import Any, Dict, List, Optional, Tuple, TypeVar

import gymnasium as gym
import numpy as np
from gymnasium.core import RenderFrame
from gymnasium.envs.registration import EnvSpec
from gymnasium.utils import seeding

ArrayType = TypeVar("ArrayType")


class VectorMultiAgentEnv:

    metadata: Dict[str, Any] = {}
    spec: Optional[EnvSpec] = None
    render_mode: Optional[str] = None
    closed: bool = False

    envs: Optional[List] = None

    # TODO (simon, sven): We could think about enabling here different
    # spaces for different envs (e.g. different high/lows). In this
    # case we would need here actually "batched" spaces and not a
    # single on that holds for all sub-envs.
    single_observation_spaces: Optional[Dict[str, gym.Space]] = None
    single_action_spaces: Optional[Dict[str, gym.Space]] = None
    # Note, the proper `gym` spaces are needed for the connector pipeline.
    single_observation_space: Optional[gym.spaces.Dict] = None
    single_action_space: Optional[gym.spaces.Dict] = None

    num_envs: int

    _np_random: Optional[np.random.Generator] = None
    _np_random_seed: Optional[int] = None

    # @OldAPIStack, use `observation_spaces` and `action_spaces`, instead.
    observation_space: Optional[gym.Space] = None
    action_space: Optional[gym.Space] = None

    # TODO (simon): Add docstrings, when final design is clear.
    def reset(
        self, *, seed: Optional[int] = None, options: Optional[Dict[str, Any]] = None
    ) -> Tuple[ArrayType, ArrayType]:
        # Set random generators with the provided seeds.
        if seed is not None:
            self._np_random, self._np_random_seed = seeding.np_random(seed)

    def step(
        self, actions: ArrayType
    ) -> Tuple[ArrayType, ArrayType, ArrayType, ArrayType, ArrayType]:
        raise NotImplementedError(f"{self.__str__()} step function is not implemented.")

    def render(self) -> Optional[Tuple[RenderFrame, ...]]:
        raise NotImplementedError(
            f"{self.__str__()} render function is not implemented."
        )

    def close(self, **kwargs: Any):

        # If already closed, there is nothing more to do.
        if self.closed:
            return

        # Otherwise close environments gracefully.
        self.close_extras(**kwargs)
        self.closed = True

    def close_extras(self, **kwargs: Any):
        # Users must not implement this.
        pass

    @property
    def unwrapped(self):
        return self

    def __del__(self):
        # Close environemnts, if necessary when deleting instances.
        if not getattr(self, "closed", True):
            self.close()

    def __repr__(self):
        if self.spec is None:
            return f"{self.__class__.__name__}(num_envs={self.num_envs})"
        else:
            return (
                f"{self.__class__.__name__}({self.spec.id}, num_envs={self.num_envs})"
            )
