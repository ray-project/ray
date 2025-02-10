import gymnasium as gym
import numpy as np

from gymnasium.core import RenderFrame
from gymnasium.envs.registration import EnvSpec
from typing import Any, Dict, Optional, Tuple, TypeVar


ArrayType = TypeVar("ArrayType")


class VectorMultiAgentEnv:

    metadata: Dict[str, Any] = {}
    spec: Optional[EnvSpec] = None
    render_mode: Optional[str] = None
    closed: bool = False

    single_observation_spaces: Optional[Dict[str, gym.Space]] = None
    single_action_spaces: Optional[Dict[str, gym.Space]] = None

    num_envs: int

    _np_random: Optional[np.random.Generator] = None
    _np_random_seed: Optional[int] = None

    # TODO (simon): Add docstrings, when final design is clear.
    def reset(
        self, *, seed: Optional[int] = None, options: Optional[Dict[str, Any]] = None
    ) -> Tuple[ArrayType, ArrayType]:
        pass

    def step(
        self, actions: ArrayType
    ) -> Tuple[ArrayType, ArrayType, ArrayType, ArrayType, ArrayType]:
        raise NotImplementedError(f"{self.__str__()} step function is not implemented.")

    def render(self) -> Optional[Tuple[RenderFrame, ...]]:
        raise NotImplementedError(
            f"{self.__str__()} render function is not implemented."
        )

    def close(self, **kwargs: Any):

        if self.closed:
            return

        self.close_extras(**kwargs)
        self.closed = True

    def close_extras(self, **kwargs: Any):
        # Users must not implement this.
        pass

    @property
    def unwrapped(self):
        return self

    def __del__(self):
        if not getattr(self, "closed", True):
            self.close()

    def __repr__(self):
        if self.spec is None:
            return f"{self.__class__.__name__}(num_envs={self.num_envs})"
        else:
            return (
                f"{self.__class__.__name__}({self.spec.id}, num_envs={self.num_envs})"
            )
