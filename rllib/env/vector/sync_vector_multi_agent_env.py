import gymnasium as gym
import numpy as np
import tree

from copy import deepcopy
from typing import Any, Callable, Dict, Iterator, List, Optional, Sequence, Tuple, Union

from ray.rllib.env.multi_agent_env import MultiAgentEnv
from ray.rllib.env.vector.vector_multi_agent_env import ArrayType, VectorMultiAgentEnv


class SyncVectorMultiAgentEnv(VectorMultiAgentEnv):

    def __init__(
        self,
        env_fns: Union[Iterator[Callable[[], MultiAgentEnv]], Sequence[Callable[[], MultiAgentEnv]]],
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

        self._observations = self.create_empty_array(self.single_observation_spaces, n=self.num_envs, fn=np.zeros)

    def reset(self, *, seed: Optional[int] = None, options: Optional[Dict[str, Any]] = None) -> Tuple[ArrayType, ArrayType]:

        if seed is None:
            seed = [None for _ in range(self.num_envs)]
        elif isinstance(seed, int):
            seed = [seed + i for i in range(self.num_envs)]
        else:
            assert (
                len(seed) == self.num_envs,
                f"If seeds are passed as a list the length must match num_envs={self.num_envs} "
                f"but got length={len(seed)}."
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

            self._terminations[reset_mask] = False
            self._truncations[reset_mask] = False
            self._autoreset_envs[reset_mask] = False

            infos = []
            for i, (env, single_seed, env_mask) in enumerate(
                zip(self.envs, seed, reset_mask)
            ):
                if env_mask:
                    self._observations[i], env_info = env.reset(
                        seed=single_seed, options=options,
                    )

                    infos.append(env_info) #self._add_info(infos, env_info, i)

        else:

            self._terminations = np.zeros((self.num_envs,), dtype=np.bool_)
            self._truncations = np.zeros((self.num_envs,), dtype=np.bool_)
            self._autoreset_envs = np.zeros((self.num_envs,), dtype=np.bool_)

            infos = []
            for i, (env, single_seed) in enumerate(zip(self.envs, seed)):
                self._observations[i], env_info = env.reset(
                    seed=single_seed, options=options
                )

                infos.append(env_info) # self._add_info(infos, env_info, i)
     
        return deepcopy(self._observations) if self.copy else self._observations, infos
            
    # TODO (simon)
    def create_empty_array(self, space: Dict[str, gym.Space], n: int = 1, fn=np.zeros) -> List[Dict[str, gym.Space]]:
        
        return [
            tree.map_structure(lambda x: x.sample(), self.single_observation_spaces) for _ in range(n)
        ]