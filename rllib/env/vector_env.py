import logging
import gym
import numpy as np
from typing import Callable, List, Tuple

from ray.rllib.utils.annotations import override, PublicAPI
from ray.rllib.utils.typing import EnvType, EnvConfigDict, EnvObsType, \
    EnvInfoDict, EnvActionType

logger = logging.getLogger(__name__)


@PublicAPI
class VectorEnv:
    """An environment that supports batch evaluation using clones of sub-envs.
    """

    def __init__(self, observation_space: gym.Space, action_space: gym.Space,
                 num_envs: int):
        """Initializes a VectorEnv object.

        Args:
            observation_space (Space): The observation Space of a single
                sub-env.
            action_space (Space): The action Space of a single sub-env.
            num_envs (int): The number of clones to make of the given sub-env.
        """
        self.observation_space = observation_space
        self.action_space = action_space
        self.num_envs = num_envs

    @staticmethod
    def wrap(make_env: Callable[[int], EnvType] = None,
             existing_envs: List[gym.Env] = None,
             num_envs: int = 1,
             action_space: gym.Space = None,
             observation_space: gym.Space = None,
             env_config: EnvConfigDict = None):
        return _VectorizedGymEnv(
            make_env=make_env,
            existing_envs=existing_envs or [],
            num_envs=num_envs,
            observation_space=observation_space,
            action_space=action_space,
            env_config=env_config)

    @PublicAPI
    def vector_reset(self) -> List[EnvObsType]:
        """Resets all sub-environments.

        Returns:
            obs (List[any]): List of observations from each environment.
        """
        raise NotImplementedError

    @PublicAPI
    def reset_at(self, index: int) -> EnvObsType:
        """Resets a single environment.

        Returns:
            obs (obj): Observations from the reset sub environment.
        """
        raise NotImplementedError

    @PublicAPI
    def vector_step(
            self, actions: List[EnvActionType]
    ) -> Tuple[List[EnvObsType], List[float], List[bool], List[EnvInfoDict]]:
        """Performs a vectorized step on all sub environments using `actions`.

        Args:
            actions (List[any]): List of actions (one for each sub-env).

        Returns:
            obs (List[any]): New observations for each sub-env.
            rewards (List[any]): Reward values for each sub-env.
            dones (List[any]): Done values for each sub-env.
            infos (List[any]): Info values for each sub-env.
        """
        raise NotImplementedError

    @PublicAPI
    def get_unwrapped(self) -> List[EnvType]:
        """Returns the underlying sub environments.

        Returns:
            List[Env]: List of all underlying sub environments.
        """
        raise NotImplementedError


class _VectorizedGymEnv(VectorEnv):
    """Internal wrapper to translate any gym envs into a VectorEnv object.
    """

    def __init__(self,
                 make_env=None,
                 existing_envs=None,
                 num_envs=1,
                 *,
                 observation_space=None,
                 action_space=None,
                 env_config=None):
        """Initializes a _VectorizedGymEnv object.

        Args:
            make_env (Optional[callable]): Factory that produces a new gym env
                taking a single `config` dict arg. Must be defined if the
                number of `existing_envs` is less than `num_envs`.
            existing_envs (Optional[List[Env]]): Optional list of already
                instantiated sub environments.
            num_envs (int): Total number of sub environments in this VectorEnv.
            action_space (Optional[Space]): The action space. If None, use
                existing_envs[0]'s action space.
            observation_space (Optional[Space]): The observation space.
                If None, use existing_envs[0]'s action space.
            env_config (Optional[dict]): Additional sub env config to pass to
                make_env as first arg.
        """
        self.make_env = make_env
        self.envs = existing_envs
        while len(self.envs) < num_envs:
            self.envs.append(self.make_env(len(self.envs)))

        super().__init__(
            observation_space=observation_space
            or self.envs[0].observation_space,
            action_space=action_space or self.envs[0].action_space,
            num_envs=num_envs)

    @override(VectorEnv)
    def vector_reset(self):
        return [e.reset() for e in self.envs]

    @override(VectorEnv)
    def reset_at(self, index):
        return self.envs[index].reset()

    @override(VectorEnv)
    def vector_step(self, actions):
        obs_batch, rew_batch, done_batch, info_batch = [], [], [], []
        for i in range(self.num_envs):
            obs, r, done, info = self.envs[i].step(actions[i])
            if not np.isscalar(r) or not np.isreal(r) or not np.isfinite(r):
                raise ValueError(
                    "Reward should be finite scalar, got {} ({}). "
                    "Actions={}.".format(r, type(r), actions[i]))
            if not isinstance(info, dict):
                raise ValueError("Info should be a dict, got {} ({})".format(
                    info, type(info)))
            obs_batch.append(obs)
            rew_batch.append(r)
            done_batch.append(done)
            info_batch.append(info)
        return obs_batch, rew_batch, done_batch, info_batch

    @override(VectorEnv)
    def get_unwrapped(self):
        return self.envs
