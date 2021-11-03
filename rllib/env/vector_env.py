import logging
import gym
import numpy as np
from typing import Callable, List, Optional, Tuple

from ray.rllib.utils.annotations import Deprecated, override, PublicAPI
from ray.rllib.utils.typing import EnvActionType, EnvInfoDict, \
    EnvObsType, EnvType

logger = logging.getLogger(__name__)


@PublicAPI
class VectorEnv:
    """An environment that supports batch evaluation using clones of sub-envs.
    """

    def __init__(self, observation_space: gym.Space, action_space: gym.Space,
                 num_envs: int):
        """Initializes a VectorEnv instance.

        Args:
            observation_space: The observation Space of a single
                sub-env.
            action_space: The action Space of a single sub-env.
            num_envs: The number of clones to make of the given sub-env.
        """
        self.observation_space = observation_space
        self.action_space = action_space
        self.num_envs = num_envs

    @staticmethod
    def vectorize_gym_envs(
            make_env: Optional[Callable[[int], EnvType]] = None,
            existing_envs: Optional[List[gym.Env]] = None,
            num_envs: int = 1,
            action_space: Optional[gym.Space] = None,
            observation_space: Optional[gym.Space] = None,
            # Deprecated. These seem to have never been used.
            env_config=None,
            policy_config=None) -> "_VectorizedGymEnv":
        """Translates any given gym.Env(s) into a VectorizedEnv object.

        Args:
            make_env: Factory that produces a new gym.Env taking the sub-env's
                vector index as only arg. Must be defined if the
                number of `existing_envs` is less than `num_envs`.
            existing_envs: Optional list of already instantiated sub
                environments.
            num_envs: Total number of sub environments in this VectorEnv.
            action_space: The action space. If None, use existing_envs[0]'s
                action space.
            observation_space: The observation space. If None, use
                existing_envs[0]'s action space.

        Returns:
            The resulting _VectorizedGymEnv object (subclass of VectorEnv).
        """
        return _VectorizedGymEnv(
            make_env=make_env,
            existing_envs=existing_envs or [],
            num_envs=num_envs,
            observation_space=observation_space,
            action_space=action_space,
        )

    @PublicAPI
    def vector_reset(self) -> List[EnvObsType]:
        """Resets all sub-environments.

        Returns:
            List of observations from each environment.
        """
        raise NotImplementedError

    @PublicAPI
    def reset_at(self, index: Optional[int] = None) -> EnvObsType:
        """Resets a single environment.

        Args:
            index: An optional sub-env index to reset.

        Returns:
            Observations from the reset sub environment.
        """
        raise NotImplementedError

    @PublicAPI
    def vector_step(
            self, actions: List[EnvActionType]
    ) -> Tuple[List[EnvObsType], List[float], List[bool], List[EnvInfoDict]]:
        """Performs a vectorized step on all sub environments using `actions`.

        Args:
            actions: List of actions (one for each sub-env).

        Returns:
            A tuple consisting of
            1) New observations for each sub-env.
            2) Reward values for each sub-env.
            3) Done values for each sub-env.
            4) Info values for each sub-env.
        """
        raise NotImplementedError

    @PublicAPI
    def get_sub_environments(self) -> List[EnvType]:
        """Returns the underlying sub environments.

        Returns:
            List of all underlying sub environments.
        """
        return []

    # TODO: (sven) Experimental method. Make @PublicAPI at some point.
    def try_render_at(self, index: Optional[int] = None) -> \
            Optional[np.ndarray]:
        """Renders a single environment.

        Args:
            index: An optional sub-env index to render.

        Returns:
            Either a numpy RGB image (shape=(w x h x 3) dtype=uint8) or
            None in case rendering is handled directly by this method.
        """
        pass

    @Deprecated(new="vectorize_gym_envs", error=False)
    def wrap(self, *args, **kwargs) -> "_VectorizedGymEnv":
        return self.vectorize_gym_envs(*args, **kwargs)

    @Deprecated(new="get_sub_environments", error=False)
    def get_unwrapped(self) -> List[EnvType]:
        return self.get_sub_environments()


class _VectorizedGymEnv(VectorEnv):
    """Internal wrapper to translate any gym.Envs into a VectorEnv object.
    """

    def __init__(
            self,
            make_env: Optional[Callable[[int], EnvType]] = None,
            existing_envs: Optional[List[gym.Env]] = None,
            num_envs: int = 1,
            *,
            observation_space: Optional[gym.Space] = None,
            action_space: Optional[gym.Space] = None,
            # Deprecated. These seem to have never been used.
            env_config=None,
            policy_config=None,
    ):
        """Initializes a _VectorizedGymEnv object.

        Args:
            make_env: Factory that produces a new gym.Env taking the sub-env's
                vector index as only arg. Must be defined if the
                number of `existing_envs` is less than `num_envs`.
            existing_envs: Optional list of already instantiated sub
                environments.
            num_envs: Total number of sub environments in this VectorEnv.
            action_space: The action space. If None, use existing_envs[0]'s
                action space.
            observation_space: The observation space. If None, use
                existing_envs[0]'s action space.
        """
        self.envs = existing_envs

        # Fill up missing envs (so we have exactly num_envs sub-envs in this
        # VectorEnv.
        while len(self.envs) < num_envs:
            self.envs.append(make_env(len(self.envs)))

        super().__init__(
            observation_space=observation_space
            or self.envs[0].observation_space,
            action_space=action_space or self.envs[0].action_space,
            num_envs=num_envs)

    @override(VectorEnv)
    def vector_reset(self):
        return [e.reset() for e in self.envs]

    @override(VectorEnv)
    def reset_at(self, index: Optional[int] = None) -> EnvObsType:
        if index is None:
            index = 0
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
    def get_sub_environments(self):
        return self.envs

    @override(VectorEnv)
    def try_render_at(self, index: Optional[int] = None):
        if index is None:
            index = 0
        return self.envs[index].render()
