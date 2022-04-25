import logging
import gym
import numpy as np
from typing import Callable, List, Optional, Tuple, Union, Set

from ray.rllib.env.base_env import BaseEnv, _DUMMY_AGENT_ID
from ray.rllib.utils.annotations import Deprecated, override, PublicAPI
from ray.rllib.utils.typing import (
    EnvActionType,
    EnvID,
    EnvInfoDict,
    EnvObsType,
    EnvType,
    MultiEnvDict,
    AgentID,
)

logger = logging.getLogger(__name__)


@PublicAPI
class VectorEnv:
    """An environment that supports batch evaluation using clones of sub-envs."""

    def __init__(
        self, observation_space: gym.Space, action_space: gym.Space, num_envs: int
    ):
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
        policy_config=None,
    ) -> "_VectorizedGymEnv":
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
                existing_envs[0]'s observation space.

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
    def try_render_at(self, index: Optional[int] = None) -> Optional[np.ndarray]:
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

    @PublicAPI
    def to_base_env(
        self,
        make_env: Optional[Callable[[int], EnvType]] = None,
        num_envs: int = 1,
        remote_envs: bool = False,
        remote_env_batch_wait_ms: int = 0,
    ) -> "BaseEnv":
        """Converts an RLlib MultiAgentEnv into a BaseEnv object.

        The resulting BaseEnv is always vectorized (contains n
        sub-environments) to support batched forward passes, where n may
        also be 1. BaseEnv also supports async execution via the `poll` and
        `send_actions` methods and thus supports external simulators.

        Args:
            make_env: A callable taking an int as input (which indicates
                the number of individual sub-environments within the final
                vectorized BaseEnv) and returning one individual
                sub-environment.
            num_envs: The number of sub-environments to create in the
                resulting (vectorized) BaseEnv. The already existing `env`
                will be one of the `num_envs`.
            remote_envs: Whether each sub-env should be a @ray.remote
                actor. You can set this behavior in your config via the
                `remote_worker_envs=True` option.
            remote_env_batch_wait_ms: The wait time (in ms) to poll remote
                sub-environments for, if applicable. Only used if
                `remote_envs` is True.

        Returns:
            The resulting BaseEnv object.
        """
        del make_env, num_envs, remote_envs, remote_env_batch_wait_ms
        env = VectorEnvWrapper(self)
        return env


class _VectorizedGymEnv(VectorEnv):
    """Internal wrapper to translate any gym.Envs into a VectorEnv object."""

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
                existing_envs[0]'s observation space.
        """
        self.envs = existing_envs

        # Fill up missing envs (so we have exactly num_envs sub-envs in this
        # VectorEnv.
        while len(self.envs) < num_envs:
            self.envs.append(make_env(len(self.envs)))

        super().__init__(
            observation_space=observation_space or self.envs[0].observation_space,
            action_space=action_space or self.envs[0].action_space,
            num_envs=num_envs,
        )

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
            if not isinstance(info, dict):
                raise ValueError(
                    "Info should be a dict, got {} ({})".format(info, type(info))
                )
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


class VectorEnvWrapper(BaseEnv):
    """Internal adapter of VectorEnv to BaseEnv.

    We assume the caller will always send the full vector of actions in each
    call to send_actions(), and that they call reset_at() on all completed
    environments before calling send_actions().
    """

    def __init__(self, vector_env: VectorEnv):
        self.vector_env = vector_env
        self.num_envs = vector_env.num_envs
        self.new_obs = None  # lazily initialized
        self.cur_rewards = [None for _ in range(self.num_envs)]
        self.cur_dones = [False for _ in range(self.num_envs)]
        self.cur_infos = [None for _ in range(self.num_envs)]
        self._observation_space = vector_env.observation_space
        self._action_space = vector_env.action_space

    @override(BaseEnv)
    def poll(
        self,
    ) -> Tuple[MultiEnvDict, MultiEnvDict, MultiEnvDict, MultiEnvDict, MultiEnvDict]:
        from ray.rllib.env.base_env import with_dummy_agent_id

        if self.new_obs is None:
            self.new_obs = self.vector_env.vector_reset()
        new_obs = dict(enumerate(self.new_obs))
        rewards = dict(enumerate(self.cur_rewards))
        dones = dict(enumerate(self.cur_dones))
        infos = dict(enumerate(self.cur_infos))
        self.new_obs = []
        self.cur_rewards = []
        self.cur_dones = []
        self.cur_infos = []
        return (
            with_dummy_agent_id(new_obs),
            with_dummy_agent_id(rewards),
            with_dummy_agent_id(dones, "__all__"),
            with_dummy_agent_id(infos),
            {},
        )

    @override(BaseEnv)
    def send_actions(self, action_dict: MultiEnvDict) -> None:
        from ray.rllib.env.base_env import _DUMMY_AGENT_ID

        action_vector = [None] * self.num_envs
        for i in range(self.num_envs):
            action_vector[i] = action_dict[i][_DUMMY_AGENT_ID]
        (
            self.new_obs,
            self.cur_rewards,
            self.cur_dones,
            self.cur_infos,
        ) = self.vector_env.vector_step(action_vector)

    @override(BaseEnv)
    def try_reset(self, env_id: Optional[EnvID] = None) -> MultiEnvDict:
        from ray.rllib.env.base_env import _DUMMY_AGENT_ID

        assert env_id is None or isinstance(env_id, int)
        return {
            env_id
            if env_id is not None
            else 0: {_DUMMY_AGENT_ID: self.vector_env.reset_at(env_id)}
        }

    @override(BaseEnv)
    def get_sub_environments(self, as_dict: bool = False) -> Union[List[EnvType], dict]:
        if not as_dict:
            return self.vector_env.get_sub_environments()
        else:
            return {
                _id: env
                for _id, env in enumerate(self.vector_env.get_sub_environments())
            }

    @override(BaseEnv)
    def try_render(self, env_id: Optional[EnvID] = None) -> None:
        assert env_id is None or isinstance(env_id, int)
        return self.vector_env.try_render_at(env_id)

    @property
    @override(BaseEnv)
    @PublicAPI
    def observation_space(self) -> gym.spaces.Dict:
        return self._observation_space

    @property
    @override(BaseEnv)
    @PublicAPI
    def action_space(self) -> gym.Space:
        return self._action_space

    @override(BaseEnv)
    @PublicAPI
    def action_space_sample(self, agent_id: list = None) -> MultiEnvDict:
        del agent_id
        return {0: {_DUMMY_AGENT_ID: self._action_space.sample()}}

    @override(BaseEnv)
    @PublicAPI
    def observation_space_sample(self, agent_id: list = None) -> MultiEnvDict:
        del agent_id
        return {0: {_DUMMY_AGENT_ID: self._observation_space.sample()}}

    @override(BaseEnv)
    @PublicAPI
    def get_agent_ids(self) -> Set[AgentID]:
        return {_DUMMY_AGENT_ID}
