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
from ray.util import log_once

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
        restart_failed_sub_environments: bool = False,
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
            restart_failed_sub_environments: If True and any sub-environment (within
                a vectorized env) throws any error during env stepping, the
                Sampler will try to restart the faulty sub-environment. This is done
                without disturbing the other (still intact) sub-environment and without
                the RolloutWorker crashing.

        Returns:
            The resulting _VectorizedGymEnv object (subclass of VectorEnv).
        """
        return _VectorizedGymEnv(
            make_env=make_env,
            existing_envs=existing_envs or [],
            num_envs=num_envs,
            observation_space=observation_space,
            action_space=action_space,
            restart_failed_sub_environments=restart_failed_sub_environments,
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
        """Resets a single sub-environment.

        Args:
            index: An optional sub-env index to reset.

        Returns:
            Observations from the reset sub environment.
        """
        raise NotImplementedError

    @PublicAPI
    def restart_at(self, index: Optional[int] = None) -> None:
        """Restarts a single sub-environment.

        Args:
            index: An optional sub-env index to restart.
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

    @Deprecated(new="vectorize_gym_envs", error=True)
    def wrap(self, *args, **kwargs) -> "_VectorizedGymEnv":
        return self.vectorize_gym_envs(*args, **kwargs)

    @Deprecated(new="get_sub_environments", error=True)
    def get_unwrapped(self) -> List[EnvType]:
        return self.get_sub_environments()

    @PublicAPI
    def to_base_env(
        self,
        make_env: Optional[Callable[[int], EnvType]] = None,
        num_envs: int = 1,
        remote_envs: bool = False,
        remote_env_batch_wait_ms: int = 0,
        restart_failed_sub_environments: bool = False,
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
        restart_failed_sub_environments: bool = False,
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
            restart_failed_sub_environments: If True and any sub-environment (within
                a vectorized env) throws any error during env stepping, we will try to
                restart the faulty sub-environment. This is done
                without disturbing the other (still intact) sub-environments.
        """
        self.envs = existing_envs
        self.make_env = make_env
        self.restart_failed_sub_environments = restart_failed_sub_environments

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
        # Use reset_at(index) to restart and retry until
        # we successfully create a new env.
        resetted_obs = []
        for i in range(len(self.envs)):
            while True:
                obs = self.reset_at(i)
                if not isinstance(obs, Exception):
                    break
            resetted_obs.append(obs)
        return resetted_obs

    @override(VectorEnv)
    def reset_at(self, index: Optional[int] = None) -> EnvObsType:
        if index is None:
            index = 0
        try:
            obs = self.envs[index].reset()
        except Exception as e:
            if self.restart_failed_sub_environments:
                logger.exception(e.args[0])
                self.restart_at(index)
                obs = e
            else:
                raise e
        return obs

    @override(VectorEnv)
    def restart_at(self, index: Optional[int] = None) -> None:
        if index is None:
            index = 0

        # Try closing down the old (possibly faulty) sub-env, but ignore errors.
        try:
            self.envs[index].close()
        except Exception as e:
            if log_once("close_sub_env"):
                logger.warning(
                    "Trying to close old and replaced sub-environment (at vector "
                    f"index={index}), but closing resulted in error:\n{e}"
                )

        # Re-create the sub-env at the new index.
        logger.warning(f"Trying to restart sub-environment at index {index}.")
        self.envs[index] = self.make_env(index)
        logger.warning(f"Sub-environment at index {index} restarted successfully.")

    @override(VectorEnv)
    def vector_step(self, actions):
        obs_batch, rew_batch, done_batch, info_batch = [], [], [], []
        for i in range(self.num_envs):
            try:
                obs, r, done, info = self.envs[i].step(actions[i])
            except Exception as e:
                if self.restart_failed_sub_environments:
                    logger.exception(e.args[0])
                    self.restart_at(i)
                    obs, r, done, info = e, 0.0, True, {}
                else:
                    raise e
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


@PublicAPI
class VectorEnvWrapper(BaseEnv):
    """Internal adapter of VectorEnv to BaseEnv.

    We assume the caller will always send the full vector of actions in each
    call to send_actions(), and that they call reset_at() on all completed
    environments before calling send_actions().
    """

    def __init__(self, vector_env: VectorEnv):
        self.vector_env = vector_env
        self.num_envs = vector_env.num_envs
        self._observation_space = vector_env.observation_space
        self._action_space = vector_env.action_space

        # Sub-environments' states.
        self.new_obs = None
        self.cur_rewards = None
        self.cur_dones = None
        self.cur_infos = None
        # At first `poll()`, reset everything (all sub-environments).
        self.first_reset_done = False
        # Initialize sub-environments' state.
        self._init_env_state(idx=None)

    @override(BaseEnv)
    def poll(
        self,
    ) -> Tuple[MultiEnvDict, MultiEnvDict, MultiEnvDict, MultiEnvDict, MultiEnvDict]:
        from ray.rllib.env.base_env import with_dummy_agent_id

        if not self.first_reset_done:
            self.first_reset_done = True
            self.new_obs = self.vector_env.vector_reset()
        new_obs = dict(enumerate(self.new_obs))
        rewards = dict(enumerate(self.cur_rewards))
        dones = dict(enumerate(self.cur_dones))
        infos = dict(enumerate(self.cur_infos))

        # Empty all states (in case `poll()` gets called again).
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
        env_id = env_id if env_id is not None else 0
        obs = self.vector_env.reset_at(env_id)
        return {env_id: obs if isinstance(obs, Exception) else {_DUMMY_AGENT_ID: obs}}

    @override(BaseEnv)
    def try_restart(self, env_id: Optional[EnvID] = None) -> None:
        assert env_id is None or isinstance(env_id, int)
        # Restart the sub-env at the index.
        self.vector_env.restart_at(env_id)
        # Auto-reset (get ready for next `poll()`).
        self._init_env_state(env_id)

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
    def observation_space(self) -> gym.Space:
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

    def _init_env_state(self, idx: Optional[int] = None) -> None:
        """Resets all or one particular sub-environment's state (by index).

        Args:
            idx: The index to reset at. If None, reset all the sub-environments' states.
        """
        # If index is None, reset all sub-envs' states:
        if idx is None:
            self.new_obs = [None for _ in range(self.num_envs)]
            self.cur_rewards = [0.0 for _ in range(self.num_envs)]
            self.cur_dones = [False for _ in range(self.num_envs)]
            self.cur_infos = [{} for _ in range(self.num_envs)]
        # Index provided, reset only the sub-env's state at the given index.
        else:
            self.new_obs[idx] = self.vector_env.reset_at(idx)
            # Reset all other states to null values.
            self.cur_rewards[idx] = 0.0
            self.cur_dones[idx] = False
            self.cur_infos[idx] = {}
