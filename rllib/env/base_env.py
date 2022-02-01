import logging
from typing import Callable, Tuple, Optional, List, Dict, Any, TYPE_CHECKING, Union, Set

import gym
import ray
from ray.rllib.utils.annotations import Deprecated, override, PublicAPI
from ray.rllib.utils.typing import AgentID, EnvID, EnvType, MultiAgentDict, MultiEnvDict

if TYPE_CHECKING:
    from ray.rllib.models.preprocessors import Preprocessor
    from ray.rllib.env.external_env import ExternalEnv
    from ray.rllib.env.multi_agent_env import MultiAgentEnv
    from ray.rllib.env.vector_env import VectorEnv

ASYNC_RESET_RETURN = "async_reset_return"

logger = logging.getLogger(__name__)


@PublicAPI
class BaseEnv:
    """The lowest-level env interface used by RLlib for sampling.

    BaseEnv models multiple agents executing asynchronously in multiple
    vectorized sub-environments. A call to `poll()` returns observations from
    ready agents keyed by their sub-environment ID and agent IDs, and
    actions for those agents can be sent back via `send_actions()`.

    All other RLlib supported env types can be converted to BaseEnv.
    RLlib handles these conversions internally in RolloutWorker, for example:

    gym.Env => rllib.VectorEnv => rllib.BaseEnv
    rllib.MultiAgentEnv (is-a gym.Env) => rllib.VectorEnv => rllib.BaseEnv
    rllib.ExternalEnv => rllib.BaseEnv

    Examples:
        >>> env = MyBaseEnv()
        >>> obs, rewards, dones, infos, off_policy_actions = env.poll()
        >>> print(obs)
        {
            "env_0": {
                "car_0": [2.4, 1.6],
                "car_1": [3.4, -3.2],
            },
            "env_1": {
                "car_0": [8.0, 4.1],
            },
            "env_2": {
                "car_0": [2.3, 3.3],
                "car_1": [1.4, -0.2],
                "car_3": [1.2, 0.1],
            },
        }
        >>> env.send_actions({
        ...   "env_0": {
        ...     "car_0": 0,
        ...     "car_1": 1,
        ...   }, ...
        ... })
        >>> obs, rewards, dones, infos, off_policy_actions = env.poll()
        >>> print(obs)
        {
            "env_0": {
                "car_0": [4.1, 1.7],
                "car_1": [3.2, -4.2],
            }, ...
        }
        >>> print(dones)
        {
            "env_0": {
                "__all__": False,
                "car_0": False,
                "car_1": True,
            }, ...
        }
    """

    def to_base_env(
        self,
        make_env: Optional[Callable[[int], EnvType]] = None,
        num_envs: int = 1,
        remote_envs: bool = False,
        remote_env_batch_wait_ms: int = 0,
    ) -> "BaseEnv":
        """Converts an RLlib-supported env into a BaseEnv object.

        Supported types for the `env` arg are gym.Env, BaseEnv,
        VectorEnv, MultiAgentEnv, ExternalEnv, or ExternalMultiAgentEnv.

        The resulting BaseEnv is always vectorized (contains n
        sub-environments) to support batched forward passes, where n may also
        be 1. BaseEnv also supports async execution via the `poll` and
        `send_actions` methods and thus supports external simulators.

        TODO: Support gym3 environments, which are already vectorized.

        Args:
            env: An already existing environment of any supported env type
                to convert/wrap into a BaseEnv. Supported types are gym.Env,
                BaseEnv, VectorEnv, MultiAgentEnv, ExternalEnv, and
                ExternalMultiAgentEnv.
            make_env: A callable taking an int as input (which indicates the
                number of individual sub-environments within the final
                vectorized BaseEnv) and returning one individual
                sub-environment.
            num_envs: The number of sub-environments to create in the
                resulting (vectorized) BaseEnv. The already existing `env`
                will be one of the `num_envs`.
            remote_envs: Whether each sub-env should be a @ray.remote actor.
                You can set this behavior in your config via the
                `remote_worker_envs=True` option.
            remote_env_batch_wait_ms: The wait time (in ms) to poll remote
                sub-environments for, if applicable. Only used if
                `remote_envs` is True.
            policy_config: Optional policy config dict.

        Returns:
            The resulting BaseEnv object.
        """
        del make_env, num_envs, remote_envs, remote_env_batch_wait_ms
        return self

    @PublicAPI
    def poll(
        self,
    ) -> Tuple[MultiEnvDict, MultiEnvDict, MultiEnvDict, MultiEnvDict, MultiEnvDict]:
        """Returns observations from ready agents.

        All return values are two-level dicts mapping from EnvID to dicts
        mapping from AgentIDs to (observation/reward/etc..) values.
        The number of agents and sub-environments may vary over time.

        Returns:
            Tuple consisting of
            1) New observations for each ready agent.
            2) Reward values for each ready agent. If the episode is
            just started, the value will be None.
            3) Done values for each ready agent. The special key "__all__"
            is used to indicate env termination.
            4) Info values for each ready agent.
            5) Agents may take off-policy actions. When that
            happens, there will be an entry in this dict that contains the
            taken action. There is no need to send_actions() for agents that
            have already chosen off-policy actions.
        """
        raise NotImplementedError

    @PublicAPI
    def send_actions(self, action_dict: MultiEnvDict) -> None:
        """Called to send actions back to running agents in this env.

        Actions should be sent for each ready agent that returned observations
        in the previous poll() call.

        Args:
            action_dict: Actions values keyed by env_id and agent_id.
        """
        raise NotImplementedError

    @PublicAPI
    def try_reset(
        self, env_id: Optional[EnvID] = None
    ) -> Optional[Union[MultiAgentDict, MultiEnvDict]]:
        """Attempt to reset the sub-env with the given id or all sub-envs.

        If the environment does not support synchronous reset, None can be
        returned here.

        Args:
            env_id: The sub-environment's ID if applicable. If None, reset
                the entire Env (i.e. all sub-environments).

        Note: A MultiAgentDict is returned when using the deprecated wrapper
        classes such as `ray.rllib.env.base_env._MultiAgentEnvToBaseEnv`,
        however for consistency with the poll() method, a `MultiEnvDict` is
        returned from the new wrapper classes, such as
        `ray.rllib.env.multi_agent_env.MultiAgentEnvWrapper`.

        Returns:
            The reset (multi-agent) observation dict. None if reset is not
            supported.
        """
        return None

    @PublicAPI
    def get_sub_environments(self, as_dict: bool = False) -> Union[List[EnvType], dict]:
        """Return a reference to the underlying sub environments, if any.

        Args:
            as_dict: If True, return a dict mapping from env_id to env.

        Returns:
            List or dictionary of the underlying sub environments or [] / {}.
        """
        if as_dict:
            return {}
        return []

    @PublicAPI
    def get_agent_ids(self) -> Set[AgentID]:
        """Return the agent ids for the sub_environment.

        Returns:
            All agent ids for each the environment.
        """
        return {_DUMMY_AGENT_ID}

    @PublicAPI
    def try_render(self, env_id: Optional[EnvID] = None) -> None:
        """Tries to render the sub-environment with the given id or all.

        Args:
            env_id: The sub-environment's ID, if applicable.
                If None, renders the entire Env (i.e. all sub-environments).
        """

        # By default, do nothing.
        pass

    @PublicAPI
    def stop(self) -> None:
        """Releases all resources used."""

        # Try calling `close` on all sub-environments.
        for env in self.get_sub_environments():
            if hasattr(env, "close"):
                env.close()

    @Deprecated(new="get_sub_environments", error=False)
    def get_unwrapped(self) -> List[EnvType]:
        return self.get_sub_environments()

    @PublicAPI
    @property
    def observation_space(self) -> gym.Space:
        """Returns the observation space for each agent.

        Note: samples from the observation space need to be preprocessed into a
            `MultiEnvDict` before being used by a policy.

        Returns:
            The observation space for each environment.
        """
        raise NotImplementedError

    @PublicAPI
    @property
    def action_space(self) -> gym.Space:
        """Returns the action space for each agent.

        Note: samples from the action space need to be preprocessed into a
            `MultiEnvDict` before being passed to `send_actions`.

        Returns:
            The observation space for each environment.
        """
        raise NotImplementedError

    @PublicAPI
    def action_space_sample(self, agent_id: list = None) -> MultiEnvDict:
        """Returns a random action for each environment, and potentially each
            agent in that environment.

        Args:
            agent_id: List of agent ids to sample actions for. If None or empty
                list, sample actions for all agents in the environment.

        Returns:
            A random action for each environment.
        """
        logger.warning("action_space_sample() has not been implemented")
        del agent_id
        return {}

    @PublicAPI
    def observation_space_sample(self, agent_id: list = None) -> MultiEnvDict:
        """Returns a random observation for each environment, and potentially
            each agent in that environment.

        Args:
            agent_id: List of agent ids to sample actions for. If None or empty
                list, sample actions for all agents in the environment.

        Returns:
            A random action for each environment.
        """
        logger.warning("observation_space_sample() has not been implemented")
        del agent_id
        return {}

    @PublicAPI
    def last(
        self,
    ) -> Tuple[MultiEnvDict, MultiEnvDict, MultiEnvDict, MultiEnvDict, MultiEnvDict]:
        """Returns the last observations, rewards, and done flags that were
            returned by the environment.

        Returns:
            The last observations, rewards, and done flags for each environment
        """
        logger.warning("last has not been implemented for this environment.")
        return {}, {}, {}, {}, {}

    @PublicAPI
    def observation_space_contains(self, x: MultiEnvDict) -> bool:
        """Checks if the given observation is valid for each environment.

        Args:
            x: Observations to check.

        Returns:
            True if the observations are contained within their respective
                spaces. False otherwise.
        """
        self._space_contains(self.observation_space, x)

    @PublicAPI
    def action_space_contains(self, x: MultiEnvDict) -> bool:
        """Checks if the given actions is valid for each environment.

        Args:
            x: Actions to check.

        Returns:
            True if the actions are contained within their respective
                spaces. False otherwise.
        """
        return self._space_contains(self.action_space, x)

    def _space_contains(self, space: gym.Space, x: MultiEnvDict) -> bool:
        """Check if the given space contains the observations of x.

        Args:
            space: The space to if x's observations are contained in.
            x: The observations to check.

        Returns:
            True if the observations of x are contained in space.
        """
        agents = set(self.get_agent_ids())
        for multi_agent_dict in x.values():
            for agent_id, obs in multi_agent_dict:
                if (agent_id not in agents) or (not space[agent_id].contains(obs)):
                    return False

        return True


# Fixed agent identifier when there is only the single agent in the env
_DUMMY_AGENT_ID = "agent0"


@Deprecated(new="with_dummy_agent_id", error=False)
def _with_dummy_agent_id(
    env_id_to_values: Dict[EnvID, Any], dummy_id: "AgentID" = _DUMMY_AGENT_ID
) -> MultiEnvDict:
    return {k: {dummy_id: v} for (k, v) in env_id_to_values.items()}


def with_dummy_agent_id(
    env_id_to_values: Dict[EnvID, Any], dummy_id: "AgentID" = _DUMMY_AGENT_ID
) -> MultiEnvDict:
    return {k: {dummy_id: v} for (k, v) in env_id_to_values.items()}


@Deprecated(
    old="ray.rllib.env.base_env._ExternalEnvToBaseEnv",
    new="ray.rllib.env.external.ExternalEnvWrapper",
    error=False,
)
class _ExternalEnvToBaseEnv(BaseEnv):
    """Internal adapter of ExternalEnv to BaseEnv."""

    def __init__(
        self, external_env: "ExternalEnv", preprocessor: "Preprocessor" = None
    ):
        from ray.rllib.env.external_multi_agent_env import ExternalMultiAgentEnv

        self.external_env = external_env
        self.prep = preprocessor
        self.multiagent = issubclass(type(external_env), ExternalMultiAgentEnv)
        self.action_space = external_env.action_space
        if preprocessor:
            self.observation_space = preprocessor.observation_space
        else:
            self.observation_space = external_env.observation_space
        external_env.start()

    @override(BaseEnv)
    def poll(
        self,
    ) -> Tuple[MultiEnvDict, MultiEnvDict, MultiEnvDict, MultiEnvDict, MultiEnvDict]:
        with self.external_env._results_avail_condition:
            results = self._poll()
            while len(results[0]) == 0:
                self.external_env._results_avail_condition.wait()
                results = self._poll()
                if not self.external_env.is_alive():
                    raise Exception("Serving thread has stopped.")
        limit = self.external_env._max_concurrent_episodes
        assert len(results[0]) < limit, (
            "Too many concurrent episodes, were some leaked? This "
            "ExternalEnv was created with max_concurrent={}".format(limit)
        )
        return results

    @override(BaseEnv)
    def send_actions(self, action_dict: MultiEnvDict) -> None:
        if self.multiagent:
            for env_id, actions in action_dict.items():
                self.external_env._episodes[env_id].action_queue.put(actions)
        else:
            for env_id, action in action_dict.items():
                self.external_env._episodes[env_id].action_queue.put(
                    action[_DUMMY_AGENT_ID]
                )

    def _poll(
        self,
    ) -> Tuple[MultiEnvDict, MultiEnvDict, MultiEnvDict, MultiEnvDict, MultiEnvDict]:
        all_obs, all_rewards, all_dones, all_infos = {}, {}, {}, {}
        off_policy_actions = {}
        for eid, episode in self.external_env._episodes.copy().items():
            data = episode.get_data()
            cur_done = (
                episode.cur_done_dict["__all__"]
                if self.multiagent
                else episode.cur_done
            )
            if cur_done:
                del self.external_env._episodes[eid]
            if data:
                if self.prep:
                    all_obs[eid] = self.prep.transform(data["obs"])
                else:
                    all_obs[eid] = data["obs"]
                all_rewards[eid] = data["reward"]
                all_dones[eid] = data["done"]
                all_infos[eid] = data["info"]
                if "off_policy_action" in data:
                    off_policy_actions[eid] = data["off_policy_action"]
        if self.multiagent:
            # Ensure a consistent set of keys
            # rely on all_obs having all possible keys for now.
            for eid, eid_dict in all_obs.items():
                for agent_id in eid_dict.keys():

                    def fix(d, zero_val):
                        if agent_id not in d[eid]:
                            d[eid][agent_id] = zero_val

                    fix(all_rewards, 0.0)
                    fix(all_dones, False)
                    fix(all_infos, {})
            return (all_obs, all_rewards, all_dones, all_infos, off_policy_actions)
        else:
            return (
                _with_dummy_agent_id(all_obs),
                _with_dummy_agent_id(all_rewards),
                _with_dummy_agent_id(all_dones, "__all__"),
                _with_dummy_agent_id(all_infos),
                _with_dummy_agent_id(off_policy_actions),
            )


@Deprecated(
    old="ray.rllib.env.base_env._VectorEnvToBaseEnv",
    new="ray.rllib.env.vector_env.VectorEnvWrapper",
    error=False,
)
class _VectorEnvToBaseEnv(BaseEnv):
    """Internal adapter of VectorEnv to BaseEnv.

    We assume the caller will always send the full vector of actions in each
    call to send_actions(), and that they call reset_at() on all completed
    environments before calling send_actions().
    """

    def __init__(self, vector_env: "VectorEnv"):
        self.vector_env = vector_env
        self.action_space = vector_env.action_space
        self.observation_space = vector_env.observation_space
        self.num_envs = vector_env.num_envs
        self.new_obs = None  # lazily initialized
        self.cur_rewards = [None for _ in range(self.num_envs)]
        self.cur_dones = [False for _ in range(self.num_envs)]
        self.cur_infos = [None for _ in range(self.num_envs)]

    @override(BaseEnv)
    def poll(
        self,
    ) -> Tuple[MultiEnvDict, MultiEnvDict, MultiEnvDict, MultiEnvDict, MultiEnvDict]:
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
            _with_dummy_agent_id(new_obs),
            _with_dummy_agent_id(rewards),
            _with_dummy_agent_id(dones, "__all__"),
            _with_dummy_agent_id(infos),
            {},
        )

    @override(BaseEnv)
    def send_actions(self, action_dict: MultiEnvDict) -> None:
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
    def try_reset(self, env_id: Optional[EnvID] = None) -> MultiAgentDict:
        assert env_id is None or isinstance(env_id, int)
        return {_DUMMY_AGENT_ID: self.vector_env.reset_at(env_id)}

    @override(BaseEnv)
    def get_sub_environments(self) -> List[EnvType]:
        return self.vector_env.get_sub_environments()

    @override(BaseEnv)
    def try_render(self, env_id: Optional[EnvID] = None) -> None:
        assert env_id is None or isinstance(env_id, int)
        return self.vector_env.try_render_at(env_id)


@Deprecated(
    old="ray.rllib.env.base_env._MultiAgentEnvToBaseEnv",
    new="ray.rllib.env.multi_agent_env.MultiAgentEnvWrapper",
    error=False,
)
class _MultiAgentEnvToBaseEnv(BaseEnv):
    """Internal adapter of MultiAgentEnv to BaseEnv.

    This also supports vectorization if num_envs > 1.
    """

    def __init__(
        self,
        make_env: Callable[[int], EnvType],
        existing_envs: "MultiAgentEnv",
        num_envs: int,
    ):
        """Wraps MultiAgentEnv(s) into the BaseEnv API.

        Args:
            make_env (Callable[[int], EnvType]): Factory that produces a new
                MultiAgentEnv intance. Must be defined, if the number of
                existing envs is less than num_envs.
            existing_envs (List[MultiAgentEnv]): List of already existing
                multi-agent envs.
            num_envs (int): Desired num multiagent envs to have at the end in
                total. This will include the given (already created)
                `existing_envs`.
        """
        from ray.rllib.env.multi_agent_env import MultiAgentEnv

        self.make_env = make_env
        self.envs = existing_envs
        self.num_envs = num_envs
        self.dones = set()
        while len(self.envs) < self.num_envs:
            self.envs.append(self.make_env(len(self.envs)))
        for env in self.envs:
            assert isinstance(env, MultiAgentEnv)
        self.env_states = [_MultiAgentEnvState(env) for env in self.envs]

    @override(BaseEnv)
    def poll(
        self,
    ) -> Tuple[MultiEnvDict, MultiEnvDict, MultiEnvDict, MultiEnvDict, MultiEnvDict]:
        obs, rewards, dones, infos = {}, {}, {}, {}
        for i, env_state in enumerate(self.env_states):
            obs[i], rewards[i], dones[i], infos[i] = env_state.poll()
        return obs, rewards, dones, infos, {}

    @override(BaseEnv)
    def send_actions(self, action_dict: MultiEnvDict) -> None:
        for env_id, agent_dict in action_dict.items():
            if env_id in self.dones:
                raise ValueError("Env {} is already done".format(env_id))
            env = self.envs[env_id]
            obs, rewards, dones, infos = env.step(agent_dict)
            assert isinstance(obs, dict), "Not a multi-agent obs"
            assert isinstance(rewards, dict), "Not a multi-agent reward"
            assert isinstance(dones, dict), "Not a multi-agent return"
            assert isinstance(infos, dict), "Not a multi-agent info"
            # Allow `__common__` entry in `infos` for data unrelated with any
            # agent, but rather with the environment itself.
            if set(infos).difference(set(obs) | {"__common__"}):
                raise ValueError(
                    "Key set for infos must be a subset of obs: "
                    "{} vs {}".format(infos.keys(), obs.keys())
                )
            if "__all__" not in dones:
                raise ValueError(
                    "In multi-agent environments, '__all__': True|False must "
                    "be included in the 'done' dict: got {}.".format(dones)
                )
            if dones["__all__"]:
                self.dones.add(env_id)
            self.env_states[env_id].observe(obs, rewards, dones, infos)

    @override(BaseEnv)
    def try_reset(self, env_id: Optional[EnvID] = None) -> Optional[MultiAgentDict]:
        obs = self.env_states[env_id].reset()
        assert isinstance(obs, dict), "Not a multi-agent obs"
        if obs is not None and env_id in self.dones:
            self.dones.remove(env_id)
        return obs

    @override(BaseEnv)
    def get_sub_environments(self) -> List[EnvType]:
        return [state.env for state in self.env_states]

    @override(BaseEnv)
    def try_render(self, env_id: Optional[EnvID] = None) -> None:
        if env_id is None:
            env_id = 0
        assert isinstance(env_id, int)
        return self.envs[env_id].render()


@Deprecated(
    old="ray.rllib.env.base_env._MultiAgentEnvState",
    new="ray.rllib.env.multi_agent_env._MultiAgentEnvState",
    error=False,
)
class _MultiAgentEnvState:
    def __init__(self, env: "MultiAgentEnv"):
        from ray.rllib.env.multi_agent_env import MultiAgentEnv

        assert isinstance(env, MultiAgentEnv)
        self.env = env
        self.initialized = False

    def poll(
        self,
    ) -> Tuple[MultiAgentDict, MultiAgentDict, MultiAgentDict, MultiAgentDict]:
        if not self.initialized:
            self.reset()
            self.initialized = True

        observations = self.last_obs
        rewards = {}
        dones = {"__all__": self.last_dones["__all__"]}
        infos = {"__common__": self.last_infos.get("__common__")}

        # If episode is done, release everything we have.
        if dones["__all__"]:
            rewards = self.last_rewards
            self.last_rewards = {}
            dones = self.last_dones
            self.last_dones = {}
            self.last_obs = {}
            infos = self.last_infos
            self.last_infos = {}
        # Only release those agents' rewards/dones/infos, whose
        # observations we have.
        else:
            for ag in observations.keys():
                if ag in self.last_rewards:
                    rewards[ag] = self.last_rewards[ag]
                    del self.last_rewards[ag]
                if ag in self.last_dones:
                    dones[ag] = self.last_dones[ag]
                    del self.last_dones[ag]
                if ag in self.last_infos:
                    infos[ag] = self.last_infos[ag]
                    del self.last_infos[ag]

        self.last_dones["__all__"] = False
        return observations, rewards, dones, infos

    def observe(
        self,
        obs: MultiAgentDict,
        rewards: MultiAgentDict,
        dones: MultiAgentDict,
        infos: MultiAgentDict,
    ):
        self.last_obs = obs
        for ag, r in rewards.items():
            if ag in self.last_rewards:
                self.last_rewards[ag] += r
            else:
                self.last_rewards[ag] = r
        for ag, d in dones.items():
            if ag in self.last_dones:
                self.last_dones[ag] = self.last_dones[ag] or d
            else:
                self.last_dones[ag] = d
        self.last_infos = infos

    def reset(self) -> MultiAgentDict:
        self.last_obs = self.env.reset()
        self.last_rewards = {}
        self.last_dones = {"__all__": False}
        self.last_infos = {"__common__": {}}
        return self.last_obs


def convert_to_base_env(
    env: EnvType,
    make_env: Callable[[int], EnvType] = None,
    num_envs: int = 1,
    remote_envs: bool = False,
    remote_env_batch_wait_ms: int = 0,
) -> "BaseEnv":
    """Converts an RLlib-supported env into a BaseEnv object.

    Supported types for the `env` arg are gym.Env, BaseEnv,
    VectorEnv, MultiAgentEnv, ExternalEnv, or ExternalMultiAgentEnv.

    The resulting BaseEnv is always vectorized (contains n
    sub-environments) to support batched forward passes, where n may also
    be 1. BaseEnv also supports async execution via the `poll` and
    `send_actions` methods and thus supports external simulators.

    TODO: Support gym3 environments, which are already vectorized.

    Args:
        env: An already existing environment of any supported env type
            to convert/wrap into a BaseEnv. Supported types are gym.Env,
            BaseEnv, VectorEnv, MultiAgentEnv, ExternalEnv, and
            ExternalMultiAgentEnv.
        make_env: A callable taking an int as input (which indicates the
            number of individual sub-environments within the final
            vectorized BaseEnv) and returning one individual
            sub-environment.
        num_envs: The number of sub-environments to create in the
            resulting (vectorized) BaseEnv. The already existing `env`
            will be one of the `num_envs`.
        remote_envs: Whether each sub-env should be a @ray.remote actor.
            You can set this behavior in your config via the
            `remote_worker_envs=True` option.
        remote_env_batch_wait_ms: The wait time (in ms) to poll remote
            sub-environments for, if applicable. Only used if
            `remote_envs` is True.

    Returns:
        The resulting BaseEnv object.
    """

    from ray.rllib.env.remote_base_env import RemoteBaseEnv
    from ray.rllib.env.external_env import ExternalEnv
    from ray.rllib.env.multi_agent_env import MultiAgentEnv
    from ray.rllib.env.vector_env import VectorEnv, VectorEnvWrapper

    if remote_envs and num_envs == 1:
        raise ValueError(
            "Remote envs only make sense to use if num_envs > 1 "
            "(i.e. vectorization is enabled)."
        )

    # Given `env` is already a BaseEnv -> Return as is.
    if isinstance(env, (BaseEnv, MultiAgentEnv, VectorEnv, ExternalEnv)):
        return env.to_base_env(
            make_env=make_env,
            num_envs=num_envs,
            remote_envs=remote_envs,
            remote_env_batch_wait_ms=remote_env_batch_wait_ms,
        )
    # `env` is not a BaseEnv yet -> Need to convert/vectorize.
    else:
        # Sub-environments are ray.remote actors:
        if remote_envs:
            # Determine, whether the already existing sub-env (could
            # be a ray.actor) is multi-agent or not.
            multiagent = (
                ray.get(env._is_multi_agent.remote())
                if hasattr(env, "_is_multi_agent")
                else False
            )
            env = RemoteBaseEnv(
                make_env,
                num_envs,
                multiagent=multiagent,
                remote_env_batch_wait_ms=remote_env_batch_wait_ms,
                existing_envs=[env],
            )
        # Sub-environments are not ray.remote actors.
        else:
            # Convert gym.Env to VectorEnv ...
            env = VectorEnv.vectorize_gym_envs(
                make_env=make_env,
                existing_envs=[env],
                num_envs=num_envs,
                action_space=env.action_space,
                observation_space=env.observation_space,
            )
            # ... then the resulting VectorEnv to a BaseEnv.
            env = VectorEnvWrapper(env)

    # Make sure conversion went well.
    assert isinstance(env, BaseEnv), env

    return env
