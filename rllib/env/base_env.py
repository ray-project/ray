from typing import Callable, Tuple, Optional, List, Dict, Any, TYPE_CHECKING

from ray.rllib.env.external_env import ExternalEnv
from ray.rllib.env.external_multi_agent_env import ExternalMultiAgentEnv
from ray.rllib.env.multi_agent_env import MultiAgentEnv
from ray.rllib.env.vector_env import VectorEnv
from ray.rllib.utils.annotations import override, PublicAPI
from ray.rllib.utils.typing import EnvType, MultiEnvDict, EnvID, \
    AgentID, MultiAgentDict

if TYPE_CHECKING:
    from ray.rllib.models.preprocessors import Preprocessor

ASYNC_RESET_RETURN = "async_reset_return"


@PublicAPI
class BaseEnv:
    """The lowest-level env interface used by RLlib for sampling.

    BaseEnv models multiple agents executing asynchronously in multiple
    environments. A call to poll() returns observations from ready agents
    keyed by their environment and agent ids, and actions for those agents
    can be sent back via send_actions().

    All other env types can be adapted to BaseEnv. RLlib handles these
    conversions internally in RolloutWorker, for example:

        gym.Env => rllib.VectorEnv => rllib.BaseEnv
        rllib.MultiAgentEnv => rllib.BaseEnv
        rllib.ExternalEnv => rllib.BaseEnv

    Attributes:
        action_space (gym.Space): Action space. This must be defined for
            single-agent envs. Multi-agent envs can set this to None.
        observation_space (gym.Space): Observation space. This must be defined
            for single-agent envs. Multi-agent envs can set this to None.

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
        >>> env.send_actions(
            actions={
                "env_0": {
                    "car_0": 0,
                    "car_1": 1,
                }, ...
            })
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

    @staticmethod
    def to_base_env(env: EnvType,
                    make_env: Callable[[int], EnvType] = None,
                    num_envs: int = 1,
                    remote_envs: bool = False,
                    remote_env_batch_wait_ms: int = 0) -> "BaseEnv":
        """Wraps any env type as needed to expose the async interface."""

        from ray.rllib.env.remote_vector_env import RemoteVectorEnv
        if remote_envs and num_envs == 1:
            raise ValueError(
                "Remote envs only make sense to use if num_envs > 1 "
                "(i.e. vectorization is enabled).")

        if not isinstance(env, BaseEnv):
            if isinstance(env, MultiAgentEnv):
                if remote_envs:
                    env = RemoteVectorEnv(
                        make_env,
                        num_envs,
                        multiagent=True,
                        remote_env_batch_wait_ms=remote_env_batch_wait_ms)
                else:
                    env = _MultiAgentEnvToBaseEnv(
                        make_env=make_env,
                        existing_envs=[env],
                        num_envs=num_envs)
            elif isinstance(env, ExternalEnv):
                if num_envs != 1:
                    raise ValueError(
                        "External(MultiAgent)Env does not currently support "
                        "num_envs > 1. One way of solving this would be to "
                        "treat your Env as a MultiAgentEnv hosting only one "
                        "type of agent but with several copies.")
                env = _ExternalEnvToBaseEnv(env)
            elif isinstance(env, VectorEnv):
                env = _VectorEnvToBaseEnv(env)
            else:
                if remote_envs:
                    env = RemoteVectorEnv(
                        make_env,
                        num_envs,
                        multiagent=False,
                        remote_env_batch_wait_ms=remote_env_batch_wait_ms)
                else:
                    env = VectorEnv.wrap(
                        make_env=make_env,
                        existing_envs=[env],
                        num_envs=num_envs,
                        action_space=env.action_space,
                        observation_space=env.observation_space)
                    env = _VectorEnvToBaseEnv(env)
        assert isinstance(env, BaseEnv), env
        return env

    @PublicAPI
    def poll(self) -> Tuple[MultiEnvDict, MultiEnvDict, MultiEnvDict,
                            MultiEnvDict, MultiEnvDict]:
        """Returns observations from ready agents.

        The returns are two-level dicts mapping from env_id to a dict of
        agent_id to values. The number of agents and envs can vary over time.

        Returns
        -------
            obs (dict): New observations for each ready agent.
            rewards (dict): Reward values for each ready agent. If the
                episode is just started, the value will be None.
            dones (dict): Done values for each ready agent. The special key
                "__all__" is used to indicate env termination.
            infos (dict): Info values for each ready agent.
            off_policy_actions (dict): Agents may take off-policy actions. When
                that happens, there will be an entry in this dict that contains
                the taken action. There is no need to send_actions() for agents
                that have already chosen off-policy actions.

        """
        raise NotImplementedError

    @PublicAPI
    def send_actions(self, action_dict: MultiEnvDict) -> None:
        """Called to send actions back to running agents in this env.

        Actions should be sent for each ready agent that returned observations
        in the previous poll() call.

        Args:
            action_dict (dict): Actions values keyed by env_id and agent_id.
        """
        raise NotImplementedError

    @PublicAPI
    def try_reset(self,
                  env_id: Optional[EnvID] = None) -> Optional[MultiAgentDict]:
        """Attempt to reset the sub-env with the given id or all sub-envs.

        If the environment does not support synchronous reset, None can be
        returned here.

        Args:
            env_id (Optional[int]): The sub-env ID if applicable. If None,
                reset the entire Env (i.e. all sub-envs).

        Returns:
            Optional[MultiAgentDict]: Resetted (multi-agent) observation dict
                or None if reset is not supported.
        """
        return None

    @PublicAPI
    def get_unwrapped(self) -> List[EnvType]:
        """Return a reference to the underlying gym envs, if any.

        Returns:
            envs (list): Underlying gym envs or [].
        """
        return []

    @PublicAPI
    def stop(self) -> None:
        """Releases all resources used."""

        for env in self.get_unwrapped():
            if hasattr(env, "close"):
                env.close()


# Fixed agent identifier when there is only the single agent in the env
_DUMMY_AGENT_ID = "agent0"


def _with_dummy_agent_id(env_id_to_values: Dict[EnvID, Any],
                         dummy_id: "AgentID" = _DUMMY_AGENT_ID
                         ) -> MultiEnvDict:
    return {k: {dummy_id: v} for (k, v) in env_id_to_values.items()}


class _ExternalEnvToBaseEnv(BaseEnv):
    """Internal adapter of ExternalEnv to BaseEnv."""

    def __init__(self,
                 external_env: ExternalEnv,
                 preprocessor: "Preprocessor" = None):
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
    def poll(self) -> Tuple[MultiEnvDict, MultiEnvDict, MultiEnvDict,
                            MultiEnvDict, MultiEnvDict]:
        with self.external_env._results_avail_condition:
            results = self._poll()
            while len(results[0]) == 0:
                self.external_env._results_avail_condition.wait()
                results = self._poll()
                if not self.external_env.isAlive():
                    raise Exception("Serving thread has stopped.")
        limit = self.external_env._max_concurrent_episodes
        assert len(results[0]) < limit, \
            ("Too many concurrent episodes, were some leaked? This "
             "ExternalEnv was created with max_concurrent={}".format(limit))
        return results

    @override(BaseEnv)
    def send_actions(self, action_dict: MultiEnvDict) -> None:
        if self.multiagent:
            for env_id, actions in action_dict.items():
                self.external_env._episodes[env_id].action_queue.put(actions)
        else:
            for env_id, action in action_dict.items():
                self.external_env._episodes[env_id].action_queue.put(
                    action[_DUMMY_AGENT_ID])

    def _poll(self) -> Tuple[MultiEnvDict, MultiEnvDict, MultiEnvDict,
                             MultiEnvDict, MultiEnvDict]:
        all_obs, all_rewards, all_dones, all_infos = {}, {}, {}, {}
        off_policy_actions = {}
        for eid, episode in self.external_env._episodes.copy().items():
            data = episode.get_data()
            cur_done = episode.cur_done_dict[
                "__all__"] if self.multiagent else episode.cur_done
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
            return (all_obs, all_rewards, all_dones, all_infos,
                    off_policy_actions)
        else:
            return _with_dummy_agent_id(all_obs), \
                _with_dummy_agent_id(all_rewards), \
                _with_dummy_agent_id(all_dones, "__all__"), \
                _with_dummy_agent_id(all_infos), \
                _with_dummy_agent_id(off_policy_actions)


class _VectorEnvToBaseEnv(BaseEnv):
    """Internal adapter of VectorEnv to BaseEnv.

    We assume the caller will always send the full vector of actions in each
    call to send_actions(), and that they call reset_at() on all completed
    environments before calling send_actions().
    """

    def __init__(self, vector_env: VectorEnv):
        self.vector_env = vector_env
        self.action_space = vector_env.action_space
        self.observation_space = vector_env.observation_space
        self.num_envs = vector_env.num_envs
        self.new_obs = None  # lazily initialized
        self.cur_rewards = [None for _ in range(self.num_envs)]
        self.cur_dones = [False for _ in range(self.num_envs)]
        self.cur_infos = [None for _ in range(self.num_envs)]

    @override(BaseEnv)
    def poll(self) -> Tuple[MultiEnvDict, MultiEnvDict, MultiEnvDict,
                            MultiEnvDict, MultiEnvDict]:
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
        return _with_dummy_agent_id(new_obs), \
            _with_dummy_agent_id(rewards), \
            _with_dummy_agent_id(dones, "__all__"), \
            _with_dummy_agent_id(infos), {}

    @override(BaseEnv)
    def send_actions(self, action_dict: MultiEnvDict) -> None:
        action_vector = [None] * self.num_envs
        for i in range(self.num_envs):
            action_vector[i] = action_dict[i][_DUMMY_AGENT_ID]
        self.new_obs, self.cur_rewards, self.cur_dones, self.cur_infos = \
            self.vector_env.vector_step(action_vector)

    @override(BaseEnv)
    def try_reset(self,
                  env_id: Optional[EnvID] = None) -> Optional[MultiAgentDict]:
        return {_DUMMY_AGENT_ID: self.vector_env.reset_at(env_id)}

    @override(BaseEnv)
    def get_unwrapped(self) -> List[EnvType]:
        return self.vector_env.get_unwrapped()


class _MultiAgentEnvToBaseEnv(BaseEnv):
    """Internal adapter of MultiAgentEnv to BaseEnv.

    This also supports vectorization if num_envs > 1.
    """

    def __init__(self, make_env: Callable[[int], EnvType],
                 existing_envs: List[MultiAgentEnv], num_envs: int):
        """Wrap existing multi-agent envs.

        Args:
            make_env (func|None): Factory that produces a new multiagent env.
                Must be defined if the number of existing envs is less than
                num_envs.
            existing_envs (list): List of existing multiagent envs.
            num_envs (int): Desired num multiagent envs to keep total.
        """
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
    def poll(self) -> Tuple[MultiEnvDict, MultiEnvDict, MultiEnvDict,
                            MultiEnvDict, MultiEnvDict]:
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
            if set(obs.keys()) != set(rewards.keys()):
                raise ValueError(
                    "Key set for obs and rewards must be the same: "
                    "{} vs {}".format(obs.keys(), rewards.keys()))
            if set(infos).difference(set(obs)):
                raise ValueError("Key set for infos must be a subset of obs: "
                                 "{} vs {}".format(infos.keys(), obs.keys()))
            if "__all__" not in dones:
                raise ValueError(
                    "In multi-agent environments, '__all__': True|False must "
                    "be included in the 'done' dict: got {}.".format(dones))
            if dones["__all__"]:
                self.dones.add(env_id)
            self.env_states[env_id].observe(obs, rewards, dones, infos)

    @override(BaseEnv)
    def try_reset(self,
                  env_id: Optional[EnvID] = None) -> Optional[MultiAgentDict]:
        obs = self.env_states[env_id].reset()
        assert isinstance(obs, dict), "Not a multi-agent obs"
        if obs is not None and env_id in self.dones:
            self.dones.remove(env_id)
        return obs

    @override(BaseEnv)
    def get_unwrapped(self) -> List[EnvType]:
        return [state.env for state in self.env_states]


class _MultiAgentEnvState:
    def __init__(self, env: MultiAgentEnv):
        assert isinstance(env, MultiAgentEnv)
        self.env = env
        self.initialized = False

    def poll(self) -> Tuple[MultiAgentDict, MultiAgentDict, MultiAgentDict,
                            MultiAgentDict, MultiAgentDict]:
        if not self.initialized:
            self.reset()
            self.initialized = True
        obs, rew, dones, info = (self.last_obs, self.last_rewards,
                                 self.last_dones, self.last_infos)
        self.last_obs = {}
        self.last_rewards = {}
        self.last_dones = {"__all__": False}
        self.last_infos = {}
        return obs, rew, dones, info

    def observe(self, obs: MultiAgentDict, rewards: MultiAgentDict,
                dones: MultiAgentDict, infos: MultiAgentDict):
        self.last_obs = obs
        self.last_rewards = rewards
        self.last_dones = dones
        self.last_infos = infos

    def reset(self) -> MultiAgentDict:
        self.last_obs = self.env.reset()
        self.last_rewards = {
            agent_id: None
            for agent_id in self.last_obs.keys()
        }
        self.last_dones = {
            agent_id: False
            for agent_id in self.last_obs.keys()
        }
        self.last_infos = {agent_id: {} for agent_id in self.last_obs.keys()}
        self.last_dones["__all__"] = False
        return self.last_obs
