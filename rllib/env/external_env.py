from six.moves import queue
import gym
import threading
import uuid
from typing import Callable, Tuple, Optional, TYPE_CHECKING

from ray.rllib.env.base_env import BaseEnv
from ray.rllib.utils.annotations import override, PublicAPI
from ray.rllib.utils.typing import (
    EnvActionType,
    EnvInfoDict,
    EnvObsType,
    EnvType,
    MultiEnvDict,
)

if TYPE_CHECKING:
    from ray.rllib.models.preprocessors import Preprocessor


@PublicAPI
class ExternalEnv(threading.Thread):
    """An environment that interfaces with external agents.

    Unlike simulator envs, control is inverted: The environment queries the
    policy to obtain actions and in return logs observations and rewards for
    training. This is in contrast to gym.Env, where the algorithm drives the
    simulation through env.step() calls.

    You can use ExternalEnv as the backend for policy serving (by serving HTTP
    requests in the run loop), for ingesting offline logs data (by reading
    offline transitions in the run loop), or other custom use cases not easily
    expressed through gym.Env.

    ExternalEnv supports both on-policy actions (through self.get_action()),
    and off-policy actions (through self.log_action()).

    This env is thread-safe, but individual episodes must be executed serially.

    Examples:
        >>> from ray.tune import register_env
        >>> from ray.rllib.agents.dqn import DQNTrainer # doctest: +SKIP
        >>> YourExternalEnv = ... # doctest: +SKIP
        >>> register_env("my_env", # doctest: +SKIP
        ...     lambda config: YourExternalEnv(config))
        >>> trainer = DQNTrainer(env="my_env") # doctest: +SKIP
        >>> while True: # doctest: +SKIP
        >>>     print(trainer.train()) # doctest: +SKIP
    """

    @PublicAPI
    def __init__(
        self,
        action_space: gym.Space,
        observation_space: gym.Space,
        max_concurrent: int = 100,
    ):
        """Initializes an ExternalEnv instance.

        Args:
            action_space: Action space of the env.
            observation_space: Observation space of the env.
            max_concurrent: Max number of active episodes to allow at
                once. Exceeding this limit raises an error.
        """

        threading.Thread.__init__(self)

        self.daemon = True
        self.action_space = action_space
        self.observation_space = observation_space
        self._episodes = {}
        self._finished = set()
        self._results_avail_condition = threading.Condition()
        self._max_concurrent_episodes = max_concurrent

    @PublicAPI
    def run(self):
        """Override this to implement the run loop.

        Your loop should continuously:
            1. Call self.start_episode(episode_id)
            2. Call self.[get|log]_action(episode_id, obs, [action]?)
            3. Call self.log_returns(episode_id, reward)
            4. Call self.end_episode(episode_id, obs)
            5. Wait if nothing to do.

        Multiple episodes may be started at the same time.
        """
        raise NotImplementedError

    @PublicAPI
    def start_episode(
        self, episode_id: Optional[str] = None, training_enabled: bool = True
    ) -> str:
        """Record the start of an episode.

        Args:
            episode_id: Unique string id for the episode or
                None for it to be auto-assigned and returned.
            training_enabled: Whether to use experiences for this
                episode to improve the policy.

        Returns:
            Unique string id for the episode.
        """

        if episode_id is None:
            episode_id = uuid.uuid4().hex

        if episode_id in self._finished:
            raise ValueError("Episode {} has already completed.".format(episode_id))

        if episode_id in self._episodes:
            raise ValueError("Episode {} is already started".format(episode_id))

        self._episodes[episode_id] = _ExternalEnvEpisode(
            episode_id, self._results_avail_condition, training_enabled
        )

        return episode_id

    @PublicAPI
    def get_action(self, episode_id: str, observation: EnvObsType) -> EnvActionType:
        """Record an observation and get the on-policy action.

        Args:
            episode_id: Episode id returned from start_episode().
            observation: Current environment observation.

        Returns:
            Action from the env action space.
        """

        episode = self._get(episode_id)
        return episode.wait_for_action(observation)

    @PublicAPI
    def log_action(
        self, episode_id: str, observation: EnvObsType, action: EnvActionType
    ) -> None:
        """Record an observation and (off-policy) action taken.

        Args:
            episode_id: Episode id returned from start_episode().
            observation: Current environment observation.
            action: Action for the observation.
        """

        episode = self._get(episode_id)
        episode.log_action(observation, action)

    @PublicAPI
    def log_returns(
        self, episode_id: str, reward: float, info: Optional[EnvInfoDict] = None
    ) -> None:
        """Records returns (rewards and infos) from the environment.

        The reward will be attributed to the previous action taken by the
        episode. Rewards accumulate until the next action. If no reward is
        logged before the next action, a reward of 0.0 is assumed.

        Args:
            episode_id: Episode id returned from start_episode().
            reward: Reward from the environment.
            info: Optional info dict.
        """

        episode = self._get(episode_id)
        episode.cur_reward += reward

        if info:
            episode.cur_info = info or {}

    @PublicAPI
    def end_episode(self, episode_id: str, observation: EnvObsType) -> None:
        """Records the end of an episode.

        Args:
            episode_id: Episode id returned from start_episode().
            observation: Current environment observation.
        """

        episode = self._get(episode_id)
        self._finished.add(episode.episode_id)
        episode.done(observation)

    def _get(self, episode_id: str) -> "_ExternalEnvEpisode":
        """Get a started episode by its ID or raise an error."""

        if episode_id in self._finished:
            raise ValueError("Episode {} has already completed.".format(episode_id))

        if episode_id not in self._episodes:
            raise ValueError("Episode {} not found.".format(episode_id))

        return self._episodes[episode_id]

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
        if num_envs != 1:
            raise ValueError(
                "External(MultiAgent)Env does not currently support "
                "num_envs > 1. One way of solving this would be to "
                "treat your Env as a MultiAgentEnv hosting only one "
                "type of agent but with several copies."
            )
        env = ExternalEnvWrapper(self)

        return env


class _ExternalEnvEpisode:
    """Tracked state for each active episode."""

    def __init__(
        self,
        episode_id: str,
        results_avail_condition: threading.Condition,
        training_enabled: bool,
        multiagent: bool = False,
    ):
        self.episode_id = episode_id
        self.results_avail_condition = results_avail_condition
        self.training_enabled = training_enabled
        self.multiagent = multiagent
        self.data_queue = queue.Queue()
        self.action_queue = queue.Queue()
        if multiagent:
            self.new_observation_dict = None
            self.new_action_dict = None
            self.cur_reward_dict = {}
            self.cur_done_dict = {"__all__": False}
            self.cur_info_dict = {}
        else:
            self.new_observation = None
            self.new_action = None
            self.cur_reward = 0.0
            self.cur_done = False
            self.cur_info = {}

    def get_data(self):
        if self.data_queue.empty():
            return None
        return self.data_queue.get_nowait()

    def log_action(self, observation, action):
        if self.multiagent:
            self.new_observation_dict = observation
            self.new_action_dict = action
        else:
            self.new_observation = observation
            self.new_action = action
        self._send()
        self.action_queue.get(True, timeout=60.0)

    def wait_for_action(self, observation):
        if self.multiagent:
            self.new_observation_dict = observation
        else:
            self.new_observation = observation
        self._send()
        return self.action_queue.get(True, timeout=300.0)

    def done(self, observation):
        if self.multiagent:
            self.new_observation_dict = observation
            self.cur_done_dict = {"__all__": True}
        else:
            self.new_observation = observation
            self.cur_done = True
        self._send()

    def _send(self):
        if self.multiagent:
            if not self.training_enabled:
                for agent_id in self.cur_info_dict:
                    self.cur_info_dict[agent_id]["training_enabled"] = False
            item = {
                "obs": self.new_observation_dict,
                "reward": self.cur_reward_dict,
                "done": self.cur_done_dict,
                "info": self.cur_info_dict,
            }
            if self.new_action_dict is not None:
                item["off_policy_action"] = self.new_action_dict
            self.new_observation_dict = None
            self.new_action_dict = None
            self.cur_reward_dict = {}
        else:
            item = {
                "obs": self.new_observation,
                "reward": self.cur_reward,
                "done": self.cur_done,
                "info": self.cur_info,
            }
            if self.new_action is not None:
                item["off_policy_action"] = self.new_action
            self.new_observation = None
            self.new_action = None
            self.cur_reward = 0.0
            if not self.training_enabled:
                item["info"]["training_enabled"] = False

        with self.results_avail_condition:
            self.data_queue.put_nowait(item)
            self.results_avail_condition.notify()


class ExternalEnvWrapper(BaseEnv):
    """Internal adapter of ExternalEnv to BaseEnv."""

    def __init__(
        self, external_env: "ExternalEnv", preprocessor: "Preprocessor" = None
    ):
        from ray.rllib.env.external_multi_agent_env import ExternalMultiAgentEnv

        self.external_env = external_env
        self.prep = preprocessor
        self.multiagent = issubclass(type(external_env), ExternalMultiAgentEnv)
        self._action_space = external_env.action_space
        if preprocessor:
            self._observation_space = preprocessor.observation_space
        else:
            self._observation_space = external_env.observation_space
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
        from ray.rllib.env.base_env import _DUMMY_AGENT_ID

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
        from ray.rllib.env.base_env import with_dummy_agent_id

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
                with_dummy_agent_id(all_obs),
                with_dummy_agent_id(all_rewards),
                with_dummy_agent_id(all_dones, "__all__"),
                with_dummy_agent_id(all_infos),
                with_dummy_agent_id(off_policy_actions),
            )

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
