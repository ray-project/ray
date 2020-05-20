import uuid

from ray.rllib.utils.annotations import override, PublicAPI
from ray.rllib.env.external_multi_agent_env import ExternalMultiAgentEnv


# TODO: (sven): Make this the only ExternalEnv at some point for simplicity
#  reasons.
@PublicAPI
class ExternalMultiAgentVectorEnv(ExternalMultiAgentEnv):
    """A multi-agent, vectorized, external Environment."""

    @PublicAPI
    def __init__(self, action_space, observation_space, num_envs=1):
        """Initializes a multi-agent vectorized external env.

        Args:
            action_space (gym.Space): Action space of the env.
            observation_space (gym.Space): Observation space of the env.
            num_envs (int): Number of parallel (vectorized) sub-envs. Use 1
                for no vectorization.
        """
        ExternalMultiAgentEnv.__init__(self, action_space, observation_space)
        self.num_envs = num_envs

    @PublicAPI
    def run(self):
        """Override this to implement the multi-agent run loop.

        Your loop should continuously:
            1. Call self.start_episode(episode_id)
            2. Call self.get_action(episode_id, obs_dict)
                    -or-
                    self.log_action(episode_id, obs_dict, action_dict)
            3. Call self.log_returns(episode_id, reward_dict)
            4. Call self.end_episode(episode_id, obs_dict)
            5. Wait if nothing to do.

        Multiple episodes may be started at the same time.
        """
        raise NotImplementedError

    @PublicAPI
    @override(ExternalMultiAgentEnv)
    def start_episode(self, vector_episode_id=None, training_enabled=True):
        """Starts a vectorized episode (after a `vector_reset` on VectorEnv).

        Args:
            vector_episode_id (str): The unique ID of a VectorEpisode object.
            training_enabled (bool): Whether training on this episode should
                be enabled or not. The "training_enabled" flag is sent within
                the "info" field of a message and allows for switching on/off
                individual Agents' training.
        """

        if vector_episode_id is None:
            vector_episode_id = uuid.uuid4().hex

        if vector_episode_id in self._finished:
            raise ValueError(
                "Episode {} has already completed.".format(vector_episode_id))

        if vector_episode_id in self._vector_episodes:
            raise ValueError(
                "Episode {} is already started".format(vector_episode_id))

        self._vector_episodes[vector_episode_id] = MultiAgentVectorEpisode(
            vector_episode_id,
            self._results_avail_condition,
            training_enabled,
            num_envs=self.num_envs)

        return vector_episode_id

    @PublicAPI
    def get_action(self, vector_episode_id, observation_dict):
        """Record an observation and get the on-policy action.

        `observation_dict` is expected to contain the observations
        of all agents acting in this MultiAgentVectorEpisode step.
        Two key-levels: 1) vector index 2) agent.

        Arguments:
            vector_episode_id (str): Episode ID for the MultiAgentVectorEpisode
                returned from `start_episode()`.
            observation_dict (dict): Current environment observations.

        Returns:
            action (dict): Actions from the env action space.
        """

        vector_episode = self._get(vector_episode_id)
        return vector_episode.wait_for_action(observation_dict)

    @PublicAPI
    @override(ExternalMultiAgentEnv)
    def log_action(self, vector_episode_id, observation_dict, action_dict):
        """Record an observation and (off-policy) action taken.

        Arguments:
            vector_episode_id (str): Episode id returned from start_episode().
            observation_dict (dict): Current environment observation.
            action_dict (dict): Action for the observation.
        """

        vector_episode = self._get(vector_episode_id)
        vector_episode.log_action(observation_dict, action_dict)

    @PublicAPI
    @override(ExternalMultiAgentEnv)
    def log_returns(self,
                    vector_episode_id,
                    reward_dict,
                    info_dict=None,
                    multiagent_done_dict=None):
        """Record returns from the environment.

        The reward will be attributed to the previous action taken by the
        episode. Rewards accumulate until the next action. If no reward is
        logged before the next action, a reward of 0.0 is assumed.

        Arguments:
            episode_id (str): Episode id returned from start_episode().
            reward_dict (dict): Reward from the environment agents.
            info_dict (dict): Optional info dict.
            multiagent_done_dict (dict): Optional done dict for agents.
        """

        vector_episode = self._get(vector_episode_id)

        # accumulate reward by agent
        # for existing agents, we want to add the reward up
        for agent, rew in reward_dict.items():
            if agent in vector_episode.cur_reward_dict:
                vector_episode.cur_reward_dict[agent] += rew
            else:
                vector_episode.cur_reward_dict[agent] = rew

        if multiagent_done_dict:
            for agent, done in multiagent_done_dict.items():
                if agent in vector_episode.cur_done_dict:
                    vector_episode.cur_done_dict[agent] = done
                else:
                    vector_episode.cur_done_dict[agent] = done

        if info_dict:
            vector_episode.cur_info_dict = info_dict or {}

    @PublicAPI
    @override(ExternalMultiAgentEnv)
    def end_episode(self, vector_episode_id, observation_dict):
        """Record the end of an episode.

        Arguments:
            episode_id (str): Episode id returned from start_episode().
            observation_dict (dict): Current environment observation.
        """

        vector_episode = self._get(vector_episode_id)
        self._finished.add(vector_episode.episode_id)
        vector_episode.done(observation_dict)


class MultiAgentVectorEpisode:
    """Tracked state for a multi-agent vector episode.
    
    A Multi-Agent Vector Episode represents a "rollout" in between two
    vector_reset() calls, in which all (individual) episodes in the underlying
    VectorEnv are reset. Single `done`s for sub-episodes should be handled
    with reset_at()"""

    def __init__(self,
                 episode_id,
                 results_avail_condition,
                 training_enabled,
                 multiagent=False,
                 num_envs=1):
        assert not (multiagent and (num_envs > 1)), \
            "Multiagent AND vectorized (num_envs > 1) not supported yet in " \
            "_ExternalEnvEpisode!"

        self.episode_id = episode_id
        self.results_avail_condition = results_avail_condition
        self.training_enabled = training_enabled
        self.multiagent = multiagent
        self.num_envs = num_envs
        self.data_queue = queue.Queue()
        self.action_queue = queue.Queue()
        if multiagent:
            self.new_observations = None
            self.new_actions = None
            self.cur_rewards = {}
            self.cur_dones = {"__all__": False}
            self.cur_infos = {}
        elif self.num_envs > 1:
            self.new_observations = None
            self.new_actions = None
            self.cur_rewards = [0.0 for _ in range(self.num_envs)]
            self.cur_dones = [False for _ in range(self.num_envs)]
            self.cur_infos = {}
        else:
            self.new_observations = None
            self.new_actions = None
            self.cur_rewards = 0.0
            self.cur_dones = False
            self.cur_infos = {}

    def get_data(self):
        if self.data_queue.empty():
            return None
        return self.data_queue.get_nowait()

    def log_action(self, observation, action):
        self.new_observations = observation
        self.new_actions = action
        self._send()
        self.action_queue.get(True, timeout=1000000.0)

    def wait_for_action(self, observation):
        self.new_observations = observation
        self._send()
        return self.action_queue.get(True, timeout=1000000.0)

    def done(self, observation):
        self.new_observation = observation
        if self.multiagent:
            self.cur_dones = {"__all__": True}
        elif self.num_envs > 1:
            self.cur_dones = [True for _ in range(self.num_envs)]
        else:
            self.cur_dones = True
        self._send()

    def _send(self):
        if self.multiagent:
            if not self.training_enabled:
                for agent_id in self.cur_infos:
                    self.cur_infos[agent_id]["training_enabled"] = False

        item = {
            "obs": self.new_observations,
            "reward": self.cur_rewards,
            "done": self.cur_dones,
            "info": self.cur_infos,
        }
        if self.new_actions is not None:
            item["off_policy_action"] = self.new_actions
        self.new_observations = None
        self.new_actions = None
        if self.multiagent:
            self.cur_rewards = {}
        else:
            if not self.training_enabled:
                item["info"]["training_enabled"] = False
            if self.num_envs > 1:
                self.cur_rewards = [0.0 for _ in range(self.num_envs)]
            else:
                self.cur_rewards = 0.0

        with self.results_avail_condition:
            self.data_queue.put_nowait(item)
            self.results_avail_condition.notify()
