from six.moves import queue

from ray.rllib.utils.annotations import override, PublicAPI
from ray.rllib.env.external_multi_agent_env import ExternalMultiAgentEnv
from ray.rllib.evaluation.episode import MultiAgentEpisode


# TODO: (sven): Make this the only ExternalEnv at some point for simplicity
#  reasons.
@PublicAPI
class OBSOLETED_ExternalMultiAgentVectorEnv(ExternalMultiAgentEnv):
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
        self.data_queue = queue.Queue()
        self.action_queue = queue.Queue()

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

    @override(ExternalMultiAgentEnv)
    def start_episode(self, episode_id=None, training_enabled=True):
        if episode_id in self._finished:
            raise ValueError(
                "Episode {} has already completed.".format(episode_id))

        if episode_id in self._episodes:
            raise ValueError(
                "Episode {} is already started".format(episode_id))

        episode = MultiAgentEpisode(None, None, lambda: 0, None, episode_id=episode_id)
        # Update ID, in case it was auto-generated.
        episode_id = episode.episode_id
        self._episodes[episode_id] = episode
        # Return ID.
        return episode_id

    @override(ExternalMultiAgentEnv)
    def get_action(self, episode_id, observation_dict):
        # Loop through envs.
        for env_id, episode in self._episodes.items():
            # Loop through Agents.
            obs = observation_dict[env_id]
            for agent_id in obs.keys():
                episode._set_last_observation(agent_id, obs[agent_id])
        self._send(episode_id)
        return self.action_queue.get(True, timeout=1000000.0) #TODO: change back to 60

    #@override(ExternalMultiAgentEnv)
    #def log_action(self, episode_id, observation_dict, action_dict):
    #    episode = self._get(episode_id)
    #    episode.log_action(observation_dict, action_dict)

    #@PublicAPI
    #@override(ExternalMultiAgentEnv)
    #def log_returns(self,
    #                vector_episode_id,
    #                reward_dict,
    #                info_dict=None,
    #                multiagent_done_dict=None):
    #    """Record returns from the environment.

    #    The reward will be attributed to the previous action taken by the
    #    episode. Rewards accumulate until the next action. If no reward is
    #    logged before the next action, a reward of 0.0 is assumed.

    #    Arguments:
    #        episode_id (str): Episode id returned from start_episode().
    #        reward_dict (dict): Reward from the environment agents.
    #        info_dict (dict): Optional info dict.
    #        multiagent_done_dict (dict): Optional done dict for agents.
    #    """

    #    vector_episode = self._get(vector_episode_id)

    #    # accumulate reward by agent
    #    # for existing agents, we want to add the reward up
    #    for agent, rew in reward_dict.items():
    #        if agent in vector_episode.cur_reward_dict:
    #            vector_episode.cur_reward_dict[agent] += rew
    #        else:
    #            vector_episode.cur_reward_dict[agent] = rew

    #    if multiagent_done_dict:
    #        for agent, done in multiagent_done_dict.items():
    #            if agent in vector_episode.cur_done_dict:
    #                vector_episode.cur_done_dict[agent] = done
    #            else:
    #                vector_episode.cur_done_dict[agent] = done

    #    if info_dict:
    #        vector_episode.cur_info_dict = info_dict or {}

    @PublicAPI
    @override(ExternalMultiAgentEnv)
    def end_episode(self, episode_id, observation_dict):
        """Record the end of an episode.

        Arguments:
            episode_id (str): Episode id returned from start_episode().
            observation_dict (dict): Current environment observation.
        """

        #episode = self._get(episode_id)
        self._finished.add(episode_id)
        #TODO -> only update one slot in new_observations.
        self.new_observation = observation_dict
        self.cur_dones = {"__all__": True}
        self._send(episode_id)

    def _send(self, episode_ids):
        #if not self.training_enabled:
        #    for agent_id in self.cur_infos:
        #        self.cur_infos[agent_id]["training_enabled"] = False

        # Build per-env, per-agent struct for obs, rewards, etc..
        print()
        item = {
            "obs": {env_id: dict(self._episodes[env_id]._agent_to_last_obs.items()) for env_id in episode_ids},
            "reward": {env_id: {agent_id: rew_hist[-1] for agent_id, rew_hist in self._episodes[env_id]._agent_reward_history.items()} for env_id in episode_ids},
            "done": {env_id: {agent_id: False for agent_id, rew_hist in self._episodes[env_id]._agent_reward_history.items()} for env_id in episode_ids},
            "info": {env_id: dict(self._episodes[env_id]._agent_to_last_pi_info.items()) for env_id in episode_ids},
        }
        #if self.new_actions is not None:
        #    item["off_policy_action"] = self.new_actions
        #self.new_observations = None
        #self.new_actions = None
        #self.cur_rewards = {}
        #else:
        #    if not self.training_enabled:
        #        item["info"]["training_enabled"] = False
        #    if self.num_envs > 1:
        #        self.cur_rewards = [0.0 for _ in range(self.num_envs)]
        #    else:
        #        self.cur_rewards = 0.0

        with self._results_avail_condition:
            self.data_queue.put_nowait(item)
            self._results_avail_condition.notify()
