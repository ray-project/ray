from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import uuid

from ray.rllib.utils.annotations import override, PublicAPI
from ray.rllib.env.external_env import ExternalEnv, _ExternalEnvEpisode


@PublicAPI
class ExternalMultiAgentEnv(ExternalEnv):
    """This is the multi-agent version of ExternalEnv."""

    @PublicAPI
    def __init__(self, action_space, observation_space, max_concurrent=100):
        """Initialize a multi-agent external env.

        ExternalMultiAgentEnv subclasses must call this during their __init__.

        Arguments:
            action_space (gym.Space): Action space of the env.
            observation_space (gym.Space): Observation space of the env.
            max_concurrent (int): Max number of active episodes to allow at
                once. Exceeding this limit raises an error.
        """
        ExternalEnv.__init__(self, action_space, observation_space,
                             max_concurrent)

        # we require to know all agents' spaces
        if isinstance(self.action_space, dict) or isinstance(
                self.observation_space, dict):
            if not (self.action_space.keys() == self.observation_space.keys()):
                raise ValueError("Agent ids disagree for action space and obs "
                                 "space dict: {} {}".format(
                                     self.action_space.keys(),
                                     self.observation_space.keys()))

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
    @override(ExternalEnv)
    def start_episode(self, episode_id=None, training_enabled=True):
        if episode_id is None:
            episode_id = uuid.uuid4().hex

        if episode_id in self._finished:
            raise ValueError(
                "Episode {} has already completed.".format(episode_id))

        if episode_id in self._episodes:
            raise ValueError(
                "Episode {} is already started".format(episode_id))

        self._episodes[episode_id] = _ExternalEnvEpisode(
            episode_id,
            self._results_avail_condition,
            training_enabled,
            multiagent=True)

        return episode_id

    @PublicAPI
    @override(ExternalEnv)
    def get_action(self, episode_id, observation_dict):
        """Record an observation and get the on-policy action.
        observation_dict is expected to contain the observation
        of all agents acting in this episode step.

        Arguments:
            episode_id (str): Episode id returned from start_episode().
            observation_dict (dict): Current environment observation.

        Returns:
            action (dict): Action from the env action space.
        """

        episode = self._get(episode_id)
        return episode.wait_for_action(observation_dict)

    @PublicAPI
    @override(ExternalEnv)
    def log_action(self, episode_id, observation_dict, action_dict):
        """Record an observation and (off-policy) action taken.

        Arguments:
            episode_id (str): Episode id returned from start_episode().
            observation_dict (dict): Current environment observation.
            action_dict (dict): Action for the observation.
        """

        episode = self._get(episode_id)
        episode.log_action(observation_dict, action_dict)

    @PublicAPI
    @override(ExternalEnv)
    def log_returns(self, episode_id, reward_dict, info_dict=None):
        """Record returns from the environment.

        The reward will be attributed to the previous action taken by the
        episode. Rewards accumulate until the next action. If no reward is
        logged before the next action, a reward of 0.0 is assumed.

        Arguments:
            episode_id (str): Episode id returned from start_episode().
            reward_dict (dict): Reward from the environment agents.
            info (dict): Optional info dict.
        """

        episode = self._get(episode_id)

        # accumulate reward by agent
        # for existing agents, we want to add the reward up
        for agent, rew in reward_dict.items():
            if agent in episode.cur_reward_dict:
                episode.cur_reward_dict[agent] += rew
            else:
                episode.cur_reward_dict[agent] = rew
        if info_dict:
            episode.cur_info_dict = info_dict or {}

    @PublicAPI
    @override(ExternalEnv)
    def end_episode(self, episode_id, observation_dict):
        """Record the end of an episode.

        Arguments:
            episode_id (str): Episode id returned from start_episode().
            observation_dict (dict): Current environment observation.
        """

        episode = self._get(episode_id)
        self._finished.add(episode.episode_id)
        episode.done(observation_dict)
