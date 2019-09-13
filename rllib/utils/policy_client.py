from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import logging
import pickle

from ray.rllib.utils.annotations import PublicAPI

logger = logging.getLogger(__name__)

try:
    import requests  # `requests` is not part of stdlib.
except ImportError:
    requests = None
    logger.warning(
        "Couldn't import `requests` library. Be sure to install it on"
        " the client side.")


@PublicAPI
class PolicyClient(object):
    """REST client to interact with a RLlib policy server."""

    START_EPISODE = "START_EPISODE"
    GET_ACTION = "GET_ACTION"
    LOG_ACTION = "LOG_ACTION"
    LOG_RETURNS = "LOG_RETURNS"
    END_EPISODE = "END_EPISODE"

    @PublicAPI
    def __init__(self, address):
        self._address = address

    @PublicAPI
    def start_episode(self, episode_id=None, training_enabled=True):
        """Record the start of an episode.

        Arguments:
            episode_id (str): Unique string id for the episode or None for
                it to be auto-assigned.
            training_enabled (bool): Whether to use experiences for this
                episode to improve the policy.

        Returns:
            episode_id (str): Unique string id for the episode.
        """

        return self._send({
            "episode_id": episode_id,
            "command": PolicyClient.START_EPISODE,
            "training_enabled": training_enabled,
        })["episode_id"]

    @PublicAPI
    def get_action(self, episode_id, observation):
        """Record an observation and get the on-policy action.

        Arguments:
            episode_id (str): Episode id returned from start_episode().
            observation (obj): Current environment observation.

        Returns:
            action (obj): Action from the env action space.
        """
        return self._send({
            "command": PolicyClient.GET_ACTION,
            "observation": observation,
            "episode_id": episode_id,
        })["action"]

    @PublicAPI
    def log_action(self, episode_id, observation, action):
        """Record an observation and (off-policy) action taken.

        Arguments:
            episode_id (str): Episode id returned from start_episode().
            observation (obj): Current environment observation.
            action (obj): Action for the observation.
        """
        self._send({
            "command": PolicyClient.LOG_ACTION,
            "observation": observation,
            "action": action,
            "episode_id": episode_id,
        })

    @PublicAPI
    def log_returns(self, episode_id, reward, info=None):
        """Record returns from the environment.

        The reward will be attributed to the previous action taken by the
        episode. Rewards accumulate until the next action. If no reward is
        logged before the next action, a reward of 0.0 is assumed.

        Arguments:
            episode_id (str): Episode id returned from start_episode().
            reward (float): Reward from the environment.
        """
        self._send({
            "command": PolicyClient.LOG_RETURNS,
            "reward": reward,
            "info": info,
            "episode_id": episode_id,
        })

    @PublicAPI
    def end_episode(self, episode_id, observation):
        """Record the end of an episode.

        Arguments:
            episode_id (str): Episode id returned from start_episode().
            observation (obj): Current environment observation.
        """
        self._send({
            "command": PolicyClient.END_EPISODE,
            "observation": observation,
            "episode_id": episode_id,
        })

    def _send(self, data):
        payload = pickle.dumps(data)
        response = requests.post(self._address, data=payload)
        if response.status_code != 200:
            logger.error("Request failed {}: {}".format(response.text, data))
        response.raise_for_status()
        parsed = pickle.loads(response.content)
        return parsed
