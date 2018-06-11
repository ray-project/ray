from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import pickle

try:
    import requests  # `requests` is not part of stdlib.
except ImportError:
    requests = None
    print("Couldn't import `requests` library. Be sure to install it on"
          " the client side.")


class PolicyClient(object):
    """Client to interact with a RLlib policy server."""

    START_EPISODE = "START_EPISODE"
    GET_ACTION = "GET_ACTION"
    LOG_ACTION = "LOG_ACTION"
    LOG_RETURNS = "LOG_RETURNS"
    END_EPISODE = "END_EPISODE"

    def __init__(self, address):
        self._address = address

    def start_episode(self, episode_id=None):
        """Record the start of an episode.

        Arguments:
            episode_id (str): Unique string id for the episode or None for
                it to be auto-assigned. Auto-assignment only works if there
                is at most one active episode at a time.
        """
        self._send({
            "command": PolicyClient.START_EPISODE,
            "episode_id": episode_id,
        })

    def get_action(self, observation, episode_id=None):
        """Record an observation and get the on-policy action.

        Arguments:
            observation (obj): Current environment observation.
            episode_id (str): Episode id passed to start_episode() or None.

        Returns:
            action (obj): Action from the env action space.
        """
        return self._send({
            "command": PolicyClient.GET_ACTION,
            "observation": observation,
            "episode_id": episode_id,
        })["action"]

    def log_action(self, observation, action, episode_id=None):
        """Record an observation and (off-policy) action taken.

        Arguments:
            observation (obj): Current environment observation.
            action (obj): Action for the observation.
            episode_id (str): Episode id passed to start_episode() or None.
        """
        self._send({
            "command": PolicyClient.LOG_ACTION,
            "observation": observation,
            "action": action,
            "episode_id": episode_id,
        })

    def log_returns(self, reward, info=None, episode_id=None):
        """Record returns from the environment.

        The reward will be attributed to the previous action taken by the
        episode. Rewards accumulate until the next action. If no reward is
        logged before the next action, a reward of 0.0 is assumed.

        Arguments:
            episode_id (str): Episode id passed to start_episode() or None.
            reward (float): Reward from the environment.
        """
        self._send({
            "command": PolicyClient.LOG_RETURNS,
            "reward": reward,
            "info": info,
            "episode_id": episode_id,
        })

    def end_episode(self, observation, episode_id=None):
        """Record the end of an episode.

        Arguments:
            episode_id (str): Episode id passed by start_episode() or None.
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
            print("Request failed", data)
            print(response.text)
        response.raise_for_status()
        parsed = pickle.loads(response.content)
        return parsed
