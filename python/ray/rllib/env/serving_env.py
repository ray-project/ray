from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from six.moves import queue
import threading
import uuid


class ServingEnv(threading.Thread):
    """An environment that provides policy serving.

    Unlike simulator envs, control is inverted. The environment queries the
    policy to obtain actions and logs observations and rewards for training.
    This is in contrast to gym.Env, where the algorithm drives the simulation
    through env.step() calls.

    You can use ServingEnv as the backend for policy serving (by serving HTTP
    requests in the run loop), for ingesting offline logs data (by reading
    offline transitions in the run loop), or other custom use cases not easily
    expressed through gym.Env.

    ServingEnv supports both on-policy serving (through self.get_action()), and
    off-policy serving (through self.log_action()).

    This env is thread-safe, but individual episodes must be executed serially.

    Attributes:
        action_space (gym.Space): Action space.
        observation_space (gym.Space): Observation space.

    Examples:
        >>> register_env("my_env", lambda config: YourServingEnv(config))
        >>> agent = DQNAgent(env="my_env")
        >>> while True:
              print(agent.train())
    """

    def __init__(self, action_space, observation_space, max_concurrent=100):
        """Initialize a serving env.

        ServingEnv subclasses must call this during their __init__.

        Arguments:
            action_space (gym.Space): Action space of the env.
            observation_space (gym.Space): Observation space of the env.
            max_concurrent (int): Max number of active episodes to allow at
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

    def run(self):
        """Override this to implement the run loop.

        Your loop should continuously:
            1. Call self.start_episode(episode_id)
            2. Call self.get_action(episode_id, obs)
                    -or-
                    self.log_action(episode_id, obs, action)
            3. Call self.log_returns(episode_id, reward)
            4. Call self.end_episode(episode_id, obs)
            5. Wait if nothing to do.

        Multiple episodes may be started at the same time.
        """
        raise NotImplementedError

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

        if episode_id is None:
            episode_id = uuid.uuid4().hex

        if episode_id in self._finished:
            raise ValueError(
                "Episode {} has already completed.".format(episode_id))

        if episode_id in self._episodes:
            raise ValueError(
                "Episode {} is already started".format(episode_id))

        self._episodes[episode_id] = _ServingEnvEpisode(
            episode_id, self._results_avail_condition, training_enabled)

        return episode_id

    def get_action(self, episode_id, observation):
        """Record an observation and get the on-policy action.

        Arguments:
            episode_id (str): Episode id returned from start_episode().
            observation (obj): Current environment observation.

        Returns:
            action (obj): Action from the env action space.
        """

        episode = self._get(episode_id)
        return episode.wait_for_action(observation)

    def log_action(self, episode_id, observation, action):
        """Record an observation and (off-policy) action taken.

        Arguments:
            episode_id (str): Episode id returned from start_episode().
            observation (obj): Current environment observation.
            action (obj): Action for the observation.
        """

        episode = self._get(episode_id)
        episode.log_action(observation, action)

    def log_returns(self, episode_id, reward, info=None):
        """Record returns from the environment.

        The reward will be attributed to the previous action taken by the
        episode. Rewards accumulate until the next action. If no reward is
        logged before the next action, a reward of 0.0 is assumed.

        Arguments:
            episode_id (str): Episode id returned from start_episode().
            reward (float): Reward from the environment.
            info (dict): Optional info dict.
        """

        episode = self._get(episode_id)
        episode.cur_reward += reward
        if info:
            episode.cur_info = info or {}

    def end_episode(self, episode_id, observation):
        """Record the end of an episode.

        Arguments:
            episode_id (str): Episode id returned from start_episode().
            observation (obj): Current environment observation.
        """

        episode = self._get(episode_id)
        self._finished.add(episode.episode_id)
        episode.done(observation)

    def _get(self, episode_id):
        """Get a started episode or raise an error."""

        if episode_id in self._finished:
            raise ValueError(
                "Episode {} has already completed.".format(episode_id))

        if episode_id not in self._episodes:
            raise ValueError("Episode {} not found.".format(episode_id))

        return self._episodes[episode_id]


class _ServingEnvEpisode(object):
    """Tracked state for each active episode."""

    def __init__(self, episode_id, results_avail_condition, training_enabled):
        self.episode_id = episode_id
        self.results_avail_condition = results_avail_condition
        self.training_enabled = training_enabled
        self.data_queue = queue.Queue()
        self.action_queue = queue.Queue()
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
        self.new_observation = observation
        self.new_action = action
        self._send()
        self.action_queue.get(True, timeout=60.0)

    def wait_for_action(self, observation):
        self.new_observation = observation
        self._send()
        return self.action_queue.get(True, timeout=60.0)

    def done(self, observation):
        self.new_observation = observation
        self.cur_done = True
        self._send()

    def _send(self):
        item = {
            "obs": self.new_observation,
            "reward": self.cur_reward,
            "done": self.cur_done,
            "info": self.cur_info,
        }
        if self.new_action is not None:
            item["off_policy_action"] = self.new_action
        if not self.training_enabled:
            item["info"]["training_enabled"] = False
        self.new_observation = None
        self.new_action = None
        self.cur_reward = 0.0
        with self.results_avail_condition:
            self.data_queue.put_nowait(item)
            self.results_avail_condition.notify()
