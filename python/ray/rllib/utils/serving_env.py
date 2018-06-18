from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from six.moves import queue
import threading

from ray.rllib.utils.async_vector_env import AsyncVectorEnv


class ServingEnv(threading.Thread):
    """Environment that provides policy serving.

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

    TODO: Provide a HTTP server/client example based on ServingEnv.

    Examples:
        >>> register_env("my_env", lambda config: YourServingEnv(config))
        >>> agent = DQNAgent(env="my_env")
        >>> while True:
              print(agent.train())
    """

    def __init__(self, action_space, observation_space, max_concurrent=100):
        """Initialize a serving env.

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
        self._num_episodes = 0
        self._cur_default_episode_id = None
        self._results_avail_condition = threading.Condition()
        self._max_concurrent_episodes = max_concurrent

    def run(self):
        """Override this to implement the run loop.

        Your loop should continuously:
            1. Call self.start_episode()
            2. Call self.get_action() or self.log_action()
            3. Call self.log_returns()
            4. Call self.end_episode()
            5. Wait if nothing to do.

        Multiple episodes may be started at the same time.
        """
        raise NotImplementedError

    def start_episode(self, episode_id=None):
        """Record the start of an episode.

        Arguments:
            episode_id (str): Unique string id for the episode or None for
                it to be auto-assigned. Auto-assignment only works if there
                is at most one active episode at a time.
        """

        if episode_id is None:
            if self._cur_default_episode_id:
                raise ValueError(
                    "An existing episode is still active. You must pass "
                    "`episode_id` if there are going to be multiple active "
                    "episodes at once.")
            episode_id = "default_{}".format(self._num_episodes)
            self._cur_default_episode_id = episode_id
            self._num_episodes += 1

        if episode_id in self._finished:
            raise ValueError(
                "Episode {} has already completed.".format(episode_id))

        if episode_id in self._episodes:
            raise ValueError(
                "Episode {} is already started".format(episode_id))

        self._episodes[episode_id] = _Episode(
            episode_id, self._results_avail_condition)

    def get_action(self, observation, episode_id=None):
        """Record an observation and get the on-policy action.

        Arguments:
            observation (obj): Current environment observation.
            episode_id (str): Episode id passed to start_episode() or None.

        Returns:
            action (obj): Action from the env action space.
        """

        episode = self._get(episode_id)
        return episode.wait_for_action(observation)

    def log_action(self, observation, action, episode_id=None):
        """Record an observation and (off-policy) action taken.

        Arguments:
            observation (obj): Current environment observation.
            action (obj): Action for the observation.
            episode_id (str): Episode id passed to start_episode() or None.
        """

        episode = self._get(episode_id)
        episode.log_action(observation, action)

    def log_returns(self, reward, info=None, episode_id=None):
        """Record returns from the environment.

        The reward will be attributed to the previous action taken by the
        episode. Rewards accumulate until the next action. If no reward is
        logged before the next action, a reward of 0.0 is assumed.

        Arguments:
            episode_id (str): Episode id passed to start_episode() or None.
            reward (float): Reward from the environment.
        """

        episode = self._get(episode_id)
        episode.cur_reward += reward
        if info:
            episode.cur_info = info

    def end_episode(self, observation, episode_id=None):
        """Record the end of an episode.

        Arguments:
            episode_id (str): Episode id passed by start_episode() or None.
            observation (obj): Current environment observation.
        """

        episode = self._get(episode_id)
        self._finished.add(episode.episode_id)
        self._cur_default_episode_id = None
        episode.done(observation)

    def _get(self, episode_id=None):
        """Get a started episode or raise an error."""

        if episode_id is None:
            episode_id = self._cur_default_episode_id

        if episode_id in self._finished:
            raise ValueError(
                "Episode {} has already completed.".format(episode_id))

        if episode_id not in self._episodes:
            raise ValueError("Episode {} not found.".format(episode_id))

        return self._episodes[episode_id]


class _ServingEnvToAsync(AsyncVectorEnv):
    """Internal adapter of ServingEnv to AsyncVectorEnv."""

    def __init__(self, serving_env):
        self.serving_env = serving_env
        serving_env.start()

    def poll(self):
        with self.serving_env._results_avail_condition:
            results = self._poll()
            while len(results[0]) == 0:
                self.serving_env._results_avail_condition.wait()
                results = self._poll()
                if not self.serving_env.isAlive():
                    raise Exception("Serving thread has stopped.")
        limit = self.serving_env._max_concurrent_episodes
        assert len(results[0]) < limit, \
            ("Too many concurrent episodes, were some leaked? This ServingEnv "
             "was created with max_concurrent={}".format(limit))
        return results

    def _poll(self):
        all_obs, all_rewards, all_dones, all_infos = {}, {}, {}, {}
        off_policy_actions = {}
        for eid, episode in self.serving_env._episodes.copy().items():
            data = episode.get_data()
            if episode.cur_done:
                del self.serving_env._episodes[eid]
            if data:
                all_obs[eid] = data["obs"]
                all_rewards[eid] = data["reward"]
                all_dones[eid] = data["done"]
                all_infos[eid] = data["info"]
                if "off_policy_action" in data:
                    off_policy_actions[eid] = data["off_policy_action"]
        return all_obs, all_rewards, all_dones, all_infos, off_policy_actions

    def send_actions(self, action_dict):
        for eid, action in action_dict.items():
            self.serving_env._episodes[eid].action_queue.put(action)


class _Episode(object):
    """Tracked state for each active episode."""

    def __init__(self, episode_id, results_avail_condition):
        self.episode_id = episode_id
        self.results_avail_condition = results_avail_condition
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
        self.new_observation = None
        self.new_action = None
        self.cur_reward = 0.0
        with self.results_avail_condition:
            self.data_queue.put_nowait(item)
            self.results_avail_condition.notify()
