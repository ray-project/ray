from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from six.moves import queue
import threading
import time

from ray.rllib.utils.async_vector_env import AsyncVectorEnv


class ServingEnv(threading.Thread):
    """Environment that provides policy serving.

    Unlike simulator envs, control is inverted. The agent queries the env to
    obtain actions and records observations and rewards for training. This is
    in contrast to gym.Env, where the algorithm drives the simulation through
    env.step() calls.

    You can use ServingEnv as the backend for policy serving (by serving HTTP
    requests in the run loop), for ingesting offline logs data (by reading
    offline transitions in the run loop), or other custom use cases not easily
    expressed through gym.Env.

    ServingEnv supports both on-policy serving (through self.get_action()), and
    off-policy serving (through self.log_action()).

    Examples:
        >>> register_env("my_env", lambda config: YourServingEnv(config))
        >>> agent = DQNAgent(env="my_env")
        >>> while True:
              print(agent.train())
    """

    def __init__(self, action_space, observation_space):
        threading.Thread.__init__(self)
        self.daemon = True
        self.action_space = action_space
        self.observation_space = observation_space
        self._episodes = {}
        self._num_episodes = 0
        self._cur_default_episode_id = None

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
            episode_id (str): Unique string id for the episode or None if there
                is only going to be a single active episode.
        """

        if not episode_id:
            if self._cur_default_episode_id:
                raise ValueError(
                    "An existing episode is still active. You must pass "
                    "`episode_id` if there are going to be multiple active "
                    "episodes at once.")
            episode_id = "default_{}".format(self._num_episodes)
            self._cur_default_episode_id = episode_id
            self._num_episodes += 1

        if episode_id in self._episodes:
            raise ValueError(
                "Episode {} is already started".format(episode_id))

        self._episodes[episode_id] = _Episode()
        return episode_id

    def get_action(self, observation, episode_id=None):
        """Record an observation and get the policy action.

        Arguments:
            observation (obj): Current environment observation.
            episode_id (str): Episode id passed to start_episode() or None.

        Returns:
            action (obj): Action from the env action space.
        """

        episode_id = episode_id or self._cur_default_episode_id
        if episode_id not in self._episodes:
            raise ValueError("Episode {} not found.".format(episode_id))

        episode = self._episodes[episode_id]
        return episode.wait_for_action(observation)

    def log_returns(self, reward, info=None, episode_id=None):
        """Record returns from the environment.

        The reward will be attributed to the previous action taken by the
        episode. Rewards accumulate until the next action. If no reward is
        logged before the next action, a reward of 0.0 is assumed.

        Arguments:
            episode_id (str): Episode id passed to start_episode() or None.
            reward (float): Reward from the environment.

        Returns:
            action (obj): Action from the env action space.
        """

        episode_id = episode_id or self._cur_default_episode_id
        if episode_id not in self._episodes:
            raise ValueError("Episode {} not found.".format(episode_id))

        episode = self._episodes[episode_id]
        episode.cur_reward += reward
        if info:
            episode.cur_info = info

    def end_episode(self, observation, episode_id=None):
        """Record the end of an episode.

        Arguments:
            episode_id (str): Episode id passed by start_episode() or None.
            observation (obj): Current environment observation.
        """

        episode_id = episode_id or self._cur_default_episode_id
        if episode_id not in self._episodes:
            raise ValueError("Episode {} not found.".format(episode_id))

        self._cur_default_episode_id = None
        episode = self._episodes[episode_id]
        episode.done(observation)


class _ServingEnvToAsync(AsyncVectorEnv):
    def __init__(self, serving_env):
        self.serving_env = serving_env
        serving_env.start()

    def poll(self):
        results = self._poll()
        while len(results[0]) == 0:
            time.sleep(.001)
            results = self._poll()
            if not self.serving_env.isAlive():
                raise Exception("Serving thread has stopped.")
        return results

    def _poll(self):
        all_obs, all_rewards, all_dones, all_infos = {}, {}, {}, {}
        for eid, episode in self.serving_env._episodes.copy().items():
            data = episode.get_data()
            if episode.cur_done:
                del self.serving_env._episodes[eid]
            if data:
                all_obs[eid] = data["obs"]
                all_rewards[eid] = data["reward"]
                all_dones[eid] = data["done"]
                all_infos[eid] = data["info"]
        return all_obs, all_rewards, all_dones, all_infos

    def send_actions(self, action_dict):
        for eid, action in action_dict.items():
            self.serving_env._episodes[eid].action_queue.put(action)


class _Episode(object):
    def __init__(self):
        self.data_queue = queue.Queue(maxsize=1)
        self.action_queue = queue.Queue(maxsize=1)
        self.new_observation = None
        self.cur_reward = 0.0
        self.cur_done = False
        self.cur_info = {}

    def get_data(self):
        if self.data_queue.empty():
            return None
        return self.data_queue.get_nowait()

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
        self.new_observation = None
        self.cur_reward = 0.0
        self.data_queue.put_nowait(item)
