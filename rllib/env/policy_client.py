"""REST client to interact with a policy server.

This client supports both local and remote policy inference modes. Local
inference is faster but causes more compute to be done on the client.
"""

import logging
import threading
import time

import ray.cloudpickle as pickle
from ray.rllib.evaluation.rollout_worker import RolloutWorker
from ray.rllib.env import ExternalEnv, MultiAgentEnv, ExternalMultiAgentEnv
from ray.rllib.utils.annotations import PublicAPI

logger = logging.getLogger(__name__)
logger.setLevel("INFO")  # TODO(ekl) seems to be needed for cartpole_client.py

try:
    import requests  # `requests` is not part of stdlib.
except ImportError:
    requests = None
    logger.warning(
        "Couldn't import `requests` library. Be sure to install it on"
        " the client side.")


@PublicAPI
class PolicyClient:
    """REST client to interact with a RLlib policy server."""

    # Commands for local inference mode.
    GET_WORKER_ARGS = "GET_WORKER_ARGS"
    GET_WEIGHTS = "GET_WEIGHTS"
    REPORT_SAMPLES = "REPORT_SAMPLES"

    # Commands for remote inference mode.
    START_EPISODE = "START_EPISODE"
    GET_ACTION = "GET_ACTION"
    LOG_ACTION = "LOG_ACTION"
    LOG_RETURNS = "LOG_RETURNS"
    END_EPISODE = "END_EPISODE"

    @PublicAPI
    def __init__(self, address, inference_mode="local", update_interval=10.0):
        """Create a PolicyClient instance.

        Args:
            address (str): Server to connect to (e.g., "localhost:9090").
            inference_mode (str): Whether to use 'local' or 'remote' policy
                inference for computing actions.
            update_interval (float): If using 'local' inference mode, the
                policy is refreshed after this many seconds have passed.
        """
        self.address = address
        if inference_mode == "local":
            self.local = True
            self._setup_local_rollout_worker(update_interval)
        elif inference_mode == "remote":
            self.local = False
        else:
            raise ValueError(
                "inference_mode must be either 'local' or 'remote'")

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

        if self.local:
            self._update_local_policy()
            return self.env.start_episode(episode_id, training_enabled)

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

        if self.local:
            self._update_local_policy()
            return self.env.get_action(episode_id, observation)

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

        if self.local:
            self._update_local_policy()
            return self.env.log_action(episode_id, observation, action)

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

        if self.local:
            self._update_local_policy()
            return self.env.log_returns(episode_id, reward, info)

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

        if self.local:
            self._update_local_policy()
            return self.env.end_episode(episode_id, observation)

        self._send({
            "command": PolicyClient.END_EPISODE,
            "observation": observation,
            "episode_id": episode_id,
        })

    def _send(self, data):
        payload = pickle.dumps(data)
        response = requests.post(self.address, data=payload)
        if response.status_code != 200:
            logger.error("Request failed {}: {}".format(response.text, data))
        response.raise_for_status()
        parsed = pickle.loads(response.content)
        return parsed

    def _setup_local_rollout_worker(self, update_interval):
        self.update_interval = update_interval
        self.last_updated = 0

        logger.info("Querying server for rollout worker settings.")
        kwargs = self._send({
            "command": PolicyClient.GET_WORKER_ARGS,
        })["worker_args"]

        (self.rollout_worker,
         self.inference_thread) = create_embedded_rollout_worker(
             kwargs, self._send)
        self.env = self.rollout_worker.env

    def _update_local_policy(self):
        assert self.inference_thread.is_alive()
        if time.time() - self.last_updated > self.update_interval:
            logger.info("Querying server for new policy weights.")
            resp = self._send({
                "command": PolicyClient.GET_WEIGHTS,
            })
            weights = resp["weights"]
            global_vars = resp["global_vars"]
            logger.info(
                "Updating rollout worker weights and global vars {}.".format(
                    global_vars))
            self.rollout_worker.set_weights(weights, global_vars)
            self.last_updated = time.time()


class _LocalInferenceThread(threading.Thread):
    """Thread that handles experience generation (worker.sample() loop)."""

    def __init__(self, rollout_worker, send_fn):
        super().__init__()
        self.daemon = True
        self.rollout_worker = rollout_worker
        self.send_fn = send_fn

    def run(self):
        try:
            while True:
                logger.info("Generating new batch of experiences.")
                samples = self.rollout_worker.sample()
                metrics = self.rollout_worker.get_metrics()
                logger.info("Sending batch of {} steps back to server.".format(
                    samples.count))
                self.send_fn({
                    "command": PolicyClient.REPORT_SAMPLES,
                    "samples": samples,
                    "metrics": metrics,
                })
        except Exception as e:
            logger.info("Error: inference worker thread died!", e)


def auto_wrap_external(real_env_creator):
    """Wrap an environment in the ExternalEnv interface if needed.

    Args:
        real_env_creator (fn): Create an env given the env_config.
    """

    def wrapped_creator(env_config):
        real_env = real_env_creator(env_config)
        if not (isinstance(real_env, ExternalEnv)
                or isinstance(real_env, ExternalMultiAgentEnv)):
            logger.info(
                "The env you specified is not a type of ExternalEnv. "
                "Attempting to convert it automatically to ExternalEnv.")

            if isinstance(real_env, MultiAgentEnv):
                external_cls = MultiAgentEnv
            else:
                external_cls = ExternalEnv

            class ExternalEnvWrapper(external_cls):
                def __init__(self, real_env):
                    super().__init__(real_env.action_space,
                                     real_env.observation_space)

                def run(self):
                    # Since we are calling methods on this class in the
                    # client, run doesn't need to do anything.
                    time.sleep(999999)

            return ExternalEnvWrapper(real_env)

    return wrapped_creator


def create_embedded_rollout_worker(kwargs, send_fn):
    """Create a local rollout worker and a thread that samples from it.

    Arguments:
        kwargs (dict): args for the RolloutWorker constructor.
        send_fn (fn): function to send a JSON request to the server.
    """

    # Since the server acts as an input datasource, we have to reset the
    # input config to the default, which runs env rollouts.
    kwargs = kwargs.copy()
    del kwargs["input_creator"]
    logger.info("Creating rollout worker with kwargs={}".format(kwargs))
    real_env_creator = kwargs["env_creator"]
    kwargs["env_creator"] = auto_wrap_external(real_env_creator)

    rollout_worker = RolloutWorker(**kwargs)
    inference_thread = _LocalInferenceThread(rollout_worker, send_fn)
    inference_thread.start()
    return rollout_worker, inference_thread
