"""REST client to interact with a policy server.

This client supports both local and remote policy inference modes. Local
inference is faster but causes more compute to be done on the client.
"""

import logging
import threading
import time
from typing import Union, Optional
from enum import Enum

import ray.cloudpickle as pickle
from ray.rllib.env.external_env import ExternalEnv
from ray.rllib.env.external_multi_agent_env import ExternalMultiAgentEnv
from ray.rllib.env.multi_agent_env import MultiAgentEnv
from ray.rllib.policy.sample_batch import MultiAgentBatch
from ray.rllib.utils.annotations import PublicAPI
from ray.rllib.utils.typing import (
    MultiAgentDict,
    EnvInfoDict,
    EnvObsType,
    EnvActionType,
)

logger = logging.getLogger(__name__)

try:
    import requests  # `requests` is not part of stdlib.
except ImportError:
    requests = None
    logger.warning(
        "Couldn't import `requests` library. Be sure to install it on"
        " the client side."
    )


@PublicAPI
class Commands(Enum):
    # Generic commands (for both modes).
    ACTION_SPACE = "ACTION_SPACE"
    OBSERVATION_SPACE = "OBSERVATION_SPACE"

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
class PolicyClient:
    """REST client to interact with an RLlib policy server."""

    @PublicAPI
    def __init__(
        self, address: str, inference_mode: str = "local", update_interval: float = 10.0
    ):
        """Create a PolicyClient instance.

        Args:
            address: Server to connect to (e.g., "localhost:9090").
            inference_mode: Whether to use 'local' or 'remote' policy
                inference for computing actions.
            update_interval (float or None): If using 'local' inference mode,
                the policy is refreshed after this many seconds have passed,
                or None for manual control via client.
        """
        self.address = address
        self.env: ExternalEnv = None
        if inference_mode == "local":
            self.local = True
            self._setup_local_rollout_worker(update_interval)
        elif inference_mode == "remote":
            self.local = False
        else:
            raise ValueError("inference_mode must be either 'local' or 'remote'")

    @PublicAPI
    def start_episode(
        self, episode_id: Optional[str] = None, training_enabled: bool = True
    ) -> str:
        """Record the start of one or more episode(s).

        Args:
            episode_id (Optional[str]): Unique string id for the episode or
                None for it to be auto-assigned.
            training_enabled: Whether to use experiences for this
                episode to improve the policy.

        Returns:
            episode_id: Unique string id for the episode.
        """

        if self.local:
            self._update_local_policy()
            return self.env.start_episode(episode_id, training_enabled)

        return self._send(
            {
                "episode_id": episode_id,
                "command": Commands.START_EPISODE,
                "training_enabled": training_enabled,
            }
        )["episode_id"]

    @PublicAPI
    def get_action(
        self, episode_id: str, observation: Union[EnvObsType, MultiAgentDict]
    ) -> Union[EnvActionType, MultiAgentDict]:
        """Record an observation and get the on-policy action.

        Args:
            episode_id: Episode id returned from start_episode().
            observation: Current environment observation.

        Returns:
            action: Action from the env action space.
        """

        if self.local:
            self._update_local_policy()
            if isinstance(episode_id, (list, tuple)):
                actions = {
                    eid: self.env.get_action(eid, observation[eid])
                    for eid in episode_id
                }
                return actions
            else:
                return self.env.get_action(episode_id, observation)
        else:
            return self._send(
                {
                    "command": Commands.GET_ACTION,
                    "observation": observation,
                    "episode_id": episode_id,
                }
            )["action"]

    @PublicAPI
    def log_action(
        self,
        episode_id: str,
        observation: Union[EnvObsType, MultiAgentDict],
        action: Union[EnvActionType, MultiAgentDict],
    ) -> None:
        """Record an observation and (off-policy) action taken.

        Args:
            episode_id: Episode id returned from start_episode().
            observation: Current environment observation.
            action: Action for the observation.
        """

        if self.local:
            self._update_local_policy()
            return self.env.log_action(episode_id, observation, action)

        self._send(
            {
                "command": Commands.LOG_ACTION,
                "observation": observation,
                "action": action,
                "episode_id": episode_id,
            }
        )

    @PublicAPI
    def log_returns(
        self,
        episode_id: str,
        reward: float,
        info: Union[EnvInfoDict, MultiAgentDict] = None,
        multiagent_done_dict: Optional[MultiAgentDict] = None,
    ) -> None:
        """Record returns from the environment.

        The reward will be attributed to the previous action taken by the
        episode. Rewards accumulate until the next action. If no reward is
        logged before the next action, a reward of 0.0 is assumed.

        Args:
            episode_id: Episode id returned from start_episode().
            reward: Reward from the environment.
            info: Extra info dict.
            multiagent_done_dict: Multi-agent done information.
        """

        if self.local:
            self._update_local_policy()
            if multiagent_done_dict is not None:
                assert isinstance(reward, dict)
                return self.env.log_returns(
                    episode_id, reward, info, multiagent_done_dict
                )
            return self.env.log_returns(episode_id, reward, info)

        self._send(
            {
                "command": Commands.LOG_RETURNS,
                "reward": reward,
                "info": info,
                "episode_id": episode_id,
                "done": multiagent_done_dict,
            }
        )

    @PublicAPI
    def end_episode(
        self, episode_id: str, observation: Union[EnvObsType, MultiAgentDict]
    ) -> None:
        """Record the end of an episode.

        Args:
            episode_id: Episode id returned from start_episode().
            observation: Current environment observation.
        """

        if self.local:
            self._update_local_policy()
            return self.env.end_episode(episode_id, observation)

        self._send(
            {
                "command": Commands.END_EPISODE,
                "observation": observation,
                "episode_id": episode_id,
            }
        )

    @PublicAPI
    def update_policy_weights(self) -> None:
        """Query the server for new policy weights, if local inference is enabled."""
        self._update_local_policy(force=True)

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
        kwargs = self._send(
            {
                "command": Commands.GET_WORKER_ARGS,
            }
        )["worker_args"]
        (self.rollout_worker, self.inference_thread) = _create_embedded_rollout_worker(
            kwargs, self._send
        )
        self.env = self.rollout_worker.env

    def _update_local_policy(self, force=False):
        assert self.inference_thread.is_alive()
        if (
            self.update_interval
            and time.time() - self.last_updated > self.update_interval
        ) or force:
            logger.info("Querying server for new policy weights.")
            resp = self._send(
                {
                    "command": Commands.GET_WEIGHTS,
                }
            )
            weights = resp["weights"]
            global_vars = resp["global_vars"]
            logger.info(
                "Updating rollout worker weights and global vars {}.".format(
                    global_vars
                )
            )
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
                if isinstance(samples, MultiAgentBatch):
                    logger.info(
                        "Sending batch of {} env steps ({} agent steps) to "
                        "server.".format(samples.env_steps(), samples.agent_steps())
                    )
                else:
                    logger.info(
                        "Sending batch of {} steps back to server.".format(
                            samples.count
                        )
                    )
                self.send_fn(
                    {
                        "command": Commands.REPORT_SAMPLES,
                        "samples": samples,
                        "metrics": metrics,
                    }
                )
        except Exception as e:
            logger.info("Error: inference worker thread died!", e)


def _auto_wrap_external(real_env_creator):
    """Wrap an environment in the ExternalEnv interface if needed.

    Args:
        real_env_creator: Create an env given the env_config.
    """

    def wrapped_creator(env_config):
        real_env = real_env_creator(env_config)
        if not isinstance(real_env, (ExternalEnv, ExternalMultiAgentEnv)):
            logger.info(
                "The env you specified is not a supported (sub-)type of "
                "ExternalEnv. Attempting to convert it automatically to "
                "ExternalEnv."
            )

            if isinstance(real_env, MultiAgentEnv):
                external_cls = ExternalMultiAgentEnv
            else:
                external_cls = ExternalEnv

            class _ExternalEnvWrapper(external_cls):
                def __init__(self, real_env):
                    super().__init__(
                        observation_space=real_env.observation_space,
                        action_space=real_env.action_space,
                    )

                def run(self):
                    # Since we are calling methods on this class in the
                    # client, run doesn't need to do anything.
                    time.sleep(999999)

            return _ExternalEnvWrapper(real_env)
        return real_env

    return wrapped_creator


def _create_embedded_rollout_worker(kwargs, send_fn):
    """Create a local rollout worker and a thread that samples from it.

    Args:
        kwargs: Args for the RolloutWorker constructor.
        send_fn: Function to send a JSON request to the server.
    """

    # Since the server acts as an input datasource, we have to reset the
    # input config to the default, which runs env rollouts.
    kwargs = kwargs.copy()
    del kwargs["input_creator"]

    # Since the server also acts as an output writer, we might have to reset
    # the output config to the default, i.e. "output": None, otherwise a
    # local rollout worker might write to an unknown output directory
    del kwargs["output_creator"]

    # If server has no env (which is the expected case):
    # Generate a dummy ExternalEnv here using RandomEnv and the
    # given observation/action spaces.
    if kwargs["config"].env is None:
        from ray.rllib.examples.env.random_env import RandomEnv, RandomMultiAgentEnv

        config = {
            "action_space": kwargs["config"].action_space,
            "observation_space": kwargs["config"].observation_space,
        }
        is_ma = kwargs["config"].is_multi_agent()
        kwargs["env_creator"] = _auto_wrap_external(
            lambda _: (RandomMultiAgentEnv if is_ma else RandomEnv)(config)
        )
        # kwargs["config"].env = True
    # Otherwise, use the env specified by the server args.
    else:
        real_env_creator = kwargs["env_creator"]
        kwargs["env_creator"] = _auto_wrap_external(real_env_creator)

    logger.info("Creating rollout worker with kwargs={}".format(kwargs))
    from ray.rllib.evaluation.rollout_worker import RolloutWorker

    rollout_worker = RolloutWorker(**kwargs)

    inference_thread = _LocalInferenceThread(rollout_worker, send_fn)
    inference_thread.start()

    return rollout_worker, inference_thread
