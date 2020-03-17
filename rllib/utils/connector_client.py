"""REST client to interact with a ConnectorServer.

Inference is done on the client side for each step using a locally cached
policy from the server. The policy is periodically updated on a given interval.

For an example, run `examples/cartpole_server.py --use-connector` along
with `examples/cartpole_client.py --use-connector`.
"""

import threading
import time

from ray.rllib.env.external_env import ExternalEnv
from ray.rllib.env.external_multi_agent_env import ExternalMultiAgentEnv
from ray.rllib.evaluation.rollout_worker import RolloutWorker
from ray.rllib.utils.annotations import override, PublicAPI
from ray.rllib.utils.policy_client import PolicyClient


@PublicAPI
class ConnectorClient(PolicyClient):
    """RLlib connector client to use with the ConnectorServer.

    This implements the same interface as PolicyClient but with higher
    performance by caching the policy client side.
    """

    GET_WORKER_ARGS = "GET_WORKER_ARGS"
    GET_WEIGHTS = "GET_WEIGHTS"
    REPORT_SAMPLES = "REPORT_SAMPLES"

    def __init__(self, address, auto_wrap_env=True, update_interval=10.0):
        super().__init__(address)
        self.update_interval = update_interval
        self.last_updated = 0

        print("Querying server for rollout worker settings.")
        kwargs = self._send({
            "command": ConnectorClient.GET_WORKER_ARGS,
        })["worker_args"]

        # Since the server acts as an input datasource, we have to reset the
        # input config to the default, which runs env rollouts.
        del kwargs["input_creator"]
        print("Creating rollout worker with kwargs={}".format(kwargs))

        if auto_wrap_env:
            real_creator = kwargs["env_creator"]

            def wrap_env(env_config):
                real_env = real_creator(env_config)

                class ExternalEnvWrapper(ExternalEnv):
                    def __init__(self, real_env):
                        ExternalEnv.__init__(self, real_env.action_space,
                                             real_env.observation_space)

                    def run(self):
                        # Since we are calling methods on this class in the
                        # client, run doesn't need to do anything.
                        time.sleep(999999)

                return ExternalEnvWrapper(real_env)

            kwargs["env_creator"] = wrap_env

        self.rollout_worker = RolloutWorker(**kwargs)
        self.inference_thread = _InferenceThread(self)
        self.inference_thread.start()
        self.env = self.rollout_worker.env

        if not (isinstance(self.env, ExternalEnv)
                or isinstance(self.env, ExternalMultiAgentEnv)):
            raise ValueError(
                "You must specify an ExternalEnv class when using the "
                "application connector API. If you want this client to "
                "automatically wrap your env in an ExternalEnv interface, "
                "pass auto_wrap_env=True.")

    def _do_updates(self):
        assert self.inference_thread.is_alive()
        if time.time() - self.last_updated > self.update_interval:
            print("Querying server for new policy weights.")
            weights = self._send({
                "command": ConnectorClient.GET_WEIGHTS,
            })["weights"]
            print("Updating rollout worker weights.")
            self.rollout_worker.set_weights(weights)
            self.last_updated = time.time()

    @override(PolicyClient)
    def start_episode(self, episode_id=None, training_enabled=True):
        self._do_updates()
        return self.env.start_episode(episode_id, training_enabled)

    @override(PolicyClient)
    def get_action(self, episode_id, observation):
        self._do_updates()
        return self.env.get_action(episode_id, observation)

    @override(PolicyClient)
    def log_action(self, episode_id, observation, action):
        self._do_updates()
        return self.env.log_action(episode_id, observation, action)

    @override(PolicyClient)
    def log_returns(self, episode_id, reward, info=None):
        self._do_updates()
        return self.env.log_returns(episode_id, reward, info)

    @override(PolicyClient)
    def end_episode(self, episode_id, observation):
        self._do_updates()
        return self.env.end_episode(episode_id, observation)


class _InferenceThread(threading.Thread):
    """Thread that handles experience generation (worker.sample() loop)."""

    def __init__(self, client):
        super().__init__()
        self.daemon = True
        self.client = client

    def run(self):
        try:
            while True:
                print("Generating new batch of experiences.")
                samples = self.client.rollout_worker.sample()
                metrics = self.client.rollout_worker.get_metrics()
                print("Sending batch of {} steps back to server.".format(
                    samples.count))
                self.client._send({
                    "command": ConnectorClient.REPORT_SAMPLES,
                    "samples": samples,
                    "metrics": metrics,
                })
        except Exception as e:
            print("Error: inference worker thread died!", e)
