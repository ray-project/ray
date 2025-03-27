import numpy as np

from ray.rllib.algorithms.algorithm_config import AlgorithmConfig
from ray.rllib.core import DEFAULT_MODULE_ID
from ray.rllib.env import INPUT_ENV_SPACES
from ray.rllib.env.env_runner import EnvRunner
from ray.rllib.env.single_agent_env_runner import SingleAgentEnvRunner
from ray.rllib.utils.annotations import override
from ray.rllib.utils.checkpoints import Checkpointable
from ray.rllib.utils.typing import StateDict


class SEEDEnvRunner(EnvRunner):
    """SEED: EnvRunner that implements ZMQ Router-Dealer communication pattern."""

    @override(EnvRunner)
    def __init__(self, *, config: AlgorithmConfig, **kwargs):
        super().__init__(config=config)

        self.worker_index: int = kwargs.get("worker_index")
        self.num_workers: int = kwargs.get("num_workers", self.config.num_env_runners)
        self.tune_trial_id: str = kwargs.get("tune_trial_id")

        self._callbacks = None
        self.metrics = None
        self.dealer_channel = None
        self.env = None
        self.make_env()

        # This should be the default.
        self._needs_initial_reset: bool = True

    def start_zmq(self, dealer_channel):
        self.dealer_channel = dealer_channel

    @override(EnvRunner)
    def assert_healthy(self):
        """Checks that self.__init__() has been completed properly.

        Ensures that the instances has an environment defined.

        Raises:
            AssertionError: If the EnvRunner Actor has NOT been properly initialized.
        """
        assert self.env

    @override(EnvRunner)
    def sample(self):
        while True:
            self._sample()

    def _sample(self):
        # Receive the message from the RouterChannel.
        actions = self.dealer_channel.read()

        # Compute an environment step.
        if self._needs_initial_reset:
            observation, info = self.env.reset()
            reward = np.zeros(shape=(self.num_envs,))
            terminated = truncated = np.zeros(shape=(self.num_envs,)).astype(bool)
            self._needs_initial_reset = False
        else:
            actions = np.frombuffer(actions, dtype=np.int32)
            observation, reward, terminated, truncated, info = self.env.step(actions)

        # Send the new state to the RouterChannel.
        self.dealer_channel.write(
            (
                observation.tobytes(),
                reward.tobytes(),
                terminated.tobytes(),
                truncated.tobytes(),
            )
        )

    @override(EnvRunner)
    def make_env(self):
        return SingleAgentEnvRunner.make_env(self)

    @override(EnvRunner)
    def make_module(self):
        raise NotImplementedError("SEEDEnvRunner doesn't have a module!")

    @override(EnvRunner)
    def stop(self):
        # Close our env object via gymnasium's API.
        self.env.close()

    def get_spaces(self):
        return {
            INPUT_ENV_SPACES: (self.env.observation_space, self.env.action_space),
            DEFAULT_MODULE_ID: (
                self.env.single_observation_space,
                self.env.single_action_space,
            ),
        }

    @override(Checkpointable)
    def set_state(self, state: StateDict) -> None:
        pass

    @override(Checkpointable)
    def get_state(self, **kwargs) -> StateDict:
        return {}
