from abc import ABCMeta, abstractmethod
from collections import defaultdict
import pickle
import socket
import threading
import time
from typing import Collection, DefaultDict, List, Optional, Union, TypeVar, \
    Generic

from ray.rllib.core import (
    COMPONENT_RL_MODULE,
    COMPONENT_ENV_TO_MODULE_CONNECTOR,
    COMPONENT_MODULE_TO_ENV_CONNECTOR,
)
from ray.rllib.env.env_runner import EnvRunner
from ray.rllib.env.multi_agent_episode import MultiAgentEpisode
from ray.rllib.env.single_agent_episode import SingleAgentEpisode
from ray.rllib.env.external.rllink import (
    get_rllink_message,
    send_rllink_message,
    RLlink,
)
from ray.rllib.utils.annotations import override
from ray.rllib.utils.checkpoints import Checkpointable
from ray.rllib.utils.metrics.metrics_logger import MetricsLogger
from ray.rllib.utils.typing import EpisodeID, StateDict

T_EpisodeType = TypeVar(
    "T_EpisodeType", bound=SingleAgentEpisode | MultiAgentEpisode
)


class BaseExternalEnvRunnerServer(
    EnvRunner, Checkpointable, Generic[T_EpisodeType], metaclass=ABCMeta
):
    @override(EnvRunner)
    def __init__(self, *, config, **kwargs):
        super().__init__(config=config, **kwargs)

        self.worker_index: int = kwargs.get("worker_index", 0)

        self._weights_seq_no = 0

        # Build the module from its spec.
        self.make_module()

        self.host = "localhost"
        self.port = int(self.config.env_config.get("port", 5555)) + self.worker_index
        self.server_socket = None
        self.client_socket = None
        self.address = None

        self.metrics = MetricsLogger()

        self._episode_chunks_to_return: Optional[List[T_EpisodeType]] = None
        self._done_episodes_for_metrics: List[T_EpisodeType] = []
        self._ongoing_episodes_for_metrics: DefaultDict[
            EpisodeID, List[T_EpisodeType]
        ] = defaultdict(list)

        self._sample_lock = threading.Lock()
        self._on_policy_lock = threading.Lock()
        self._blocked_on_state = False

        # Start a background thread for client communication.
        self.thread = threading.Thread(
            target=self._client_message_listener, daemon=True
        )
        self.thread.start()

    @property
    @abstractmethod
    def episode_type(self) -> type[T_EpisodeType]: ...

    @property
    @abstractmethod
    def base_env_runner_type(self) -> type[EnvRunner]: ...

    @override(EnvRunner)
    def assert_healthy(self):
        """Checks that the server socket is open and listening."""
        assert (
            self.server_socket is not None
        ), "Server socket is None (not connected, not listening)."

    @override(EnvRunner)
    def sample(self, **kwargs):
        """Waits for the client to send episodes."""
        while True:
            with self._sample_lock:
                if self._episode_chunks_to_return is not None:
                    num_env_steps = 0
                    num_episodes_completed = 0
                    for eps in self._episode_chunks_to_return:
                        if eps.is_done:
                            self._done_episodes_for_metrics.append(eps)
                            num_episodes_completed += 1
                        else:
                            self._ongoing_episodes_for_metrics[
                                eps.id_].append(eps)
                        num_env_steps += len(eps)

                    ret = self._episode_chunks_to_return
                    self._episode_chunks_to_return = None

                    self.base_env_runner_type._increase_sampled_metrics(
                        self, num_env_steps, num_episodes_completed
                    )

                    return ret
            time.sleep(0.01)

    @override(EnvRunner)
    def get_metrics(self):
        return self.base_env_runner_type.get_metrics(self)

    def _log_episode_metrics(self, *args, **kwargs) -> None:
        self.base_env_runner_type._log_episode_metrics(self, *args, **kwargs)

    @override(EnvRunner)
    def stop(self):
        """Closes the client and server sockets."""
        self._close_sockets_if_necessary()

    @override(Checkpointable)
    def get_ctor_args_and_kwargs(self):
        return (
            (),  # *args
            {"config": self.config},  # **kwargs
        )

    @override(Checkpointable)
    def get_checkpointable_components(self):
        return [
            (COMPONENT_RL_MODULE, self.module),
        ]

    @override(Checkpointable)
    def get_state(
        self,
        components: Optional[Union[str, Collection[str]]] = None,
        *,
        not_components: Optional[Union[str, Collection[str]]] = None,
        **kwargs,
    ) -> StateDict:
        additional_not_components = [
            COMPONENT_ENV_TO_MODULE_CONNECTOR,
            COMPONENT_MODULE_TO_ENV_CONNECTOR,
        ]
        if not_components is None:
            not_components = additional_not_components
        elif isinstance(not_components, str):
            not_components = [not_components] + additional_not_components
        else:
            additional_not_components.extend(not_components)
            not_components = additional_not_components
        return self.base_env_runner_type.get_state(
            self, components=components, not_components=not_components
        )

    @override(Checkpointable)
    def set_state(self, state: StateDict) -> None:
        self.base_env_runner_type.set_state(self, state=state)

        if self._blocked_on_state is True:
            self._send_set_state_message()
            self._blocked_on_state = False

    def _client_message_listener(self):
        """Entry point for the listener thread."""

        # Set up the server socket and bind to the specified host and port.
        self._recycle_sockets()

        # Enter an endless message receival- and processing loop.
        while True:
            # As long as we are blocked on a new state, sleep a bit and continue.
            # Do NOT process any incoming messages (until we send out the new state
            # back to the client).
            if self._blocked_on_state is True:
                time.sleep(0.01)
                continue

            try:
                # Blocking call to get next message.
                msg_type, msg_body = get_rllink_message(self.client_socket)

                # Process the message received based on its type.
                # Initial handshake.
                if msg_type == RLlink.PING:
                    self._send_pong_message()

                # Episode data from the client.
                elif msg_type in [
                    RLlink.EPISODES,
                    RLlink.EPISODES_AND_GET_STATE,
                ]:
                    self._process_episodes_message(msg_type, msg_body)

                # Client requests the state (model weights).
                elif msg_type == RLlink.GET_STATE:
                    self._send_set_state_message()

                # Clients requests config information.
                elif msg_type == RLlink.GET_CONFIG:
                    self._send_set_config_message()

            except ConnectionError as e:
                print(f"Messaging/connection error {e}! Recycling sockets ...")
                self._recycle_sockets(5.0)
                continue

    def _recycle_sockets(self, sleep: float = 0.0):
        # Close all old sockets, if they exist.
        self._close_sockets_if_necessary()

        time.sleep(sleep)

        # Start listening on the configured port.
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        # Allow reuse of the address.
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server_socket.bind((self.host, self.port))
        # Listen for a single connection.
        self.server_socket.listen(1)
        print(f"Waiting for client to connect to port {self.port}...")

        self.client_socket, self.address = self.server_socket.accept()
        print(f"Connected to client at {self.address}")

    def _close_sockets_if_necessary(self):
        if self.client_socket:
            self.client_socket.close()
        if self.server_socket:
            self.server_socket.close()

    def _send_pong_message(self):
        send_rllink_message(self.client_socket, {"type": RLlink.PONG.name})

    def _process_episodes_message(self, msg_type, msg_body):
        # On-policy training -> we have to block until we get a new `set_state` call
        # (b/c the learning step is done and we can send new weights back to all
        # clients).
        if msg_type == RLlink.EPISODES_AND_GET_STATE:
            self._blocked_on_state = True

        episodes = []
        for episode_state in msg_body["episodes"]:
            episode = self.episode_type.from_state(episode_state)
            episodes.append(episode.to_numpy())

        # Push episodes into the to-be-returned list (for `sample()` requests).
        with self._sample_lock:
            if isinstance(self._episode_chunks_to_return, list):
                self._episode_chunks_to_return.extend(episodes)
            else:
                self._episode_chunks_to_return = episodes

    def _send_set_state_message(self):
        send_rllink_message(
            self.client_socket,
            {
                "type": RLlink.SET_STATE.name,
                "state": self.get_state(inference_only=True),
            },
        )

    def _send_set_config_message(self):
        send_rllink_message(
            self.client_socket,
            {
                "type": RLlink.SET_CONFIG.name,
                # TODO (sven): We need AlgorithmConfig to be a `Checkpointable` with a
                #  msgpack'able state.
                "config": pickle.dumps(self.config.get_state()),
            },
        )
