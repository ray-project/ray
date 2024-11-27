import socket
import struct
import json

import gymnasium as gym

from ray.rllib.env.env_runner import EnvRunner
from ray.rllib.env.single_agent_episode import SingleAgentEpisode
from ray.rllib.env.utils.external_env_protocol import MessageTypes
from ray.rllib.utils.annotations import ExperimentalAPI, override


@ExperimentalAPI
class ExternalTcpSingleAgentEnvRunner(EnvRunner):
    """A SingleAgentEnvRunner communicating with an external env through a TCP socket.

    This particular implementation assumes:
    - Only one external client ever connects to this env runner.
    - The external client performs inference locally through an ONNX model.
    - A copy of the RLModule is kept at all times on the env runner, but never used
    for inference, only as a data (weights) container.
    - There is no environment and no connectors on this env runner. The external env
    is responsible for generating the data to create episodes.
    """
    def __init__(self, *, config, host='localhost', port=5000, **kwargs):
        """
        Initializes a ExternalTcpSingleAgentEnvRunner instance.

        Args:
            config: The AlgorithmConfig to use for setup.
            host: Hostname or IP address to bind the server socket.
            port: Port number to bind the server socket.
            **kwargs: Additional arguments.
        """
        super().__init__(config=config, **kwargs)

        self.host = host
        self.port = port
        self.server_socket = None
        self.client_socket = None
        self.address = None

        # Start listening on the configured port.
        self._setup_server_socket()
        self._accept_client()

    @override(EnvRunner)
    def assert_healthy(self):
        """Checks that the client socket is connected."""
        assert self.client_socket is not None, "Client socket is not connected."

    @override(EnvRunner)
    def sample(
        self,
        *,
        num_timesteps: int = None,
        num_episodes: int = None,
        # explore: bool = None,  # TODO: support this?
        # random_actions: bool = False,  # TODO: support this?
        # force_reset: bool = False,  # TODO: support this?
    ):
        """Interacts with the client to collect samples."""
        # Send "sample" command to client.
        self._send_message({"type": MessageTypes.SAMPLE, "num_timesteps": num_timesteps})

        # Wait for response (blocking).
        message = self._recv_message()
        if message.get("type") == 'episodes':
            episodes_data = message.get('episodes', [])
            episodes = []
            for episode_data in episodes_data:
                episode = SingleAgentEpisode(
                    observation_space=None,  # Replace with actual observation space
                    action_space=None,       # Replace with actual action space
                )
                for step in episode_data:
                    episode.add_env_step(
                        observation=step["obs"],
                        action=step["action"],
                        reward=step["reward"],
                        infos=step.get("info", {}),
                        terminated=step.get("terminated", False),
                        truncated=step.get("truncated", False),
                    )
                episodes.append(episode.finalize())
            return episodes
        else:
            raise ValueError("Unexpected message type from client.")

    def get_spaces(self):
        """Returns the observation and action spaces."""
        # Return empty or appropriate spaces as needed
        return {}

    @override(EnvRunner)
    def stop(self):
        """Closes the client and server sockets."""
        if self.client_socket:
            self.client_socket.close()
        if self.server_socket:
            self.server_socket.close()

    def _setup_server_socket(self):
        """Sets up the server socket and binds to the specified host and port."""
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        # Allow reuse of the address.
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server_socket.bind((self.host, self.port))
        # Listen for a single connection.
        self.server_socket.listen(1)

    def _accept_client(self):
        """Accepts a client connection."""
        print("Waiting for client to connect...")
        self.client_socket, self.address = self.server_socket.accept()
        print(f"Connected to client at {self.address}")
        self._handle_initial_ping()

    def _handle_initial_ping(self):
        """Handles the initial 'ping' message from the client."""
        message = self._recv_message()
        if message.get('type') == "ping":
            self._send_message({"type": "pong"})
        else:
            raise ValueError("Expected 'ping' message from client.")

    def _recv_message(self):
        """Receives a message from the client following the length-header protocol."""
        # Read the length header (8 bytes)
        header = self._recv_all(8)
        if not header:
            raise ConnectionError("Failed to receive message length header.")
        # `>`: Big endian (most significant byte first); `Q`: uint64.
        (msg_length,) = struct.unpack(">Q", header)
        # Read the message body
        body = self._recv_all(msg_length)
        if not body:
            raise ConnectionError("Failed to receive message body.")
        # Decode JSON.
        message = json.loads(body.decode("utf-8"))
        # Check for proper protocol.
        if "type" not in message:
            raise
        return message

    def _recv_all(self, length):
        """Helper function to receive a specific number of bytes."""
        data = b''
        while len(data) < length:
            packet = self.client_socket.recv(length - len(data))
            if not packet:
                return None
            data += packet
        return data

    def _send_message(self, message: dict):
        """Sends a message to the client with a length header."""
        body = json.dumps(message).encode("utf-8")
        # `>`: Big endian (most significant byte first); `Q`: uint64.
        header = struct.pack(">Q", len(body))
        self.client_socket.sendall(header + body)


class _DummyCartPoleClient:
    """A dummy client that runs CartPole and acts as a testing external env."""

    def __init__(self):
        self.env = gym.make("CartPole-v1")
