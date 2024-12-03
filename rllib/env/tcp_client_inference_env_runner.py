import base64
from collections import defaultdict
import gzip
import json
import pathlib
import socket
import tempfile
import threading
import time
from typing import Collection, DefaultDict, List, Optional, Union

import gymnasium as gym
import numpy as np
import onnxruntime

from ray.rllib.core import (
    Columns,
    COMPONENT_RL_MODULE,
    DEFAULT_AGENT_ID,
    DEFAULT_MODULE_ID,
)
from ray.rllib.env import INPUT_ENV_SPACES
from ray.rllib.env.env_runner import EnvRunner
from ray.rllib.env.single_agent_env_runner import SingleAgentEnvRunner
from ray.rllib.env.single_agent_episode import SingleAgentEpisode
from ray.rllib.env.utils.external_env_protocol import MessageTypes
from ray.rllib.utils.annotations import ExperimentalAPI, override
from ray.rllib.utils.checkpoints import Checkpointable
from ray.rllib.utils.framework import try_import_torch
from ray.rllib.utils.metrics import (
    EPISODE_DURATION_SEC_MEAN,
    EPISODE_LEN_MAX,
    EPISODE_LEN_MEAN,
    EPISODE_LEN_MIN,
    EPISODE_RETURN_MAX,
    EPISODE_RETURN_MEAN,
    EPISODE_RETURN_MIN,
    WEIGHTS_SEQ_NO,
)
from ray.rllib.utils.metrics.metrics_logger import MetricsLogger
from ray.rllib.utils.numpy import softmax
from ray.rllib.utils.typing import EpisodeID, StateDict

torch, _ = try_import_torch()


@ExperimentalAPI
class TcpClientInferenceEnvRunner(EnvRunner, Checkpointable):
    """An EnvRunner communicating with an external env through a TCP socket.

    This implementation assumes:
    - Only one external client ever connects to this env runner.
    - The external client performs inference locally through an ONNX model. Thus,
    samples are sent in bulk once a certain number of timesteps has been executed on the
    client's side (no individual action requests).
    - A copy of the RLModule is kept at all times on the env runner, but never used
    for inference, only as a data (weights) container.
    TODO (sven): The above might be inefficient as we have to store basically two
     models, one in this EnvRunner, one in the env (as ONNX).
    - There is no environment and no connectors on this env runner. The external env
    is responsible for generating all the data to create episodes.
    """

    @override(EnvRunner)
    def __init__(self, *, config, **kwargs):
        """
        Initializes a TcpClientInferenceEnvRunner instance.

        Args:
            config: The AlgorithmConfig to use for setup.

        Keyword Args:
            port: The base port number. The server socket is then actually bound to
                `port` + self.worker_index.
        """
        super().__init__(config=config)

        self.worker_index: int = kwargs.get("worker_index", 0)

        self._weights_seq_no = 0

        # Build the module from its spec.
        module_spec = self.config.get_rl_module_spec(
            spaces=self.get_spaces(), inference_only=True
        )
        self.module = module_spec.build()

        self.host = "localhost"
        self.port = int(self.config.env_config.get("port", 5555)) + self.worker_index
        self.server_socket = None
        self.client_socket = None
        self.address = None

        self.metrics = MetricsLogger()

        self._episode_chunks_to_return: Optional[List[SingleAgentEpisode]] = None
        self._done_episodes_for_metrics: List[SingleAgentEpisode] = []
        self._ongoing_episodes_for_metrics: DefaultDict[
            EpisodeID, List[SingleAgentEpisode]
        ] = defaultdict(list)

        self._sample_lock = threading.Lock()
        self._on_policy_lock = threading.Lock()
        self._blocked_on_state = False

        # Start a background thread for client communication.
        self.thread = threading.Thread(
            target=self._client_message_listener, daemon=True
        )
        self.thread.start()

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
                            self._ongoing_episodes_for_metrics[eps.id_].append(eps)
                        num_env_steps += len(eps)

                    ret = self._episode_chunks_to_return
                    self._episode_chunks_to_return = None

                    SingleAgentEnvRunner._increase_sampled_metrics(
                        self, num_env_steps, num_episodes_completed
                    )

                    return ret
            time.sleep(0.01)

    @override(EnvRunner)
    def get_metrics(self):
        # TODO (sven): We should probably make this a utility function to be called
        #  from within Single/MultiAgentEnvRunner and other EnvRunner subclasses, as
        #  needed.
        # Compute per-episode metrics (only on already completed episodes).
        for eps in self._done_episodes_for_metrics:
            assert eps.is_done
            episode_length = len(eps)
            episode_return = eps.get_return()
            episode_duration_s = eps.get_duration_s()
            # Don't forget about the already returned chunks of this episode.
            if eps.id_ in self._ongoing_episodes_for_metrics:
                for eps2 in self._ongoing_episodes_for_metrics[eps.id_]:
                    episode_length += len(eps2)
                    episode_return += eps2.get_return()
                    episode_duration_s += eps2.get_duration_s()
                del self._ongoing_episodes_for_metrics[eps.id_]

            self._log_episode_metrics(
                episode_length, episode_return, episode_duration_s
            )

        # Now that we have logged everything, clear cache of done episodes.
        self._done_episodes_for_metrics.clear()

        # Return reduced metrics.
        return self.metrics.reduce()

    def get_spaces(self):
        return {
            INPUT_ENV_SPACES: (self.config.observation_space, self.config.action_space),
            DEFAULT_MODULE_ID: (
                self.config.observation_space,
                self.config.action_space,
            ),
        }

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
        return {}

    @override(Checkpointable)
    def set_state(self, state: StateDict) -> None:
        # Update the RLModule state.
        if COMPONENT_RL_MODULE in state:
            # A missing value for WEIGHTS_SEQ_NO or a value of 0 means: Force the
            # update.
            weights_seq_no = state.get(WEIGHTS_SEQ_NO, 0)

            # Only update the weigths, if this is the first synchronization or
            # if the weights of this `EnvRunner` lacks behind the actual ones.
            if weights_seq_no == 0 or self._weights_seq_no < weights_seq_no:
                rl_module_state = state[COMPONENT_RL_MODULE]
                if (
                    isinstance(rl_module_state, dict)
                    and DEFAULT_MODULE_ID in rl_module_state
                ):
                    rl_module_state = rl_module_state[DEFAULT_MODULE_ID]
                self.module.set_state(rl_module_state)

            # Update our weights_seq_no, if the new one is > 0.
            if weights_seq_no > 0:
                self._weights_seq_no = weights_seq_no

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
                msg_type, msg_body = _get_message(self.client_socket)

                # Process the message received based on its type.
                # Initial handshake.
                if msg_type == MessageTypes.PING:
                    self._send_pong_message()

                # Episode data from the client.
                elif msg_type in [
                    MessageTypes.EPISODES,
                    MessageTypes.EPISODES_AND_GET_STATE,
                ]:
                    self._process_episodes_message(msg_type, msg_body)

                # Client requests the state (model weights).
                elif msg_type == MessageTypes.GET_STATE:
                    self._send_set_state_message()

                # Clients requests some (relevant) config information.
                elif msg_type == MessageTypes.GET_CONFIG:
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
        _send_message(self.client_socket, {"type": MessageTypes.PONG.name})

    def _process_episodes_message(self, msg_type, msg_body):
        # On-policy training -> we have to block until we get a new `set_state` call
        # (b/c the learning step is done and we can sent new weights back to all
        # clients).
        if msg_type == MessageTypes.EPISODES_AND_GET_STATE:
            self._blocked_on_state = True

        episodes = []
        for episode_data in msg_body["episodes"]:
            episode = SingleAgentEpisode(
                observation_space=self.config.observation_space,
                observations=[np.array(o) for o in episode_data[Columns.OBS]],
                action_space=self.config.action_space,
                actions=episode_data[Columns.ACTIONS],
                rewards=episode_data[Columns.REWARDS],
                extra_model_outputs={
                    Columns.ACTION_DIST_INPUTS: [
                        np.array(a) for a in episode_data[Columns.ACTION_DIST_INPUTS]
                    ],
                    Columns.ACTION_LOGP: episode_data[Columns.ACTION_LOGP],
                },
                terminated=episode_data[Columns.TERMINATEDS],
                truncated=episode_data[Columns.TRUNCATEDS],
                len_lookback_buffer=0,
            )
            episodes.append(episode.finalize())

        # Push episodes into the to-be-returned list (for `sample()` requests).
        with self._sample_lock:
            if isinstance(self._episode_chunks_to_return, list):
                self._episode_chunks_to_return.extend(episodes)
            else:
                self._episode_chunks_to_return = episodes

    def _send_set_state_message(self):
        with tempfile.TemporaryDirectory() as dir:
            onnx_file = pathlib.Path(dir) / "_temp_model.onnx"
            torch.onnx.export(
                self.module,
                {
                    "batch": {
                        "obs": torch.randn(1, *self.config.observation_space.shape)
                    }
                },
                onnx_file,
                export_params=True,
            )
            with open(onnx_file, "rb") as f:
                compressed = gzip.compress(f.read())
                onnx_binary = base64.b64encode(compressed).decode("utf-8")
        _send_message(
            self.client_socket,
            {
                "type": MessageTypes.SET_STATE.name,
                "onnx_file": onnx_binary,
                WEIGHTS_SEQ_NO: self._weights_seq_no,
            },
        )

    def _send_set_config_message(self):
        _send_message(
            self.client_socket,
            {
                "type": MessageTypes.SET_CONFIG.name,
                "env_steps_per_sample": self.config.get_rollout_fragment_length(
                    worker_index=self.worker_index
                ),
                "force_on_policy": True,
            },
        )

    def _log_episode_metrics(self, length, ret, sec):
        # Log general episode metrics.
        # To mimic the old API stack behavior, we'll use `window` here for
        # these particular stats (instead of the default EMA).
        win = self.config.metrics_num_episodes_for_smoothing
        self.metrics.log_value(EPISODE_LEN_MEAN, length, window=win)
        self.metrics.log_value(EPISODE_RETURN_MEAN, ret, window=win)
        self.metrics.log_value(EPISODE_DURATION_SEC_MEAN, sec, window=win)
        # Per-agent returns.
        self.metrics.log_value(
            ("agent_episode_returns_mean", DEFAULT_AGENT_ID), ret, window=win
        )
        # Per-RLModule returns.
        self.metrics.log_value(
            ("module_episode_returns_mean", DEFAULT_MODULE_ID), ret, window=win
        )

        # For some metrics, log min/max as well.
        self.metrics.log_value(EPISODE_LEN_MIN, length, reduce="min", window=win)
        self.metrics.log_value(EPISODE_RETURN_MIN, ret, reduce="min", window=win)
        self.metrics.log_value(EPISODE_LEN_MAX, length, reduce="max", window=win)
        self.metrics.log_value(EPISODE_RETURN_MAX, ret, reduce="max", window=win)


def _send_message(sock_, message: dict):
    """Sends a message to the client with a length header."""
    body = json.dumps(message).encode("utf-8")
    header = str(len(body)).zfill(8).encode("utf-8")
    try:
        sock_.sendall(header + body)
    except Exception as e:
        raise ConnectionError(
            f"Error sending message {message} to server on socket {sock_}! "
            f"Original error was: {e}"
        )


def _get_message(sock_):
    """Receives a message from the client following the length-header protocol."""
    try:
        # Read the length header (8 bytes)
        header = _get_num_bytes(sock_, 8)
        msg_length = int(header.decode("utf-8"))
        # Read the message body
        body = _get_num_bytes(sock_, msg_length)
        # Decode JSON.
        message = json.loads(body.decode("utf-8"))
        # Check for proper protocol.
        if "type" not in message:
            raise ConnectionError(
                "Protocol Error! Message from peer does not contain `type` " "field."
            )
        return MessageTypes(message.pop("type")), message
    except Exception as e:
        raise ConnectionError(
            f"Error receiving message from peer on socket {sock_}! "
            f"Original error was: {e}"
        )


def _get_num_bytes(sock_, num_bytes):
    """Helper function to receive a specific number of bytes."""
    data = b""
    while len(data) < num_bytes:
        packet = sock_.recv(num_bytes - len(data))
        if not packet:
            raise ConnectionError(f"No data received from socket {sock_}!")
        data += packet
    return data


def _dummy_client(port: int = 5556):
    """A dummy client that runs CartPole and acts as a testing external env."""

    def _set_state(msg_body):
        with tempfile.TemporaryDirectory():
            with open("_temp_onnx", "wb") as f:
                f.write(
                    gzip.decompress(
                        base64.b64decode(msg_body["onnx_file"].encode("utf-8"))
                    )
                )
                onnx_session = onnxruntime.InferenceSession("_temp_onnx")
                output_names = [o.name for o in onnx_session.get_outputs()]
        return onnx_session, output_names

    # Connect to server.
    while True:
        try:
            print(f"Trying to connect to localhost:{port} ...")
            sock_ = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock_.connect(("localhost", port))
            break
        except ConnectionRefusedError:
            time.sleep(5)

    # Send ping-pong.
    _send_message(sock_, {"type": MessageTypes.PING.name})
    msg_type, msg_body = _get_message(sock_)
    assert msg_type == MessageTypes.PONG

    # Request config.
    _send_message(sock_, {"type": MessageTypes.GET_CONFIG.name})
    msg_type, msg_body = _get_message(sock_)
    assert msg_type == MessageTypes.SET_CONFIG
    env_steps_per_sample = msg_body["env_steps_per_sample"]
    force_on_policy = msg_body["force_on_policy"]

    # Request ONNX weights.
    _send_message(sock_, {"type": MessageTypes.GET_STATE.name})
    msg_type, msg_body = _get_message(sock_)
    assert msg_type == MessageTypes.SET_STATE
    onnx_session, output_names = _set_state(msg_body)

    # Episode collection buckets.
    episodes = []
    observations = []
    actions = []
    action_dist_inputs = []
    action_logps = []
    rewards = []

    timesteps = 0
    episode_return = 0.0

    # Start actual env loop.
    env = gym.make("CartPole-v1")
    obs, info = env.reset()
    observations.append(obs.tolist())

    while True:
        timesteps += 1
        # Perform action inference using the ONNX model.
        logits = onnx_session.run(
            output_names,
            {"onnx::Gemm_0": np.array([obs], np.float32)},
        )[0][
            0
        ]  # [0]=first return item, [0]=batch size 1

        # Stochastic sample.
        action_probs = softmax(logits)
        action = int(np.random.choice(list(range(env.action_space.n)), p=action_probs))
        logp = float(np.log(action_probs[action]))

        # Perform the env step.
        obs, reward, terminated, truncated, info = env.step(action)

        # Collect step data.
        observations.append(obs.tolist())
        actions.append(action)
        action_dist_inputs.append(logits.tolist())
        action_logps.append(logp)
        rewards.append(reward)
        episode_return += reward

        # We have to create a new episode record.
        if timesteps == env_steps_per_sample or terminated or truncated:
            episodes.append(
                {
                    Columns.OBS: observations,
                    Columns.ACTIONS: actions,
                    Columns.ACTION_DIST_INPUTS: action_dist_inputs,
                    Columns.ACTION_LOGP: action_logps,
                    Columns.REWARDS: rewards,
                    Columns.TERMINATEDS: terminated,
                    Columns.TRUNCATEDS: truncated,
                }
            )
            # We collected enough samples -> Send them to server.
            if timesteps == env_steps_per_sample:
                # Make sure the amount of data we collected is correct.
                assert sum(len(e["actions"]) for e in episodes) == env_steps_per_sample

                # Send the data to the server.
                if force_on_policy:
                    _send_message(
                        sock_,
                        {
                            "type": MessageTypes.EPISODES_AND_GET_STATE.name,
                            "episodes": episodes,
                            "timesteps": timesteps,
                        },
                    )
                    # We are forced to sample on-policy. Have to wait for a response
                    # with the state (weights) in it.
                    msg_type, msg_body = _get_message(sock_)
                    assert msg_type == MessageTypes.SET_STATE
                    onnx_session, output_names = _set_state(msg_body)

                # Sampling doesn't have to be on-policy -> continue collecting
                # samples.
                else:
                    raise NotImplementedError

                episodes = []
                timesteps = 0

            # Set new buckets to empty lists (for next episode).
            observations = [observations[-1]]
            actions = []
            action_dist_inputs = []
            action_logps = []
            rewards = []

            # The episode is done -> Reset.
            if terminated or truncated:
                obs, _ = env.reset()
                observations = [obs.tolist()]
                episode_return = 0.0
