from enum import Enum
from packaging.version import Version

from ray.rllib.utils.checkpoints import try_import_msgpack
from ray.util.annotations import DeveloperAPI


msgpack = None


@DeveloperAPI
class RLlink(Enum):
    PROTOCOL_VERSION = Version("0.0.1")

    # Requests: Client (external env) -> Server (RLlib).
    # ----
    # Ping command (initial handshake).
    PING = "PING"
    # List of episodes (similar to what an EnvRunner.sample() call would return).
    EPISODES = "EPISODES"
    # Request state (e.g. model weights).
    GET_STATE = "GET_STATE"
    # Request Algorithm config.
    GET_CONFIG = "GET_CONFIG"
    # Send episodes and request the next state update right after that.
    # Clients sending this message should wait for a SET_STATE message as an immediate
    # response. Useful for external samplers that must collect on-policy data.
    EPISODES_AND_GET_STATE = "EPISODES_AND_GET_STATE"

    # Responses: Server (RLlib) -> Client (external env).
    # ----
    # Pong response (initial handshake).
    PONG = "PONG"
    # Set state (e.g. model weights).
    SET_STATE = "SET_STATE"
    # Set Algorithm  config.
    SET_CONFIG = "SET_CONFIG"

    # @OldAPIStack (to be deprecated soon).
    ACTION_SPACE = "ACTION_SPACE"
    OBSERVATION_SPACE = "OBSERVATION_SPACE"
    GET_WORKER_ARGS = "GET_WORKER_ARGS"
    GET_WEIGHTS = "GET_WEIGHTS"
    REPORT_SAMPLES = "REPORT_SAMPLES"
    START_EPISODE = "START_EPISODE"
    GET_ACTION = "GET_ACTION"
    LOG_ACTION = "LOG_ACTION"
    LOG_RETURNS = "LOG_RETURNS"
    END_EPISODE = "END_EPISODE"

    def __str__(self):
        return self.name


@DeveloperAPI
def send_rllink_message(sock_, message: dict):
    """Sends a message to the client with a length header."""
    global msgpack
    if msgpack is None:
        msgpack = try_import_msgpack(error=True)

    body = msgpack.packb(message, use_bin_type=True)  # .encode("utf-8")
    header = str(len(body)).zfill(8).encode("utf-8")
    try:
        sock_.sendall(header + body)
    except Exception as e:
        raise ConnectionError(
            f"Error sending message {message} to server on socket {sock_}! "
            f"Original error was: {e}"
        )


@DeveloperAPI
def get_rllink_message(sock_):
    """Receives a message from the client following the length-header protocol."""
    global msgpack
    if msgpack is None:
        msgpack = try_import_msgpack(error=True)

    try:
        # Read the length header (8 bytes)
        header = _get_num_bytes(sock_, 8)
        msg_length = int(header.decode("utf-8"))
        # Read the message body
        body = _get_num_bytes(sock_, msg_length)
        # Decode JSON.
        message = msgpack.unpackb(body, raw=False)  # .loads(body.decode("utf-8"))
        # Check for proper protocol.
        if "type" not in message:
            raise ConnectionError(
                "Protocol Error! Message from peer does not contain `type` field."
            )
        return RLlink(message.pop("type")), message
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
