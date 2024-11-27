from enum import Enum

from ray.util.annotations import PublicAPI


@PublicAPI(stability="alpha")
class MessageTypes(Enum):
    # Requests: Client (external env) -> Server (RLlib).
    # ----
    # Ping command (initial handshake).
    PING = "ping"
    # List of episodes (similar to what an EnvRunner.sample() call would return).
    EPISODES = "episodes"
    # Request state (e.g. model weights).
    GET_STATE = "get_state"

    # Responses: Server (RLlib) -> Client (external env).
    # ----
    # Pong response (initial handshake).
    PONG = "pong"
    # Set state (e.g. model weights).
    SET_STATE = "set_state"

    # @OldAPIStack
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
