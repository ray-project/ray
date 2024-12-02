from enum import Enum

from ray.util.annotations import PublicAPI


@PublicAPI(stability="alpha")
class MessageTypes(Enum):
    # Requests: Client (external env) -> Server (RLlib).
    # ----
    # Ping command (initial handshake).
    PING = "PING"
    # List of episodes (similar to what an EnvRunner.sample() call would return).
    EPISODES = "EPISODES"
    # Request state (e.g. model weights).
    GET_STATE = "GET_STATE"
    # Request (relevant) config.
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
    # Set (relevant) config.
    SET_CONFIG = "SET_CONFIG"

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

    def __str__(self):
        return self.name
