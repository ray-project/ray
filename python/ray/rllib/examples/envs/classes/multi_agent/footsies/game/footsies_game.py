import logging
import time

import grpc
import numpy as np

import ray.rllib.examples.envs.classes.multi_agent.footsies.game.proto.footsies_service_pb2 as footsies_pb2
import ray.rllib.examples.envs.classes.multi_agent.footsies.game.proto.footsies_service_pb2_grpc as footsies_pb2_grpc
from ray.rllib.examples.envs.classes.multi_agent.footsies.game import constants

logger = logging.getLogger(__name__)


class FootsiesGame:
    """Handles gRPC communication with game the server.

    This class establishes communication between the
    game server and the Python harness via gRPC. It provides methods
    to start the game, reset it, get the current state, and step the
    game by a certain number of frames.
    """

    def __init__(self, host: str, port: int):
        self.host = host
        self.port = port
        self.stub = self._initialize_stub()

    @staticmethod
    def action_to_bits(action: int, is_player_1: bool) -> int:
        """Converts an action to its corresponding bit representation."""

        if isinstance(action, np.ndarray):
            action = action.item()

        if is_player_1:
            if action == constants.EnvActions.BACK:
                action = constants.GameActions.LEFT
            elif action == constants.EnvActions.FORWARD:
                action = constants.GameActions.RIGHT
            elif action == constants.EnvActions.BACK_ATTACK:
                action = constants.GameActions.LEFT_ATTACK
            elif action == constants.EnvActions.FORWARD_ATTACK:
                action = constants.GameActions.RIGHT_ATTACK
        else:
            if action == constants.EnvActions.BACK:
                action = constants.GameActions.RIGHT
            elif action == constants.EnvActions.FORWARD:
                action = constants.GameActions.LEFT
            elif action == constants.EnvActions.BACK_ATTACK:
                action = constants.GameActions.RIGHT_ATTACK
            elif action == constants.EnvActions.FORWARD_ATTACK:
                action = constants.GameActions.LEFT_ATTACK

        return constants.ACTION_TO_BITS[action]

    def get_encoded_state(self) -> footsies_pb2.EncodedGameState:
        """Gets the current encoded game state by calling the GetEncodedState RPC."""
        try:
            return self.stub.GetEncodedState(footsies_pb2.Empty())
        except Exception as e:
            logger.error(f"Error calling GetEncodedState with exception: {e}")
            raise e

    def get_state(self) -> footsies_pb2.GameState:
        """Gets the current game state by calling the GetState RPC."""
        try:
            return self.stub.GetState(footsies_pb2.Empty())
        except Exception as e:
            logger.error(f"Error calling GetState with exception: {e}")
            raise e

    def is_ready(self) -> bool:
        """Checks if the game is ready by calling the IsReady RPC."""
        try:
            return self.stub.IsReady(footsies_pb2.Empty()).value
        except Exception as e:
            logger.error(f"Error calling IsReady with exception: {e}")
            raise e

    def reset_game(self) -> None:
        """Resets the game by calling the ResetGame RPC."""
        try:
            self.stub.ResetGame(footsies_pb2.Empty())
        except Exception as e:
            logger.error(f"Error calling ResetGame with exception: {e}")
            raise e

    def start_game(self) -> None:
        """Starts the game by calling the StartGame RPC."""
        try:
            self.stub.StartGame(footsies_pb2.Empty())

            while not self.is_ready():
                logger.info("Game not ready...")
                time.sleep(0.5)
            logger.info("StartGame called successfully")

        except Exception as e:
            logger.error(f"Error calling StartGame with exception: {e}")
            raise e

    def step_n_frames(
        self, p1_action: int, p2_action: int, n_frames: int
    ) -> footsies_pb2.GameState:
        """Steps the game by n_frames with the given player actions. The provided actions will be repeated for all n_frames."""
        try:
            step_input = footsies_pb2.StepInput(
                p1_action=p1_action, p2_action=p2_action, nFrames=n_frames
            )
            return self.stub.StepNFrames(step_input)
        except Exception as e:
            logger.error(f"Error calling StepNFrames with exception: {e}")
            raise e

    def _initialize_stub(self) -> footsies_pb2_grpc.FootsiesGameServiceStub:
        try:
            channel = grpc.insecure_channel(f"{self.host}:{self.port}")
            return footsies_pb2_grpc.FootsiesGameServiceStub(channel)
        except grpc.RpcError as e:
            logger.error(f"Error connecting to gRPC stub with exception: {e}")
            raise e
