import collections
import copy
from typing import Any, Optional, Union

import numpy as np

from ray.rllib.examples.envs.classes.multi_agent.footsies.game import constants
from ray.rllib.examples.envs.classes.multi_agent.footsies.game.proto import (
    footsies_service_pb2 as footsies_pb2,
)


class FootsiesEncoder:
    """Encoder class to generate observations from the game state"""

    def __init__(self, observation_delay: int):
        self._encoding_history = {
            agent_id: collections.deque(maxlen=int(observation_delay))
            for agent_id in ["p1", "p2"]
        }
        self.observation_delay = observation_delay
        self._last_common_state: Optional[np.ndarray] = None
        self._action_id_values = list(constants.FOOTSIES_ACTION_IDS.values())

    @staticmethod
    def encode_common_state(game_state: footsies_pb2.GameState) -> np.ndarray:
        p1_state, p2_state = game_state.player1, game_state.player2

        dist_x = np.abs(p1_state.player_position_x - p2_state.player_position_x) / 8.0

        return np.array(
            [
                dist_x,
            ],
            dtype=np.float32,
        )

    @staticmethod
    def _encode_input_buffer(
        input_buffer: list[int], last_n: Optional[int] = None
    ) -> np.ndarray:
        """Encodes the input buffer into a one-hot vector.

        :param input_buffer: The input buffer to encode
        :type input_buffer: list[int]
        :return: The encoded one-hot vector
        :rtype: np.ndarray
        """

        if last_n is not None:
            input_buffer = input_buffer[last_n:]

        ib_encoding = []
        for action_id in input_buffer:
            arr = [0] * (len(constants.ACTION_TO_BITS) + 1)
            arr[action_id] = 1
            ib_encoding.extend(arr)

        input_buffer_vector = np.asarray(ib_encoding, dtype=np.float32)

        return input_buffer_vector

    def encode(
        self,
        game_state: footsies_pb2.GameState,
    ) -> dict[str, Any]:
        """Encodes the game state into observations for all agents.

        :param game_state: The game state to encode
        :type game_state: footsies_pb2.GameState
        :return: The encoded observations for all agents.
        :rtype: dict[str, Any]
        """
        common_state = self.encode_common_state(game_state)
        p1_encoding = self.encode_player_state(game_state.player1)
        p2_encoding = self.encode_player_state(game_state.player2)

        observation_delay = min(
            self.observation_delay, len(self._encoding_history["p1"])
        )

        if observation_delay > 0:
            p1_delayed_encoding = self._encoding_history["p1"][-observation_delay]
            p2_delayed_encoding = self._encoding_history["p2"][-observation_delay]
        else:
            p1_delayed_encoding = copy.deepcopy(p1_encoding)
            p2_delayed_encoding = copy.deepcopy(p2_encoding)

        self._encoding_history["p1"].append(p1_encoding)
        self._encoding_history["p2"].append(p2_encoding)
        self._last_common_state = common_state

        # Create features dictionary
        features = {}
        current_index = 0

        # Common state
        features["common_state"] = {
            "start": current_index,
            "length": len(common_state),
        }
        current_index += len(common_state)

        # Concatenate the observations for the undelayed encoding
        p1_encoding = np.hstack(list(p1_encoding.values()), dtype=np.float32)
        p2_encoding = np.hstack(list(p2_encoding.values()), dtype=np.float32)

        # Concatenate the observations for the delayed encoding
        p1_delayed_encoding = np.hstack(
            list(p1_delayed_encoding.values()), dtype=np.float32
        )
        p2_delayed_encoding = np.hstack(
            list(p2_delayed_encoding.values()), dtype=np.float32
        )

        p1_centric_observation = np.hstack(
            [common_state, p1_encoding, p2_delayed_encoding]
        )

        p2_centric_observation = np.hstack(
            [common_state, p2_encoding, p1_delayed_encoding]
        )

        return {"p1": p1_centric_observation, "p2": p2_centric_observation}

    def encode_player_state(
        self,
        player_state: footsies_pb2.PlayerState,
    ) -> dict[str, Union[int, float, list, np.ndarray]]:
        """Encodes the player state into observations.

        :param player_state: The player state to encode
        :type player_state: footsies_pb2.PlayerState
        :return: The encoded observations for the player
        :rtype: dict[str, Any]
        """
        feature_dict = {
            "player_position_x": player_state.player_position_x
            / constants.FeatureDictNormalizers.PLAYER_POSITION_X,
            "velocity_x": player_state.velocity_x
            / constants.FeatureDictNormalizers.VELOCITY_X,
            "is_dead": int(player_state.is_dead),
            "vital_health": player_state.vital_health,
            "guard_health": one_hot_encoder(player_state.guard_health, [0, 1, 2, 3]),
            "current_action_id": self._encode_action_id(player_state.current_action_id),
            "current_action_frame": player_state.current_action_frame
            / constants.FeatureDictNormalizers.CURRENT_ACTION_FRAME,
            "current_action_frame_count": player_state.current_action_frame_count
            / constants.FeatureDictNormalizers.CURRENT_ACTION_FRAME_COUNT,
            "current_action_remaining_frames": (
                player_state.current_action_frame_count
                - player_state.current_action_frame
            )
            / constants.FeatureDictNormalizers.CURRENT_ACTION_REMAINING_FRAMES,
            "is_action_end": int(player_state.is_action_end),
            "is_always_cancelable": int(player_state.is_always_cancelable),
            "current_action_hit_count": player_state.current_action_hit_count,
            "current_hit_stun_frame": player_state.current_hit_stun_frame
            / constants.FeatureDictNormalizers.CURRENT_HIT_STUN_FRAME,
            "is_in_hit_stun": int(player_state.is_in_hit_stun),
            "sprite_shake_position": player_state.sprite_shake_position,
            "max_sprite_shake_frame": player_state.max_sprite_shake_frame
            / constants.FeatureDictNormalizers.MAX_SPRITE_SHAKE_FRAME,
            "is_face_right": int(player_state.is_face_right),
            "current_frame_advantage": player_state.current_frame_advantage
            / constants.FeatureDictNormalizers.CURRENT_FRAME_ADVANTAGE,
            # The below features leak some information about the opponent!
            "would_next_forward_input_dash": int(
                player_state.would_next_forward_input_dash
            ),
            "would_next_backward_input_dash": int(
                player_state.would_next_backward_input_dash
            ),
            "special_attack_progress": min(player_state.special_attack_progress, 1.0),
        }

        return feature_dict

    def get_last_encoding(self) -> Optional[dict[str, np.ndarray]]:
        if self._last_common_state is None:
            return None

        return {
            "common_state": self._last_common_state.reshape(-1),
            "p1": np.hstack(
                list(self._encoding_history["p1"][-1].values()),
                dtype=np.float32,
            ),
            "p2": np.hstack(
                list(self._encoding_history["p2"][-1].values()),
                dtype=np.float32,
            ),
        }

    def reset(self):
        self._encoding_history = {
            agent_id: collections.deque(maxlen=int(self.observation_delay))
            for agent_id in ["p1", "p2"]
        }

    def _encode_action_id(self, action_id: int) -> np.ndarray:
        """Encodes the action id into a one-hot vector.

        :param action_id: The action id to encode
        :type action_id: int
        :return: The encoded one-hot vector
        :rtype: np.ndarray
        """

        action_vector = np.zeros(len(self._action_id_values), dtype=np.float32)

        # Get the index of the action id in constants.ActionID
        action_index = self._action_id_values.index(action_id)
        action_vector[action_index] = 1

        assert action_vector.max() == 1 and action_vector.min() == 0

        return action_vector


def one_hot_encoder(
    value: Union[int, float, str], collection: list[Union[int, float, str]]
) -> np.ndarray:
    vector = np.zeros(len(collection), dtype=np.float32)
    vector[collection.index(value)] = 1
    return vector
