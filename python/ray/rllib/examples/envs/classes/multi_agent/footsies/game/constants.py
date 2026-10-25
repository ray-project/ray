from dataclasses import dataclass

OBSERVATION_SPACE_SIZE: int = 81


@dataclass
class EnvActions:
    NONE = 0
    BACK = 1
    FORWARD = 2
    ATTACK = 3
    BACK_ATTACK = 4
    FORWARD_ATTACK = 5
    SPECIAL_CHARGE = 6


@dataclass
class GameActions:
    NONE = 0
    LEFT = 1
    RIGHT = 2
    ATTACK = 3
    LEFT_ATTACK = 4
    RIGHT_ATTACK = 5


@dataclass
class ActionBits:
    NONE: int = 0
    LEFT: int = 1 << 0
    RIGHT: int = 1 << 1
    ATTACK: int = 1 << 2
    LEFT_ATTACK: int = LEFT | ATTACK
    RIGHT_ATTACK: int = RIGHT | ATTACK


@dataclass
class ActionID:
    STAND = 0
    FORWARD = 1
    BACKWARD = 2
    DASH_FORWARD = 10
    DASH_BACKWARD = 11
    N_ATTACK = 100
    B_ATTACK = 105
    N_SPECIAL = 110
    B_SPECIAL = 115
    DAMAGE = 200
    GUARD_M = 301
    GUARD_STAND = 305
    GUARD_CROUCH = 306
    GUARD_BREAK = 310
    GUARD_PROXIMITY = 350
    DEAD = 500
    WIN = 510


@dataclass
class FeatureDictNormalizers:
    PLAYER_POSITION_X = 4.0
    VELOCITY_X = 5.0
    CURRENT_ACTION_FRAME = 25
    CURRENT_ACTION_FRAME_COUNT = 25
    CURRENT_ACTION_REMAINING_FRAMES = 25
    CURRENT_HIT_STUN_FRAME = 10
    MAX_SPRITE_SHAKE_FRAME = 10
    CURRENT_FRAME_ADVANTAGE = 10


ACTION_TO_BITS = {
    GameActions.NONE: ActionBits.NONE,
    GameActions.LEFT: ActionBits.LEFT,
    GameActions.RIGHT: ActionBits.RIGHT,
    GameActions.ATTACK: ActionBits.ATTACK,
    GameActions.LEFT_ATTACK: ActionBits.LEFT_ATTACK,
    GameActions.RIGHT_ATTACK: ActionBits.RIGHT_ATTACK,
}

FOOTSIES_ACTION_IDS = {
    "STAND": ActionID.STAND,
    "FORWARD": ActionID.FORWARD,
    "BACKWARD": ActionID.BACKWARD,
    "DASH_FORWARD": ActionID.DASH_FORWARD,
    "DASH_BACKWARD": ActionID.DASH_BACKWARD,
    "N_ATTACK": ActionID.N_ATTACK,
    "B_ATTACK": ActionID.B_ATTACK,
    "N_SPECIAL": ActionID.N_SPECIAL,
    "B_SPECIAL": ActionID.B_SPECIAL,
    "DAMAGE": ActionID.DAMAGE,
    "GUARD_M": ActionID.GUARD_M,
    "GUARD_STAND": ActionID.GUARD_STAND,
    "GUARD_CROUCH": ActionID.GUARD_CROUCH,
    "GUARD_BREAK": ActionID.GUARD_BREAK,
    "GUARD_PROXIMITY": ActionID.GUARD_PROXIMITY,
    "DEAD": ActionID.DEAD,
    "WIN": ActionID.WIN,
}

# backup file location (uploaded July 29th, 2025):
# https://ray-example-data.s3.us-west-2.amazonaws.com/rllib/env-footsies/feature_indices.json
# Dictionary mapping feature names to their index ranges within a flat observation vector.
# Each key is a feature name, and its value is a dictionary with keys:
#   "start": the starting index in the observation array.
#   "length": it's length in bytes
feature_indices = {
    "common_state": {"start": 0, "length": 1},
    "frame_count": {"start": 1, "length": 1},
    "player_position_x": {"start": 2, "length": 1},
    "velocity_x": {"start": 3, "length": 1},
    "is_dead": {"start": 4, "length": 1},
    "vital_health": {"start": 5, "length": 1},
    "guard_health": {"start": 6, "length": 4},
    "current_action_id": {"start": 10, "length": 17},
    "current_action_frame": {"start": 27, "length": 1},
    "current_action_frame_count": {"start": 28, "length": 1},
    "current_action_remaining_frames": {"start": 29, "length": 1},
    "is_action_end": {"start": 30, "length": 1},
    "is_always_cancelable": {"start": 31, "length": 1},
    "current_action_hit_count": {"start": 32, "length": 1},
    "current_hit_stun_frame": {"start": 33, "length": 1},
    "is_in_hit_stun": {"start": 34, "length": 1},
    "sprite_shake_position": {"start": 35, "length": 1},
    "max_sprite_shake_frame": {"start": 36, "length": 1},
    "is_face_right": {"start": 37, "length": 1},
    "current_frame_advantage": {"start": 38, "length": 1},
    "would_next_forward_input_dash": {"start": 39, "length": 1},
    "would_next_backward_input_dash": {"start": 40, "length": 1},
    "special_attack_progress": {"start": 41, "length": 1},
    "opponent_frame_count": {"start": 42, "length": 1},
    "opponent_player_position_x": {"start": 43, "length": 1},
    "opponent_velocity_x": {"start": 44, "length": 1},
    "opponent_is_dead": {"start": 45, "length": 1},
    "opponent_vital_health": {"start": 46, "length": 1},
    "opponent_guard_health": {"start": 47, "length": 4},
    "opponent_current_action_id": {"start": 51, "length": 17},
    "opponent_current_action_frame": {"start": 68, "length": 1},
    "opponent_current_action_frame_count": {"start": 69, "length": 1},
    "opponent_current_action_remaining_frames": {"start": 70, "length": 1},
    "opponent_is_action_end": {"start": 71, "length": 1},
    "opponent_is_always_cancelable": {"start": 72, "length": 1},
    "opponent_current_action_hit_count": {"start": 73, "length": 1},
    "opponent_current_hit_stun_frame": {"start": 74, "length": 1},
    "opponent_is_in_hit_stun": {"start": 75, "length": 1},
    "opponent_sprite_shake_position": {"start": 76, "length": 1},
    "opponent_max_sprite_shake_frame": {"start": 77, "length": 1},
    "opponent_is_face_right": {"start": 78, "length": 1},
    "opponent_current_frame_advantage": {"start": 79, "length": 1},
    "opponent_would_next_forward_input_dash": {"start": 80, "length": 1},
    "opponent_would_next_backward_input_dash": {"start": 81, "length": 1},
    "opponent_special_attack_progress": {"start": 82, "length": 1},
}
