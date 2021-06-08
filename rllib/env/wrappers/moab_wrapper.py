"""
Simulator for the Moab plate+ball balancing device.
"""
__author__ = "Edilmo Palencia"
__copyright__ = "Copyright 2021, Microsoft Corp."

import logging
import math
from typing import Optional, Union, List, Dict, Tuple, Any, Type

import gym
import numpy as np
from pyrr import matrix33, vector

from ray.rllib.env.wrappers.moabsim.moab_model import MoabModel, \
    clamp, PLATE_ORIGIN_TO_SURFACE_OFFSET
from ray.tune import register_env
from ray.tune.registry import ENV_CREATOR, _global_registry


log = logging.getLogger(__name__)


class _MoabBaseWrapper(gym.Env):
    # Distances measured in meters
    RadiusOfPlate = 0.1125  # m

    # Threshold for ball placement
    CloseEnough = 0.02

    # Velocities measured in meters per sec.
    MaxVelocity = 1.0

    # Ping-Pong ball constants
    PingPongRadius = 0.020  # m
    PingPongShell = 0.0002  # m

    # Cushion value in avoiding obstacle
    Cushion = 0.01

    # Obstacle definitions
    ObstacleRadius = 0.01
    ObstacleLocationX = 0.04
    ObstacleLocationY = 0.04

    MaxIterationCount = 250

    def __init__(self, seed: Optional[int] = None):
        super().__init__()

        self.model = MoabModel()
        self._episode_count = 0
        self.model.reset()

        state_fields_count = len(self.get_state_field_list())
        state_high = np.array([np.inf] * state_fields_count,
                        dtype=np.float32)
        self.observation_space = gym.spaces.Box(-state_high, state_high,
                                                dtype=np.float32)
        if self._is_discrete():
            self.action_space = gym.spaces.Discrete(self._n_discrete())
        else:
            if self._include_action_input_height_z():
                action_high = np.array([1.0, 1.0, 1.0])
                self.action_space = gym.spaces.Box(-action_high, action_high,
                                                   dtype=np.float32)
            else:
                action_high = np.array([1.0, 1.0])
                self.action_space = gym.spaces.Box(-action_high, action_high,
                                                   dtype=np.float32)

        self.seed(seed)
        self.viewer = None

        # new episode, iteration count reset
        self.steps_beyond_done = 0
        self.iteration_count = 0
        self._episode_count += 1

        # Domain randomization options
        self._randomize_ball = False
        self._randomize_obstacle = False

        # Config fields keeping track of initial conditions decided
        # in the reset method
        self._initial_x = 0.0
        self._initial_y = 0.0
        self._initial_vel_x = 0.0
        self._initial_vel_y = 0.0
        self._initial_pitch = 0.0
        self._initial_roll = 0.0

    def set_randomize_ball(self, randomize_ball: bool):
        self._randomize_ball = randomize_ball

    def set_randomize_obstacle(self, randomize_obstacle: bool):
        self._randomize_obstacle = randomize_obstacle

    CONFIG_INITIAL_X = "initial_x"
    CONFIG_INITIAL_Y = "initial_y"
    CONFIG_INITIAL_VEL_X = "initial_vel_x"
    CONFIG_INITIAL_VEL_Y = "initial_vel_y"
    CONFIG_INITIAL_PITCH = "initial_pitch"
    CONFIG_INITIAL_ROLL = "initial_roll"

    @classmethod
    def get_config_field_list(cls) -> List[str]:
        """Provide the list of config fields that are going to be used
        as initial condition, out of the full list of fields handle by
        the MoabModel. The values for the config are included as part
        of the info dict return in each iteration.
        """
        return [
            cls.CONFIG_INITIAL_X,
            cls.CONFIG_INITIAL_Y,
            cls.CONFIG_INITIAL_VEL_X,
            cls.CONFIG_INITIAL_VEL_Y,
            cls.CONFIG_INITIAL_PITCH,
            cls.CONFIG_INITIAL_ROLL,
        ]

    @classmethod
    def get_state_field_list(cls) -> List[str]:
        """Provide the list of state fields that are going to be used
        out of the full list of fields handle by the MoabModel.
        """
        raise NotImplementedError

    @classmethod
    def get_action_field_list(cls) -> List[str]:
        return ["input_roll", "input_pitch"]

    @classmethod
    def _include_action_input_height_z(cls) -> bool:
        """Indicates if the action on the Z dimension(the height) is
        going to be considered."""
        return False

    @classmethod
    def _is_discrete(cls) -> bool:
        """Indicates if the training will be done with discrete actions"""
        return False

    @classmethod
    def _n_discrete(cls) -> int:
        """Amount of discrete actions"""
        return 0

    @classmethod
    def _filter_state(cls, state: Dict[str, float]) -> List[float]:
        """Return the list of state fields that are actually used."""
        filtered_state = []
        for k in cls.get_state_field_list():
            filtered_state.append(state[k])
        return filtered_state

    def _reach_max_iteration_limit(self) -> bool:
        """Indicates if episode reach the max amount of iterations."""
        return self.iteration_count >= _MoabBaseWrapper.MaxIterationCount

    def _is_done(self) -> bool:
        """Indicates if episode is done."""
        done = self.model.halted() or self._reach_max_iteration_limit()
        return done

    def _get_reward(self) -> float:
        """Compute the reward.
        """
        raise NotImplementedError

    def seed(self, seed: Optional[int] = None):
        self.np_random, seed = gym.utils.seeding.np_random(seed)
        return [seed]

    def step(self, action: Union[List[float], np.ndarray]) \
        -> Tuple[np.ndarray, float, bool, Dict[Any, Any]]:
        if not self._is_discrete():
            err_msg = "%r (%s) invalid" % (action, type(action))
            assert self.action_space.contains(action), err_msg

        was_done = self._is_done()

        self.iteration_count += 1
        if was_done:
            self.steps_beyond_done += 1

        # Convert to Python types
        if not isinstance(action, list):
            action = action.tolist()
        # input_roll is the first dimension
        self.model.roll = action[0]
        # input_pitch is the second dimension
        self.model.pitch = action[1]

        # clamp inputs to legal ranges
        self.model.roll = clamp(self.model.roll, -1.0, 1.0)
        self.model.pitch = clamp(self.model.pitch, -1.0, 1.0)

        if self._include_action_input_height_z():
            self.model.height_z = clamp(action[2], -1.0, 1.0)

        self.model.step()

        state = self.model.state()
        state = self._filter_state(state)
        done = self._is_done()
        reward = self._get_reward()
        info = {
            self.CONFIG_INITIAL_X: self._initial_x,
            self.CONFIG_INITIAL_Y: self._initial_y,
            self.CONFIG_INITIAL_VEL_X: self._initial_vel_x,
            self.CONFIG_INITIAL_VEL_Y: self._initial_vel_y,
            self.CONFIG_INITIAL_PITCH: self._initial_pitch,
            self.CONFIG_INITIAL_ROLL: self._initial_roll,
        }
        return np.array(state), reward, done, info

    def reset(self):
        # new episode, iteration count reset
        self.steps_beyond_done = 0
        self.iteration_count = 0
        self._episode_count += 1

        # return to known good state to avoid accidental episode-episode dependencies
        self.model.reset()

        self._initial_x = self.np_random.uniform(low=-_MoabBaseWrapper.RadiusOfPlate * 0.5,
                                           high=_MoabBaseWrapper.RadiusOfPlate * 0.5)
        self._initial_y = self.np_random.uniform(low=-_MoabBaseWrapper.RadiusOfPlate * 0.5,
                                           high=_MoabBaseWrapper.RadiusOfPlate * 0.5)
        # initial ball state after updating plate
        self.model.set_initial_ball(self._initial_x, self._initial_y, self.model.ball.z)

        self._initial_vel_x = self.np_random.uniform(low=-_MoabBaseWrapper.MaxVelocity * 0.02,
                                               high=_MoabBaseWrapper.MaxVelocity * 0.02)
        self._initial_vel_y = self.np_random.uniform(low=-_MoabBaseWrapper.MaxVelocity * 0.02,
                                               high=_MoabBaseWrapper.MaxVelocity * 0.02)
        # velocity set as a vector
        self.model.ball_vel.x = self._initial_vel_x
        self.model.ball_vel.y = self._initial_vel_y

        self._initial_pitch = self.np_random.uniform(low=-0.2, high=0.2)
        self._initial_roll = self.np_random.uniform(low=-0.2, high=0.2)
        # initial control state. these are all [-1..1] unitless
        self.model.roll = self._initial_roll
        self.model.pitch = self._initial_pitch

        if self._randomize_ball:
            # Domain randomize the ping pong ball parameters
            ball_radius = self.np_random.uniform(low=_MoabBaseWrapper.PingPongRadius * 0.8,
                                               high=_MoabBaseWrapper.PingPongRadius * 1.2)
            ball_shell = self.np_random.uniform(low=_MoabBaseWrapper.PingPongShell * 0.8,
                                               high=_MoabBaseWrapper.PingPongShell * 1.2)
            self.model.ball_radius = ball_radius
            self.model.ball_shell = ball_shell

        if self._randomize_obstacle:
            min_obstacle_radius = (_MoabBaseWrapper.ObstacleRadius * 0.5)
            max_obstacle_radius = (_MoabBaseWrapper.ObstacleRadius * 0.8)
            assert max_obstacle_radius > min_obstacle_radius
            obstacle_radius = self.np_random.uniform(low=min_obstacle_radius,
                                                     high=max_obstacle_radius)
            self.model.obstacle_radius = obstacle_radius
            min_obstacle_pos = (_MoabBaseWrapper.CloseEnough
                                + (2 * self.model.ball_radius)
                                + self.model.obstacle_radius)
            max_obstacle_pos = (_MoabBaseWrapper.RadiusOfPlate
                                - self.model.obstacle_radius)
            assert max_obstacle_pos > min_obstacle_pos
            abs_obstacle_x = self.np_random.uniform(low=min_obstacle_pos,
                                                high=max_obstacle_pos)
            abs_obstacle_y = self.np_random.uniform(low=min_obstacle_pos,
                                                high=max_obstacle_pos)
            obstacle_x = (abs_obstacle_x if self.np_random.choice([True, False])
                          else -abs_obstacle_x)
            obstacle_y = (abs_obstacle_y if self.np_random.choice([True, False])
                          else -abs_obstacle_y)
            self.model.obstacle_x = obstacle_x
            self.model.obstacle_y = obstacle_y

        state = self.model.state()
        state = self._filter_state(state)
        return np.array(state)

    def _set_velocity_for_speed_and_direction(self, speed: float, direction: float):
        # get the heading
        dx = self.model.target_x - self.model.ball.x
        dy = self.model.target_y - self.model.ball.y

        # direction is meaningless if we're already at the target
        if (dx != 0) or (dy != 0):

            # set the magnitude
            vel = vector.set_length([dx, dy, 0.0], speed)

            # rotate by direction around Z-axis at ball position
            rot = matrix33.create_from_axis_rotation([0.0, 0.0, 1.0], direction)
            vel = matrix33.apply_to_vector(rot, vel)

            # unpack into ball velocity
            self.model.ball_vel.x = vel[0]
            self.model.ball_vel.y = vel[1]
            self.model.ball_vel.z = vel[2]


class MoabMoveToCenterWrapper(_MoabBaseWrapper):

    @classmethod
    def get_state_field_list(cls) -> List[str]:
        return ["ball_x", "ball_y", "ball_vel_x", "ball_vel_y"]

    def _get_distance_to_target(self) -> float:
        # ball.z relative to plate
        zpos = self.model.ball.z - (
            self.model.plate.z + self.model.ball_radius + PLATE_ORIGIN_TO_SURFACE_OFFSET
        )
        # ball distance from ball position on plate at origin
        distance_to_center = math.sqrt(
            math.pow(self.model.ball.x, 2.0)
            + math.pow(self.model.ball.y, 2.0)
            + math.pow(zpos, 2.0)
        )
        return distance_to_center

    def _get_shaped_reward(self) -> float:
        distance_to_center = self._get_distance_to_target()
        return MoabMoveToCenterWrapper.CloseEnough / distance_to_center * 10

    def _get_reward(self) -> float:
        # Return a negative reward if the ball is off the plate
        # or we have hit the max iteration count for the episode.
        done = self._is_done()
        if done:
            return -10

        distance_to_center = self._get_distance_to_target()
        # speed = math.hypot(self.model.ball_vel.x, self.model.ball_vel.y)

        if distance_to_center < MoabMoveToCenterWrapper.CloseEnough:
            return 10

        # Shape the reward.
        return self._get_shaped_reward()


class MoabMoveToCenterDiscreteWrapper(MoabMoveToCenterWrapper):

    values = [-0.9, -0.7, -0.5, -0.3, -0.2, -0.1, -0.09, -0.08, -0.07, -0.06,
                    -0.05, -0.04, -0.03, -0.02, -0.01, 0.0, 0.09, 0.08, 0.07, 0.06,
                    0.05, 0.04, 0.03, 0.02, 0.01, 0.1, 0.2, 0.3, 0.5, 0.7, 0.9]

    @classmethod
    def get_action_field_list(cls) -> List[str]:
        return []

    @classmethod
    def _is_discrete(cls) -> bool:
        """Indicates if the training will be done with discrete actions"""
        return True

    @classmethod
    def _n_discrete(cls) -> int:
        """Amount of discrete actions"""
        return len(cls.values) ** 2

    def step(self, action: int) -> Tuple[np.ndarray, float, bool, Dict[Any, Any]]:
        err_msg = "%r (%s) invalid" % (action, type(action))
        assert self.action_space.contains(action), err_msg

        shape = len(self.values)
        action_pitch_index = math.floor(math.fmod(action, shape))
        action = math.floor(action / shape)
        action_roll_index = math.floor(math.fmod(action, shape))
        action_list = [self.values[action_pitch_index], self.values[action_roll_index]]
        return super(MoabMoveToCenterDiscreteWrapper, self).step(action_list)


class MoabMoveToCenterPartialObservableWrapper(MoabMoveToCenterWrapper):

    @classmethod
    def get_state_field_list(cls) -> List[str]:
        return ["ball_x", "ball_y"]


class MoabMoveToCenterAvoidObstacleWrapper(MoabMoveToCenterWrapper):

    @classmethod
    def get_state_field_list(cls) -> List[str]:
        fields = MoabMoveToCenterWrapper.get_state_field_list()
        # Obstacle data
        fields.extend(["obstacle_direction", "obstacle_distance"])
        return fields

    def _is_done(self) -> bool:
        done = super(MoabMoveToCenterAvoidObstacleWrapper, self)._is_done()
        if not done:
            # Check if hit the obstacle
            done = self.model.obstacle_distance <= MoabMoveToCenterAvoidObstacleWrapper.Cushion
        return done

    def _get_shaped_reward(self) -> float:
        reward = super(MoabMoveToCenterAvoidObstacleWrapper, self)._get_shaped_reward()
        obstacle_reshape = (MoabMoveToCenterAvoidObstacleWrapper.Cushion /
                            self.model.obstacle_distance) * 10
        reward -= obstacle_reshape
        return reward


DEFAULT_ENV_CONFIG = {
    "randomize_ball": False,
    "randomize_obstacle": False,
    "seed": None, # or int
}


def _make_moab_env(config: Dict[str, Any], cls: Type[_MoabBaseWrapper]) -> _MoabBaseWrapper:
    env_config = DEFAULT_ENV_CONFIG.copy()
    env_config.update(config)
    randomize_ball = env_config["randomize_ball"]
    randomize_obstacle = env_config["randomize_obstacle"]
    seed = env_config["seed"]
    env = cls(seed)
    env.set_randomize_ball(randomize_ball)
    env.set_randomize_obstacle(randomize_obstacle)
    return env


MOAB_MOVE_TO_CENTER_ENV_NAME = "MoabMoveToCenterSim-v0"
def make_moab_move_to_center_env(config: Dict[str, Any]) -> _MoabBaseWrapper:
    return _make_moab_env(config, MoabMoveToCenterWrapper)


MOAB_MOVE_TO_CENTER_DISCRTE_ENV_NAME = "MoabMoveToCenterDiscreteSim-v0"
def make_moab_move_to_center_discrete_env(config: Dict[str, Any]) -> _MoabBaseWrapper:
    return _make_moab_env(config, MoabMoveToCenterDiscreteWrapper)


MOAB_MOVE_TO_CENTER_PARTIAL_OBSERVABLE_ENV_NAME = "MoabMoveToCenterPartialObservableSim-v0"
def make_moab_move_to_center_partial_observable_env(config: Dict[str, Any]) -> _MoabBaseWrapper:
    return _make_moab_env(config, MoabMoveToCenterPartialObservableWrapper)


MOAB_MOVE_TO_CENTER_AVOID_OBSTACLE_ENV_NAME = "MoabMoveToCenterAvoidObstacleSim-v0"
def make_moab_move_to_center_avoid_obstacle_env(config: Dict[str, Any]) -> _MoabBaseWrapper:
    return _make_moab_env(config, MoabMoveToCenterAvoidObstacleWrapper)


def ensure_moab_envs_register():
    if not _global_registry.contains(ENV_CREATOR, MOAB_MOVE_TO_CENTER_ENV_NAME):
        register_env(name=MOAB_MOVE_TO_CENTER_ENV_NAME,
                     env_creator=make_moab_move_to_center_env)
    if not _global_registry.contains(ENV_CREATOR, MOAB_MOVE_TO_CENTER_DISCRTE_ENV_NAME):
        register_env(name=MOAB_MOVE_TO_CENTER_DISCRTE_ENV_NAME,
                     env_creator=make_moab_move_to_center_discrete_env)
    if not _global_registry.contains(ENV_CREATOR,
                                     MOAB_MOVE_TO_CENTER_PARTIAL_OBSERVABLE_ENV_NAME):
        register_env(name=MOAB_MOVE_TO_CENTER_PARTIAL_OBSERVABLE_ENV_NAME,
                     env_creator=make_moab_move_to_center_partial_observable_env)
    if not _global_registry.contains(ENV_CREATOR,
                                     MOAB_MOVE_TO_CENTER_AVOID_OBSTACLE_ENV_NAME):
        register_env(name=MOAB_MOVE_TO_CENTER_AVOID_OBSTACLE_ENV_NAME,
                     env_creator=make_moab_move_to_center_avoid_obstacle_env)
