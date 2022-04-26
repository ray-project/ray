from gym.spaces import Box, Discrete
import logging
import math
import numpy as np
import time
from typing import Optional, Tuple

from ray.rllib.env.multi_agent_env import MultiAgentEnv
from ray.rllib.utils.annotations import override
from ray.rllib.utils.typing import MultiAgentDict

logger = logging.getLogger(__name__)


class UnrealEngine4AirSimCarEnv(MultiAgentEnv):

    # The vehicle client defaults to port 41451.
    AIRSIM_PORT = 41451

    def __init__(self,
                 ip: Optional[str] = None,
                 port: Optional[int] = None,
                 image_shape: Tuple[int] = (84, 84, 3),
                 #action_frequency: float = 1.0,
                 episode_horizon: int = 1000):
        """Initializes an Unreal4Env object.
        """

        import airsim

        super().__init__()

        self.ip = ip or "localhost"
        self.port = port or self.AIRSIM_PORT
        self.image_shape = image_shape
        self.observation_space = Box(0, 255, shape=image_shape, dtype=np.uint8)
        self.action_space = Discrete(6)

        self.car = airsim.CarClient(ip=self.ip + ":" + str(self.port))

        self.image_request = airsim.ImageRequest(
            "0", airsim.ImageType.DepthPerspective, pixels_as_float=True,
            compress=False
        )

        self.car_controls = airsim.CarControls()
        self.car_state = None

        self.state = {
            "position": np.zeros(3),
            "prev_position": np.zeros(3),
            "collision": None,
            "pose": None,
            "prev_pose": None,
        }

        # Frequency in Hz with which we will send actions to the simulation.
        #self.action_frequency = action_frequency

        # Reset entire env every this number of step calls.
        self.episode_horizon = episode_horizon
        # Keep track of how many times we have called `step` so far.
        self.episode_timesteps = 0

    @override(MultiAgentEnv)
    def step(
            self, action: int
    ) -> Tuple[MultiAgentDict, MultiAgentDict, MultiAgentDict, MultiAgentDict]:
        """Sets the given actions on the car."""

        # Set action.
        self._set_actions(action)
        # Get next observation, rewards, and dones..
        obs = self._get_obs()
        rewards, dones = self._get_rewards_and_dones()
        # Create infos multi-agent dict.
        infos = {"car0": self.state}

        # Global horizon reached? -> Return __all__ done=True, so user
        # can reset. Set all agents' individual `done` to True as well.
        self.episode_timesteps += 1
        if self.episode_timesteps > self.episode_horizon:
            return obs, rewards, dict({
                "__all__": True
            }, **{agent_id: True
                  for agent_id in dones}), infos

        dones["__all__"] = dones
        return {"car0": obs}, {"car0": rewards}, {"car0": dones}, infos

    @override(MultiAgentEnv)
    def reset(self) -> MultiAgentDict:
        """Calls reset on the airSimCarEnv."""
        self.episode_timesteps = 0
        self.car.reset()
        self.car.enableApiControl(True)
        self.car.armDisarm(True)
        time.sleep(0.01)
        self._set_actions(1)
        return {"car0": self._get_obs()}

    def _get_obs(self):
        from PIL import Image
        # Get the image from the car client.
        responses = self.car.simGetImages([self.image_request])
        # Convert to float32 ndarray.
        img1d = np.array(responses[0].image_data_float, dtype=np.float)
        #
        img1d = 255 / np.maximum(np.ones(img1d.size), img1d)
        img2d = np.reshape(img1d, (responses[0].height, responses[0].width))

        image = Image.fromarray(img2d)
        im_final = np.array(image.resize((84, 84)).convert("L"))
        image = im_final.reshape([84, 84, 1])

        # Update our (internal) state.
        self.car_state = self.car.getCarState()
        collision = self.car.simGetCollisionInfo().has_collided
        self.state["prev_pose"] = self.state["pose"]
        self.state["pose"] = self.car_state.kinematics_estimated
        self.state["collision"] = collision

        return image

    def _set_actions(self, action):
        """Sends a valid gym action to the AirSim car."""

        # By default, do not break and hit the gas pedal (throttle=1).
        self.car_controls.brake = 0
        self.car_controls.throttle = 1

        # Brake.
        if action == 0:
            self.car_controls.throttle = 0
            self.car_controls.brake = 1
        # No-op (no steering; only hit gas).
        elif action == 1:
            self.car_controls.steering = 0
        # Steer hard right.
        elif action == 2:
            self.car_controls.steering = 0.5
        # Steer hard left.
        elif action == 3:
            self.car_controls.steering = -0.5
        # Steer a little right.
        elif action == 4:
            self.car_controls.steering = 0.25
        # Steer a little left.
        else:
            self.car_controls.steering = -0.25

        # Send the controls back.
        self.car.setCarControls(self.car_controls)
        #time.sleep(self.action_frequency)

    def _get_rewards_and_dones(self):
        MAX_SPEED = 300
        MIN_SPEED = 10
        thresh_dist = 3.5
        beta = 3

        z = 0
        pts = [
            np.array([0, -1, z]),
            np.array([130, -1, z]),
            np.array([130, 125, z]),
            np.array([0, 125, z]),
            np.array([0, -1, z]),
            np.array([130, -1, z]),
            np.array([130, -128, z]),
            np.array([0, -128, z]),
            np.array([0, -1, z]),
        ]
        pd = self.state["pose"].position
        car_pt = np.array([pd.x_val, pd.y_val, pd.z_val])

        dist = 10000000
        for i in range(0, len(pts) - 1):
            dist = min(
                dist,
                np.linalg.norm(np.cross((car_pt - pts[i]), (car_pt - pts[i + 1])))
                / np.linalg.norm(pts[i] - pts[i + 1]),
            )

        # print(dist)
        if dist > thresh_dist:
            reward = -3
        else:
            reward_dist = math.exp(-beta * dist) - 0.5
            reward_speed = (
                (self.car_state.speed - MIN_SPEED) / (MAX_SPEED - MIN_SPEED)
            ) - 0.5
            reward = reward_dist + reward_speed

        # Figure out whether we are done.
        done = 0
        if reward < -1:
            done = 1
        if self.car_controls.brake == 0:
            if self.car_state.speed <= 1:
                done = 1
        if self.state["collision"]:
            done = 1

        return {"car0": reward}, {"car0": done}
