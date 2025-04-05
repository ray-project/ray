# This environment was created in the repo https://github.com/huawei-noah/HEBO
from os import path
from typing import Dict, List, Tuple, Union

import numpy as np
import torch
from gymnasium import Env, spaces
from gymnasium.utils import seeding

Array = Union[torch.Tensor, np.ndarray]


class SafeEnv(Env):
    """Safe environment wrapper."""

    def step(self, action: np.ndarray) -> np.ndarray:
        state = self._get_state()
        next_state, reward, done, terminated, info = super().step(action)
        info["cost"] = self._safety_cost_fn(state, action, next_state)
        return next_state, reward, done, terminated, info

    def _get_state(self):
        """Returns current state. Uses _get_obs() method if it is implemented."""
        if hasattr(self, "_get_obs"):
            return self._get_obs()
        else:
            raise NotImplementedError(
                "Please implement _get_obs method returning the current state"
            )

    def reset(self, seed: int = None, options: Dict = None):
        state, info = super().reset(seed=seed, options=options)
        info["cost"] = 0.0
        return state, info


def angle_normalize(theta: Array, is_tensor: bool = True) -> Array:
    """Normalizes an angle theta to be between -pi and pi."""
    if is_tensor:
        torch_pi = torch.Tensor(np.asarray(np.pi))
        return ((theta + torch_pi) % (2 * torch_pi)) - torch_pi
    else:
        return ((theta + np.pi) % (2 * np.pi)) - np.pi


class PendulumSwingUpParams:
    """Params for the system dynamics"""

    g_acc = 10.0
    mass = 1.0
    length = 1.0
    dt = 0.05
    max_speed = 8.0
    max_torque = 2.0

    theta_penalty = 1.0
    theta_dot_penalty = 0.1
    action_penalty = 0.001  # .001
    reward_offset = np.ceil(
        theta_penalty * np.pi**2
        + theta_dot_penalty * max_speed**2
        + action_penalty * max_torque**2
    )
    # reward_bias  = reward_offset

    unsafe_min = np.pi * (20.0 / 180)
    unsafe_max = np.pi * (30.0 / 180)
    hazard_area_size = np.pi * (1.0 / 4)
    n_constraints = 1

    def __str__(self):
        _dyn_params = {"g": self.g_acc, "m": self.mass, "l": self.length, "dt": self.dt}
        _state_lims = {"max_speed": self.max_speed, "max_torque": self.max_torque}
        _reward_params = {
            "theta_penalty": self.theta_penalty,
            "theta_dot_penalty": self.theta_dot_penalty,
            "action_penalty": self.action_penalty,
        }
        _safety_params = {
            "unsafe_min": self.unsafe_min,
            "unsafe_max": self.unsafe_max,
            "hazard_area_size": self.hazard_area_size,
            "n_constraints": self.n_constraints,
        }
        return {
            "Dynamics parameters": _dyn_params,
            "State Limits": _state_lims,
            "Reward Parameters": _reward_params,
            "Safety Parameters": _safety_params,
        }.__str__()


pendulum_cfg = {
    "action_dim": 1,  # are used
    "action_range": [-1, 1],  # are used
    "unsafe_reward": 0.0,
    "saute_discount_factor": 1.0,
    "max_ep_len": 200,
    "min_rel_budget": 1.0,
    "max_rel_budget": 1.0,
    "test_rel_budget": 1.0,
    "max_safety_budget": 1.0,
    "safety_budget": 1.0,
    "use_reward_shaping": True,
    "use_state_augmentation": True,
}


class PendulumEnv(Env):
    metadata = {"render.modes": ["human", "rgb_array"], "video.frames_per_second": 30}

    # Used for labels when plotting.
    obs_labels = [
        r"$\cos(\theta)$",
        r"$\sin(\theta)$",
        r"$\partial \theta$",
    ]

    def __init__(self, max_ep_len: int = 200, mode: str = "train"):
        super().__init__()
        self.viewer = None
        self.params = PendulumSwingUpParams()
        self.obs_high = np.array([1.0, 1.0, self.params.max_speed], dtype=np.float32)
        self.observation_space = spaces.Box(low=-self.obs_high, high=self.obs_high)
        action_high = np.float32(self.params.max_torque)
        self.action_space = spaces.Box(low=-action_high, high=action_high, shape=(1,))
        self.max_ep_len = 200

        assert (
            mode == "train" or mode == "test" or mode == "deterministic"
        ), "mode can be determinstic, test or train"
        self._mode = mode
        self.seed()

    def seed(self, seed: int = None) -> List[int]:
        self.np_random, seed = seeding.np_random(seed)
        return [seed]

    def do_simulation(self, u: np.ndarray):
        """One step simulation of dynamics on the single pendulum"""
        th, thdot = self.state  # th := theta
        dt = self.params.dt
        u = self.params.max_torque * u
        u = np.clip(u.squeeze(), -self.params.max_torque, self.params.max_torque)
        self.last_u = u  # for rendering

        newthdot = (
            thdot
            + (
                -3 * self.params.g_acc / (2 * self.params.length) * np.sin(th + np.pi)
                + 3.0 / (self.params.mass * self.params.length**2) * u
            )
            * dt
        )
        newth = th + newthdot * dt
        newthdot = np.clip(
            newthdot, -self.params.max_speed, self.params.max_speed
        )  # pylint: disable=E1111

        self.state = np.array([newth, newthdot])

    def step(self, action: np.ndarray) -> Tuple[np.ndarray, float, bool, Dict]:
        obs = self._get_obs()
        self.t += 1
        # bug fix do simulations with numpy actions not torch
        self.do_simulation(action)
        next_obs = self._get_obs()
        reward = self._reward_fn(
            obs, action, next_obs, is_tensor=False
        )  # / float(self.max_ep_len)
        done = self.t >= self.max_ep_len or self._termination_fn(
            obs, action, next_obs, is_tensor=False
        )
        info = dict()
        return next_obs, reward, done, False, info

    def reset(self, seed: int = None, options: Dict = None) -> np.ndarray:
        self.t = 0
        if self._mode == "train":
            high = np.array([np.pi, 1], dtype=np.float32)
            self.state = self.np_random.uniform(low=-high, high=high)
        elif self._mode == "test":
            high = np.array([0.2, 0.1], dtype=np.float32)
            low = np.array([-0.2, -0.1], dtype=np.float32)
            self.state = np.array(
                [np.pi, 0], dtype=np.float32
            ) + self.np_random.uniform(low=low, high=high)
        elif self._mode == "deterministic":
            self.state = np.array([np.pi, 0], dtype=np.float32)
        else:
            raise NotImplementedError
        self.last_u = None
        return self._get_obs(), {}

    def _get_obs(self) -> np.ndarray:
        theta, thetadot = self.state
        return np.array([np.cos(theta), np.sin(theta), thetadot])

    def _reward_fn(
        self, states: Array, actions: Array, next_states: Array, is_tensor: bool = True
    ) -> Array:
        """Compute rewards in batch if needed
        Mostly copied from openAI gym Pendulum-v0 and ported into torch.
        https://github.com/openai/gym/blob/master/gym/envs/classic_control/pendulum.py
        """

        actions = self.params.max_torque * actions
        cos_th, sin_th, thdot = states[..., 0], states[..., 1], states[..., 2]
        if is_tensor:
            assert (
                type(states) is torch.Tensor
                and type(next_states) is torch.Tensor
                and type(actions) is torch.Tensor
            ), "Arguments must be torch.Tensor"
            th = torch.atan2(sin_th, cos_th)
            th_norm = angle_normalize(th, is_tensor=True)
            action_squared = actions.clamp(
                -self.params.max_torque, self.params.max_torque
            )
            costs = (
                self.params.theta_penalty * th_norm**2
                + self.params.theta_dot_penalty * thdot**2
                + self.params.action_penalty * action_squared.squeeze() ** 2
            )
            reward = (-costs + self.params.reward_offset) / self.params.reward_offset
            return reward.view(-1, 1)
        else:
            assert (
                type(states) is np.ndarray
                and type(next_states) is np.ndarray
                and type(actions) is np.ndarray
            ), "Arguments must be np.ndarray"
            th = np.arctan2(sin_th, cos_th)
            th_norm = angle_normalize(th, is_tensor=False)
            action_squared = np.clip(
                actions, -self.params.max_torque, self.params.max_torque
            )
            costs = (
                self.params.theta_penalty * th_norm**2
                + self.params.theta_dot_penalty * thdot**2
                + self.params.action_penalty * action_squared.squeeze() ** 2
            )
            reward = (-costs + self.params.reward_offset) / self.params.reward_offset
            return reward

    def reward_fn(self, states: Array, actions: Array, next_states: Array) -> Array:
        """Compute rewards in batch if needed"""
        return self._reward_fn(states, actions, next_states, is_tensor=True)

    def _termination_fn(
        self, states: Array, actions: Array, next_states: Array, is_tensor: bool = True
    ) -> np.ndarray:
        """Returns done"""
        if is_tensor:
            return torch.ones(
                1,
            ).cuda()
        else:
            return False

    def render(self, mode="human"):

        if self.viewer is None:
            from gym.envs.classic_control import rendering

            self.viewer = rendering.Viewer(500, 500)
            self.viewer.set_bounds(-2.2, 2.2, -2.2, 2.2)
            rod = rendering.make_capsule(1, 0.2)
            rod.set_color(0.8, 0.3, 0.3)
            self.pole_transform = rendering.Transform()
            rod.add_attr(self.pole_transform)
            self.viewer.add_geom(rod)
            axle = rendering.make_circle(0.05)
            axle.set_color(0, 0, 0)
            self.viewer.add_geom(axle)
            fname = path.join(path.dirname(__file__), "assets/clockwise.png")
            self.img = rendering.Image(fname, 1.0, 1.0)
            self.imgtrans = rendering.Transform()
            self.img.add_attr(self.imgtrans)

        self.viewer.add_onetime(self.img)
        self.pole_transform.set_rotation(self.state[0] + np.pi / 2)
        if self.last_u:
            self.imgtrans.scale = (-self.last_u / 2, np.abs(self.last_u) / 2)

        return self.viewer.render(return_rgb_array=mode == "rgb_array")

    def close(self):
        if self.viewer:
            self.viewer.close()


class SafePendulumEnv(SafeEnv, PendulumEnv):
    """Safe Pendulum environment."""

    def _is_near_unsafe_area_batch(self, thetas):
        return ((self.params.unsafe_min - self.params.hazard_area_size) <= thetas) & (
            thetas <= (self.params.unsafe_max + self.params.hazard_area_size)
        )

    def _safety_cost_fn(
        self, states: np.ndarray, actions: np.ndarray, next_states: np.ndarray
    ) -> np.ndarray:
        """Computes a linear safety cost between the current position
        (if its near the unsafe area, aka in the hazard region)
        and the centre of the unsafe region"""
        unsafe_angle_middle = 0.5 * (
            self.params.unsafe_max + self.params.unsafe_min
        )  # 25 = (20 + 30) /2
        max_distance = (
            self.params.hazard_area_size
            + (unsafe_angle_middle - self.params.unsafe_min) * 1.0
        )  # 50 = 45 + (25 - 20)
        assert (
            type(states) is np.ndarray
            and type(next_states) is np.ndarray
            and type(actions) is np.ndarray
        ), "Arguments must be np.ndarray"
        thetas = np.arctan2(states[..., 1], states[..., 0])
        dist_to_center = np.abs(unsafe_angle_middle - thetas)  # |25 - theta|
        unsafe_mask = np.float64(
            self._is_near_unsafe_area_batch(thetas)
        )  # 20-45 = -25 <= theta <= 75 = 30+45
        costs = ((max_distance - dist_to_center) / (max_distance)) * unsafe_mask
        return costs

    def __str__(self):
        return "Safe Pendulum with angle constraints"
