#!/usr/bin/env python3

from __future__ import print_function

import os
import random
import signal
import subprocess
import time
import traceback

import numpy as np
try:
    import scipy.misc
except Exception:
    pass

from carla.client import CarlaClient
from carla.sensor import Camera
from carla.settings import CarlaSettings

import gym
from gym.spaces import Box, Discrete


RETRIES_ON_ERROR = 5
IMAGE_OUT_PATH = os.environ.get("CARLA_OUT")
SERVER_BINARY = os.environ.get(
    "CARLA_SERVER", "/home/ubuntu/carla-0.7/CarlaUE4.sh")


ENV_CONFIG = {
    "x_res": 80,
    "y_res": 80,
    "random_starting_location": False,
    "use_depth_camera": True,
    "discrete_actions": False,
    "max_steps": 150,
    "num_vehicles": 20,
    "num_pedestrians": 40,

    # Defaults to driving down the road /Game/Maps/Town02, start pos 0
    "target_x": -7.5,
    "target_y": 300,
}


class CarlaEnv(gym.Env):

    def __init__(self, config=ENV_CONFIG):
        self.config = config

        if config["discrete_actions"]:
            self.action_space = Discrete(10)
        else:
            self.action_space = Box(-1.0, 1.0, shape=(3,))
        if config["use_depth_camera"]:
            self.observation_space = Box(
                -1.0, 1.0, shape=(config["x_res"], config["y_res"], 1))
        else:
            self.observation_space = Box(
                0.0, 255.0, shape=(config["x_res"], config["y_res"], 3))
        self._spec = lambda: None
        self._spec.id = "Carla-v0"

        self.server_port = None
        self.server_process = None
        self.client = None
        self.num_steps = 0
        self.prev_measurement = None

    def init_server(self):
        print("Initializing new Carla server...")
        # Create a new server process and start the client.
        self.server_port = random.randint(10000, 60000)
        self.server_process = subprocess.Popen(
            [SERVER_BINARY, "/Game/Maps/Town02",
             "-windowed", "-ResX=400", "-ResY=300",
             "-carla-server",
             "-carla-world-port={}".format(self.server_port)],
            preexec_fn=os.setsid, stdout=open(os.devnull, "w"))

        self.client = CarlaClient("localhost", self.server_port)
        self.client.connect()

    def clear_server_state(self):
        print("Clearing Carla server state")
        try:
            if self.client:
                self.client.disconnect()
                self.client = None
        except Exception as e:
            print("Error disconnecting client: {}".format(e))
            pass
        if self.server_process:
            os.killpg(os.getpgid(self.server_process.pid), signal.SIGKILL)
            self.server_port = None
            self.server_process = None

    def __del__(self):
        self.clear_server_state()

    def reset(self):
        error = None
        for _ in range(RETRIES_ON_ERROR):
            try:
                if not self.server_process:
                    self.init_server()
                return self._reset()
            except Exception as e:
                print("Error during reset: {}".format(traceback.format_exc()))
                self.clear_server_state()
                error = e
        raise error

    def _reset(self):
        self.num_steps = 0
        self.prev_measurement = None

        # Create a CarlaSettings object. This object is a wrapper around
        # the CarlaSettings.ini file. Here we set the configuration we
        # want for the new episode.
        settings = CarlaSettings()
        settings.set(
            SynchronousMode=True,
            SendNonPlayerAgentsInfo=True,
            NumberOfVehicles=self.config["num_vehicles"],
            NumberOfPedestrians=self.config["num_pedestrians"],
            WeatherId=random.choice([1, 3, 7, 8, 14]))
        settings.randomize_seeds()

        if self.config["use_depth_camera"]:
            camera = Camera("CameraDepth", PostProcessing="Depth")
            camera.set_image_size(self.config["x_res"], self.config["y_res"])
            camera.set_position(30, 0, 130)
            settings.add_sensor(camera)
        else:
            camera = Camera("CameraRGB")
            camera.set_image_size(self.config["x_res"], self.config["y_res"])
            camera.set_position(30, 0, 130)
            settings.add_sensor(camera)

        scene = self.client.load_settings(settings)

        # Choose one player start at random.
        number_of_player_starts = len(scene.player_start_spots)
        if self.config["random_starting_location"]:
            player_start = random.randint(
                0, max(0, number_of_player_starts - 1))
        else:
            player_start = 0

        # Notify the server that we want to start the episode at the
        # player_start index. This function blocks until the server is ready
        # to start the episode.
        print("Starting new episode...")
        self.client.start_episode(player_start)

        image, measurements = self._read_observation()
        self.prev_measurement = measurements
        return self.preprocess_image(image)

    def step(self, action):
        try:
            obs = self._step(action)
            return obs
        except Exception:
            print(
                "Error during step, terminating episode early",
                traceback.format_exc())
            self.clear_server_state()
            return np.zeros(self.observation_space.shape), 0.0, True, {}

    def _step(self, action):
        if self.config["discrete_actions"]:
            action = int(action)
            assert action in range(10)
            if action == 9:
                brake = 1.0
                steer = 0.0
                throttle = 0.0
                reverse = False
            else:
                brake = 0.0
                if action >= 6:
                    steer = -1.0
                elif action >= 3:
                    steer = 1.0
                else:
                    steer = 0.0
                action %= 3
                if action == 0:
                    throttle = 0.0
                    reverse = False
                elif action == 1:
                    throttle = 1.0
                    reverse = False
                elif action == 2:
                    throttle = 1.0
                    reverse = True
        else:
            assert len(action) == 3, "Invalid action {}".format(action)
            steer = action[0]
            throttle = min(1.0, abs(action[1]))
            brake = min(1.0, abs(action[2]))
            reverse = action[1] < 0.0

        print(
            "steer", steer, "throttle", throttle, "brake", brake,
            "reverse", reverse)

        self.client.send_control(
            steer=steer, throttle=throttle, brake=brake, hand_brake=False,
            reverse=reverse)
        image, measurements = self._read_observation()
        reward, done = compute_reward(
            self.config, self.prev_measurement, measurements)
        self.prev_measurement = measurements
        if self.num_steps > self.config["max_steps"]:
            done = True
        self.num_steps += 1
        info = {}
        image = self.preprocess_image(image)
        return image, reward, done, info

    def preprocess_image(self, image):
        if self.config["use_depth_camera"]:
            data = (image.data - 0.5) * 2
            return data.reshape(self.config["x_res"], self.config["y_res"], 1)
        else:
            return image.data.reshape(
                self.config["x_res"], self.config["y_res"], 3)

    def _read_observation(self):
        # Read the data produced by the server this frame.
        measurements, sensor_data = self.client.read_data()

        # Print some of the measurements.
        print_measurements(measurements)

        observation = None
        if self.config["use_depth_camera"]:
            camera_name = "CameraDepth"
        else:
            camera_name = "CameraRGB"
        for name, image in sensor_data.items():
            if name == camera_name:
                observation = image

        if IMAGE_OUT_PATH:
            for name, image in sensor_data.items():
                scipy.misc.imsave("{}/{}-{}.jpg".format(
                    IMAGE_OUT_PATH, name, self.num_steps), image.data)

        assert observation is not None, sensor_data
        return observation, measurements


def distance(x1, y1, x2, y2):
    return ((x1 - x2)**2 + (y1 - y2)**2)**0.5


def compute_reward(config, prev, current):
    prev = prev.player_measurements
    current = current.player_measurements

    prev_x = prev.transform.location.x / 100  # cm -> m
    prev_y = prev.transform.location.y / 100
    cur_x = current.transform.location.x / 100  # cm -> m
    cur_y = current.transform.location.y / 100

    reward = 0.0
    done = False

    # Distance travelled toward the goal in m
    reward += (
        distance(prev_x, prev_y, config["target_x"], config["target_y"]) -
        distance(cur_x, cur_y, config["target_x"], config["target_y"]))

    # Change in speed (km/h)
    reward += 0.05 * (current.forward_speed - prev.forward_speed)

    # New collision damage
    reward -= .00002 * (
        current.collision_vehicles + current.collision_pedestrians +
        current.collision_other - prev.collision_vehicles -
        prev.collision_pedestrians - prev.collision_other)

    # New sidewalk intersection
    reward -= 2 * (current.intersection_offroad - prev.intersection_offroad)

    # New opposite lane intersection
    reward -= 2 * (
        current.intersection_otherlane - prev.intersection_otherlane)

    if distance(cur_x, cur_y, config["target_x"], config["target_y"]) < 10:
        done = True

    return reward, done


def print_measurements(measurements):
    number_of_agents = len(measurements.non_player_agents)
    player_measurements = measurements.player_measurements
    message = 'Vehicle at ({pos_x:.1f}, {pos_y:.1f}), '
    message += '{speed:.2f} km/h, '
    message += 'Collision: {{vehicles={col_cars:.0f}, '
    message += 'pedestrians={col_ped:.0f}, other={col_other:.0f}}}, '
    message += '{other_lane:.0f}% other lane, {offroad:.0f}% off-road, '
    message += '({agents_num:d} non-player agents in the scene)'
    message = message.format(
        pos_x=player_measurements.transform.location.x / 100,  # cm -> m
        pos_y=player_measurements.transform.location.y / 100,
        speed=player_measurements.forward_speed,
        col_cars=player_measurements.collision_vehicles,
        col_ped=player_measurements.collision_pedestrians,
        col_other=player_measurements.collision_other,
        other_lane=100 * player_measurements.intersection_otherlane,
        offroad=100 * player_measurements.intersection_offroad,
        agents_num=number_of_agents)
    print(message)


if __name__ == '__main__':
    env = CarlaEnv()
    obs = env.reset()
    print("reset", obs)
    start = time.time()
    done = False
    i = 0
    total_reward = 0.0
    while not done:
        i += 1
        if ENV_CONFIG["discrete_actions"]:
            obs, reward, done, info = env.step(1)
        else:
            obs, reward, done, info = env.step([0, 1, 0])
        total_reward += reward
        print(
            i, "obs", obs.shape, "rew", reward, "total", total_reward,
            "done", done)
    print("{} fps".format(100 / (time.time() - start)))
