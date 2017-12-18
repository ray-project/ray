#!/usr/bin/env python3

from __future__ import print_function

import os
import random
import signal
import subprocess
import time

import numpy as np

from carla.client import CarlaClient
from carla.sensor import Camera
from carla.settings import CarlaSettings

import gym
from gym.spaces import Box


RETRIES_ON_ERROR = 5
X_RES = 80
Y_RES = 80

IMAGE_OUT_PATH = os.environ.get("CARLA_OUT")
MAX_STEPS = os.environ.get("CARLA_MAX_STEPS", 1000)
SERVER_BINARY = os.environ.get(
    "CARLA_SERVER", "/home/ubuntu/carla-0.7/CarlaUE4.sh")

# Defaults to driving down the road /Game/Maps/Town02, start pos 0
TARGET_X = os.environ.get("CARLA_TARGET_X", -7.5)
TARGET_Y = os.environ.get("CARLA_TARGET_Y", 300)


class CarlaEnv(gym.Env):

    def __init__(self):
        # TODO: use a Tuple or Dict space
        self.action_space = Box(-1.0, 1.0, shape=(3,))
        self.observation_space = Box(0.0, 1.0, shape=(X_RES, Y_RES, 1))
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
        for _ in range(RETRIES_ON_ERROR):
            try:
                if not self.server_process:
                    self.init_server()
                return self._reset()
            except Exception as e:
                print("Error during reset: {}".format(e))
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
            NumberOfVehicles=20,
            NumberOfPedestrians=40,
            WeatherId=random.choice([1, 3, 7, 8, 14]))
        settings.randomize_seeds()

        # Now we want to add a couple of cameras to the player vehicle.
        # We will collect the images produced by these cameras every
        # frame.

        # The default camera captures RGB images of the scene.
        # camera0 = Camera('CameraRGB')
        # Set image resolution in pixels.
        # camera0.set_image_size(X_RES, Y_RES)
        # Set its position relative to the car in centimeters.
        # camera0.set_position(30, 0, 130)
        # settings.add_sensor(camera0)

        # Let's add another camera producing ground-truth depth.
        camera1 = Camera('CameraDepth', PostProcessing='Depth')
        camera1.set_image_size(X_RES, Y_RES)
        camera1.set_position(30, 0, 130)
        settings.add_sensor(camera1)

        scene = self.client.load_settings(settings)

        # Choose one player start at random.
        number_of_player_starts = len(scene.player_start_spots)
        player_start = 0  #random.randint(0, max(0, number_of_player_starts - 1))

        # Notify the server that we want to start the episode at the
        # player_start index. This function blocks until the server is ready
        # to start the episode.
        print('Starting new episode...')
        self.client.start_episode(player_start)

        image, measurements = self._read_observation()
        self.prev_measurement = measurements
        return self.preprocess_image(image)

    def step(self, action):
        try:
            return self._step(action)
        except Exception as e:
            print("Error during step, terminating episode early", e)
            self.clear_server_state()
            return np.zeros(self.action_space.shape), 0.0, True, {}

    def _step(self, action):
        assert len(action) == 3, "Invalid action {}".format(action)
        self.client.send_control(
            steer=action[0],
            throttle=min(1.0, abs(action[1])),
            brake=min(1.0, abs(action[2])),
            hand_brake=False,
            reverse=action[1] < 0.0)
        image, measurements = self._read_observation()
        reward, done = compute_reward(self.prev_measurement, measurements)
        self.prev_measurement = measurements
        if self.num_steps > MAX_STEPS:
            done = True
        self.num_steps += 1
        info = {}
        image = self.preprocess_image(image)
        return image, reward, done, info

    def preprocess_image(self, image):
        return image.data.reshape(X_RES, Y_RES, 1)

    def _read_observation(self):
        # Read the data produced by the server this frame.
        measurements, sensor_data = self.client.read_data()

        # Print some of the measurements.
        print_measurements(measurements)

        observation = None
        for name, image in sensor_data.items():
            if name == "CameraDepth":
                observation = image

        if IMAGE_OUT_PATH:
            for name, image in sensor_data.items():
                image.save_to_disk("{}/{}-{}.jpg".format(
                    IMAGE_OUT_PATH, name, self.num_steps))

        assert observation is not None, sensor_data
        return observation, measurements


def distance(x1, y1, x2, y2):
    return ((x1 - x2)**2 + (y1 - y2)**2)**0.5


def compute_reward(prev, current):
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
        distance(prev_x, prev_y, TARGET_X, TARGET_Y) -
        distance(cur_x, cur_y, TARGET_X, TARGET_Y))

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

    if distance(cur_x, cur_y, TARGET_X, TARGET_Y) < 10:
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
    while not done:
        i += 1
        obs, reward, done, info = env.step([0, 1, 0])
        print(i, "obs", obs.shape, "rew", reward, "done", done)
    print("{} fps".format(100 / (time.time() - start)))
