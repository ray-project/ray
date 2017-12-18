#!/usr/bin/env python3

from __future__ import print_function

import os
import random
import signal
import subprocess

from carla.client import CarlaClient
from carla.sensor import Camera
from carla.settings import CarlaSettings

import gym
from gym.spaces import Box


class CarlaEnv(gym.Env):

    def __init__(self):
        # TODO: use a Tuple or Dict space
        self.action_space = Box(-1.0, 1.0, shape=(5,))
        self.observation_space = Box(0.0, 1.0, shape=(800, 600, 3))
        self._spec = lambda: None
        self._spec.id = "Carla-v0"

        # Create a new server process and start the client.
        self.server_process = None
        self.server_port = random.randint(10000, 60000)
        self.server_process = subprocess.Popen(
            [os.environ.get(
                "CARLA_SERVER", "/home/ubuntu/carla-0.7/CarlaUE4.sh"),
             "-carla-server",
             "-carla-world-port={}".format(self.server_port)],
            preexec_fn=os.setsid)

        self.client = CarlaClient("localhost", self.server_port)
        self.client.connect()
        self.num_steps = 0
        self.prev_measurement = None

    def __del__(self):
        self.client.disconnect()
        if self.server_process:
            os.killpg(os.getpgid(self.server_process.pid), signal.SIGKILL)

    def reset(self):
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
        camera0 = Camera('CameraRGB')
        # Set image resolution in pixels.
        camera0.set_image_size(800, 600)
        # Set its position relative to the car in centimeters.
        camera0.set_position(30, 0, 130)
        settings.add_sensor(camera0)

        # Let's add another camera producing ground-truth depth.
        camera1 = Camera('CameraDepth', PostProcessing='Depth')
        camera1.set_image_size(800, 600)
        camera1.set_position(30, 0, 130)
        settings.add_sensor(camera1)

        scene = self.client.load_settings(settings)

        # Choose one player start at random.
        number_of_player_starts = len(scene.player_start_spots)
        player_start = random.randint(0, max(0, number_of_player_starts - 1))

        # Notify the server that we want to start the episode at the
        # player_start index. This function blocks until the server is ready
        # to start the episode.
        print('Starting new episode...')
        self.client.start_episode(player_start)

        image, measurements = self._read_observation()
        self.prev_measurement = measurements
        return image

    def step(self, action):
        assert len(action) == 5, "Invalid action {}".format(action)
        self.client.send_control(
            steer=action[0],
            throttle=action[1],
            brake=bool(action[2]),
            hand_brake=bool(action[3]),
            reverse=bool(action[4]))
        image, measurements = self._read_observation()
        reward, done = compute_reward(self.prev_measurement, measurements)
        self.prev_measurement = measurements
        if self.num_steps > os.environ.get("CARLA_MAX_STEPS", 1000):
            done = True
        info = {}
        return image, reward, done, info

    def _read_observation(self):
        # Read the data produced by the server this frame.
        measurements, sensor_data = self.client.read_data()

        # Print some of the measurements.
        print_measurements(measurements)

        observation = None
        for name, image in sensor_data.items():
            if name == "CameraDepth":
                observation = image

        assert observation is not None, sensor_data
        return observation, measurements


def distance(x1, y1, x2, y2):
    return ((x1 - x2)**2 + (y1 - y2)**2)**0.5


def compute_reward(prev, current):
    prev = prev.player_measurements
    current = current.player_measurements

    target_x = os.environ.get("CARLA_TARGET_X", 0.0)
    target_y = os.environ.get("CARLA_TARGET_Y", 0.0)
    prev_x = prev.transform.location.x / 100  # cm -> m
    prev_y = prev.transform.location.y / 100
    cur_x = current.transform.location.x / 100  # cm -> m
    cur_y = current.transform.location.y / 100

    reward = 0.0
    done = False

    # Distance travelled toward the goal in m
    reward += (
        distance(prev_x, prev_y, target_x, target_y) -
        distance(cur_x, cur_y, target_x, target_y))

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

    if distance(cur_x, cur_y, target_x, target_y) < 1.0:
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
    for _ in range(100):
        print("step", env.step([0, 0, 0, 0, 0]))
