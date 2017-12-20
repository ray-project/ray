from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from datetime import datetime
import cv2
import os
import json
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


# Set this where you want to save image outputs (or empty string to disable)
CARLA_OUT_PATH = os.environ.get("CARLA_OUT", os.path.expanduser("~/carla_out"))
if CARLA_OUT_PATH and not os.path.exists(CARLA_OUT_PATH):
    os.makedirs(CARLA_OUT_PATH)

# Set this to the path of your Carla binary
SERVER_BINARY = os.environ.get(
    "CARLA_SERVER", "/home/ubuntu/carla-0.7/CarlaUE4.sh")

# Number of retries if the server doesn't respond
RETRIES_ON_ERROR = 5

# Default environment configuration
ENV_CONFIG = {
    "verbose": True,
    "render_x_res": 400,
    "render_y_res": 300,
    "x_res": 80,
    "y_res": 80,
    "map": "/Game/Maps/Town02",
    "random_starting_location": False,
    "use_depth_camera": False,
    "discrete_actions": False,
    "max_steps": 50,
    "num_vehicles": 20,
    "num_pedestrians": 40,
    "weather": [1],  # [1, 3, 7, 8, 14]

    # Defaults to driving down the road /Game/Maps/Town02, start pos 0
    "target_x": -7.5,
    "target_y": 120,
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
                -1.0, 1.0, shape=(config["y_res"], config["x_res"], 1))
        else:
            self.observation_space = Box(
                0.0, 255.0, shape=(config["y_res"], config["x_res"], 3))
        self._spec = lambda: None
        self._spec.id = "Carla-v0"

        self.server_port = None
        self.server_process = None
        self.client = None
        self.num_steps = 0
        self.total_reward = 0
        self.prev_measurement = None
        self.episode_id = None
        self.measurements_file = None
        self.weather = None
        self.player_start = None

    def init_server(self):
        print("Initializing new Carla server...")
        # Create a new server process and start the client.
        self.server_port = random.randint(10000, 60000)
        self.server_process = subprocess.Popen(
            [SERVER_BINARY, self.config["map"],
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
                # reset twice since the first time a server is initialized,
                # the starting location is different
                self._reset()
                return self._reset()
            except Exception as e:
                print("Error during reset: {}".format(traceback.format_exc()))
                self.clear_server_state()
                error = e
        raise error

    def _reset(self):
        self.num_steps = 0
        self.prev_measurement = None
        self.episode_id = datetime.today().strftime("%Y-%m-%d_%H-%M-%S_%f")
        self.measurements_file = None

        # Create a CarlaSettings object. This object is a wrapper around
        # the CarlaSettings.ini file. Here we set the configuration we
        # want for the new episode.
        settings = CarlaSettings()
        self.weather = random.choice(self.config["weather"])
        settings.set(
            SynchronousMode=True,
            SendNonPlayerAgentsInfo=True,
            NumberOfVehicles=self.config["num_vehicles"],
            NumberOfPedestrians=self.config["num_pedestrians"],
            WeatherId=self.weather)
        settings.randomize_seeds()

        camera1 = Camera("CameraDepth", PostProcessing="Depth")
        camera1.set_image_size(
            self.config["render_x_res"], self.config["render_y_res"])
        camera1.set_position(30, 0, 130)
        settings.add_sensor(camera1)

        camera2 = Camera("CameraRGB")
        camera2.set_image_size(
            self.config["render_x_res"], self.config["render_y_res"])
        camera2.set_position(30, 0, 130)
        settings.add_sensor(camera2)

        scene = self.client.load_settings(settings)

        # Choose one player start at random.
        number_of_player_starts = len(scene.player_start_spots)
        if self.config["random_starting_location"]:
            self.player_start = random.randint(
                0, max(0, number_of_player_starts - 1))
        else:
            self.player_start = 0

        # Notify the server that we want to start the episode at the
        # player_start index. This function blocks until the server is ready
        # to start the episode.
        print("Starting new episode...")
        self.client.start_episode(self.player_start)

        image, py_measurements = self._read_observation()
        self.prev_measurement = py_measurements
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
            brake = max(0.0, min(1.0, action[2]))
            reverse = action[1] < 0.0

        hand_brake = False

        if self.config["verbose"]:
            print(
                "steer", steer, "throttle", throttle, "brake", brake,
                "reverse", reverse)

        self.client.send_control(
            steer=steer, throttle=throttle, brake=brake, hand_brake=hand_brake,
            reverse=reverse)

        # Process observations
        image, py_measurements = self._read_observation()
        reward, done = compute_reward(
            self.config, self.prev_measurement, py_measurements)
        if self.num_steps > self.config["max_steps"]:
            done = True
        self.total_reward += reward
        py_measurements["reward"] = reward
        py_measurements["total_reward"] = self.total_reward
        py_measurements["done"] = done
        py_measurements["action"] = action
        py_measurements["control"] = {
            "steer": steer,
            "throttle": throttle,
            "brake": brake,
            "reverse": reverse,
            "hand_brake": hand_brake,
        }
        self.prev_measurement = py_measurements

        # Write out measurements to file
        if CARLA_OUT_PATH:
            if not self.measurements_file:
                self.measurements_file = open(
                    os.path.join(
                        CARLA_OUT_PATH,
                        "measurements_{}.json".format(self.episode_id)),
                    "w")
            self.measurements_file.write(json.dumps(py_measurements))
            self.measurements_file.write("\n")
            if done:
                self.measurements_file.close()
                self.measurements_file = None

        self.num_steps += 1
        image = self.preprocess_image(image)
        return image, reward, done, py_measurements

    def preprocess_image(self, image):
        if self.config["use_depth_camera"]:
            data = (image.data - 0.5) * 2
            data = data.reshape(
                self.config["render_y_res"], self.config["render_x_res"], 1)
            data = cv2.resize(
                data, (self.config["x_res"], self.config["y_res"]),
                interpolation=cv2.INTER_AREA)
        else:
            data = image.data.reshape(
                self.config["render_y_res"], self.config["render_x_res"], 3)
            data = cv2.resize(
                data, (self.config["x_res"], self.config["y_res"]),
                interpolation=cv2.INTER_AREA)
            data = (data.astype(np.float32) - 128) / 128
        return data

    def _read_observation(self):
        # Read the data produced by the server this frame.
        measurements, sensor_data = self.client.read_data()

        # Print some of the measurements.
        if self.config["verbose"]:
            print_measurements(measurements)

        observation = None
        if self.config["use_depth_camera"]:
            camera_name = "CameraDepth"
        else:
            camera_name = "CameraRGB"
        for name, image in sensor_data.items():
            if name == camera_name:
                observation = image

        cur = measurements.player_measurements
        py_measurements = {
            "episode_id": self.episode_id,
            "step": self.num_steps,
            "x": cur.transform.location.x,
            "y": cur.transform.location.y,
            "forward_speed": cur.forward_speed,
            "collision_vehicles": cur.collision_vehicles,
            "collision_pedestrians": cur.collision_pedestrians,
            "collision_other": cur.collision_other,
            "intersection_offroad": cur.intersection_offroad,
            "intersection_otherlane": cur.intersection_otherlane,
            "weather": self.weather,
            "map": self.config["map"],
            "target_x": self.config["target_x"],
            "target_y": self.config["target_y"],
            "x_res": self.config["x_res"],
            "y_res": self.config["y_res"],
            "num_vehicles": self.config["num_vehicles"],
            "num_pedestrians": self.config["num_pedestrians"],
            "max_steps": self.config["max_steps"],
        }

        if CARLA_OUT_PATH:
            for name, image in sensor_data.items():
                out_dir = os.path.join(CARLA_OUT_PATH, name)
                if not os.path.exists(out_dir):
                    os.makedirs(out_dir)
                out_file = os.path.join(
                    out_dir,
                    "{}_{:>04}.jpg".format(self.episode_id, self.num_steps))
                scipy.misc.imsave(out_file, image.data)

        assert observation is not None, sensor_data
        return observation, py_measurements


def distance(x1, y1, x2, y2):
    return ((x1 - x2)**2 + (y1 - y2)**2)**0.5


def compute_reward(config, prev, current):
    prev_x = prev["x"] / 100  # cm -> m
    prev_y = prev["y"] / 100
    cur_x = current["x"] / 100  # cm -> m
    cur_y = current["y"] / 100

    reward = 0.0
    done = False

    # Distance travelled toward the goal in m
    reward += (
        distance(prev_x, prev_y, config["target_x"], config["target_y"]) -
        distance(cur_x, cur_y, config["target_x"], config["target_y"]))

    # Change in speed (km/h)
    reward += 0.05 * (current["forward_speed"] - prev["forward_speed"])

    # New collision damage
    reward -= .00002 * (
        current["collision_vehicles"] + current["collision_pedestrians"] +
        current["collision_other"] - prev["collision_vehicles"] -
        prev["collision_pedestrians"] - prev["collision_other"])

    # New sidewalk intersection
    reward -= 2 * (
        current["intersection_offroad"] - prev["intersection_offroad"])

    # New opposite lane intersection
    reward -= 2 * (
        current["intersection_otherlane"] - prev["intersection_otherlane"])

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
