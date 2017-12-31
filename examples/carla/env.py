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
import sys
import time
import traceback

import numpy as np
try:
    import scipy.misc
except Exception:
    pass

import gym
from gym.spaces import Box, Discrete


# Set this where you want to save image outputs (or empty string to disable)
CARLA_OUT_PATH = os.environ.get("CARLA_OUT", os.path.expanduser("~/carla_out"))
if CARLA_OUT_PATH and not os.path.exists(CARLA_OUT_PATH):
    os.makedirs(CARLA_OUT_PATH)

# Set this to the path of your Carla binary
SERVER_BINARY = os.environ.get(
    "CARLA_SERVER", os.path.expanduser("~/CARLA_0.7.0/CarlaUE4.sh"))

assert os.path.exists(SERVER_BINARY)
if "CARLA_PY_PATH" in os.environ:
    sys.path.append(os.path.expanduser(os.environ["CARLA_PY_PATH"]))
else:
    sys.path.append(
        os.path.join(os.path.dirname(SERVER_BINARY), "PythonClient"))

try:
    from carla.client import CarlaClient
    from carla.sensor import Camera
    from carla.settings import CarlaSettings
    from carla.planner.planner import Planner, REACH_GOAL, GO_STRAIGHT, \
        TURN_RIGHT, TURN_LEFT, LANE_FOLLOW
except Exception as e:
    print("Failed to import Carla python libs, try setting $CARLA_PY_PATH")
    raise e

# Carla planner commands
COMMANDS_ENUM = {
    REACH_GOAL: "REACH_GOAL",
    GO_STRAIGHT: "GO_STRAIGHT",
    TURN_RIGHT: "TURN_RIGHT",
    TURN_LEFT: "TURN_LEFT",
    LANE_FOLLOW: "LANE_FOLLOW",
}

# Number of retries if the server doesn't respond
RETRIES_ON_ERROR = 5

# Dummy Z coordinate to ues
DUMMY_Z = 22

# Simple scenario for Town02 that involves driving down a road
TEST_SCENARIO = {
    "num_vehicles": 20,
    "num_pedestrians": 40,
    "weather_distribution": [1],  # [1, 3, 7, 8, 14]
    "start_pos_id": 36,
    "end_pos_id": 40,
    "max_steps": 200,
}

# Default environment configuration
ENV_CONFIG = {
    "log_images": True,
    # TODO(ekl) cv2's video encoder is pretty terrible, we should log images
    # and then use FFMPEG to encode them
    "log_video": False,
    "verbose": True,
    "reward_function": "corl2017",
    "render_x_res": 400,
    "render_y_res": 300,
    "x_res": 80,
    "y_res": 80,
    "server_map": "/Game/Maps/Town02",
    "scenarios": [TEST_SCENARIO],
    "enable_depth_camera": False,
    "use_depth_camera": False,
    "discrete_actions": True,
}


class CarlaEnv(gym.Env):

    def __init__(self, config=ENV_CONFIG):
        self.config = config
        self.planner = Planner(self.config["server_map"].split("/")[-1])

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

        # TODO(ekl) this isn't really a proper gym spec
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
        self.scenario = None
        self.start_pos = None
        self.end_pos = None
        self.start_coord = None
        self.end_coord = None
        self.video_out = None

    def init_server(self):
        print("Initializing new Carla server...")
        # Create a new server process and start the client.
        self.server_port = random.randint(10000, 60000)
        self.server_process = subprocess.Popen(
            [SERVER_BINARY, self.config["server_map"],
             "-windowed", "-ResX=400", "-ResY=300",
             "-carla-server",
             "-carla-world-port={}".format(self.server_port)],
            preexec_fn=os.setsid, stdout=open(os.devnull, "w"))

        self.client = CarlaClient("localhost", self.server_port)
        for i in range(RETRIES_ON_ERROR):
            try:
                return self.client.connect()
            except Exception as e:
                print("Error connecting: {}, attempt {}".format(e, i))
                time.sleep(2)

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
        self.episode_id = datetime.today().strftime("%Y-%m-%d_%H-%M-%S_%f")
        self.measurements_file = None

        if CARLA_OUT_PATH and self.config["log_video"]:
            fourcc = cv2.VideoWriter_fourcc(*"MJPG")
            self.video_out = cv2.VideoWriter(
                os.path.join(CARLA_OUT_PATH, self.episode_id + ".avi"),
                fourcc, 15.0,
                (self.config["render_x_res"], self.config["render_y_res"]))
        else:
            self.video_out = None

        # Create a CarlaSettings object. This object is a wrapper around
        # the CarlaSettings.ini file. Here we set the configuration we
        # want for the new episode.
        settings = CarlaSettings()
        self.scenario = random.choice(self.config["scenarios"])
        self.weather = random.choice(self.scenario["weather_distribution"])
        settings.set(
            SynchronousMode=True,
            SendNonPlayerAgentsInfo=True,
            NumberOfVehicles=self.scenario["num_vehicles"],
            NumberOfPedestrians=self.scenario["num_pedestrians"],
            WeatherId=self.weather)
        settings.randomize_seeds()

        if self.config["enable_depth_camera"]:
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

        # Setup start and end positions
        scene = self.client.load_settings(settings)
        positions = scene.player_start_spots
        self.start_pos = positions[self.scenario["start_pos_id"]]
        self.end_pos = positions[self.scenario["end_pos_id"]]
        self.start_coord = [
            self.start_pos.location.x // 100, self.start_pos.location.y // 100]
        self.end_coord = [
            self.end_pos.location.x // 100, self.end_pos.location.y // 100]
        print(
            "Start pos {} ({}), end {} ({})".format(
                self.scenario["start_pos_id"], self.start_coord,
                self.scenario["end_pos_id"], self.end_coord))

        # Notify the server that we want to start the episode at the
        # player_start index. This function blocks until the server is ready
        # to start the episode.
        print("Starting new episode...")
        self.client.start_episode(self.scenario["start_pos_id"])

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
            steer = 2 * (sigmoid(action[0]) - 0.5)
            throttle = sigmoid(abs(action[1]))
            brake = 2 * (sigmoid(max(0, action[2])) - 0.5)
            reverse = bool(action[1] < 0.0)

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
        if self.config["verbose"]:
            print("Next command", py_measurements["next_command"])
        if type(action) is list:
            py_measurements["action"] = [float(a) for a in action]
        else:
            py_measurements["action"] = action
        py_measurements["control"] = {
            "steer": steer,
            "throttle": throttle,
            "brake": brake,
            "reverse": reverse,
            "hand_brake": hand_brake,
        }
        reward = compute_reward(
            self, self.prev_measurement, py_measurements)
        done = (self.num_steps > self.scenario["max_steps"] or
                py_measurements["next_command"] == "REACH_GOAL")
        self.total_reward += reward
        py_measurements["reward"] = reward
        py_measurements["total_reward"] = self.total_reward
        py_measurements["done"] = done
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
                if self.video_out:
                    self.video_out.release()

        self.num_steps += 1
        image = self.preprocess_image(image)
        return image, reward, done, py_measurements

    def preprocess_image(self, image):
        if self.config["use_depth_camera"]:
            assert self.config["enable_depth_camera"]
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
            "x_orient": cur.transform.orientation.x,
            "y_orient": cur.transform.orientation.y,
            "forward_speed": cur.forward_speed,
            "collision_vehicles": cur.collision_vehicles,
            "collision_pedestrians": cur.collision_pedestrians,
            "collision_other": cur.collision_other,
            "intersection_offroad": cur.intersection_offroad,
            "intersection_otherlane": cur.intersection_otherlane,
            "weather": self.weather,
            "map": self.config["server_map"],
            "start_coord": self.start_coord,
            "end_coord": self.end_coord,
            "current_scenario": self.scenario,
            "x_res": self.config["x_res"],
            "y_res": self.config["y_res"],
            "num_vehicles": self.scenario["num_vehicles"],
            "num_pedestrians": self.scenario["num_pedestrians"],
            "max_steps": self.scenario["max_steps"],
            "next_command": COMMANDS_ENUM[
                self.planner.get_next_command(
                    [cur.transform.location.x, cur.transform.location.y,
                     DUMMY_Z],
                    [cur.transform.orientation.x, cur.transform.orientation.y,
                     DUMMY_Z],
                    [self.end_pos.location.x, self.end_pos.location.y,
                     DUMMY_Z],
                    [self.end_pos.orientation.x, self.end_pos.orientation.y,
                     DUMMY_Z])],
        }

        if CARLA_OUT_PATH and self.config["log_images"]:
            for name, image in sensor_data.items():
                out_dir = os.path.join(CARLA_OUT_PATH, name)
                if not os.path.exists(out_dir):
                    os.makedirs(out_dir)
                out_file = os.path.join(
                    out_dir,
                    "{}_{:>04}.jpg".format(self.episode_id, self.num_steps))
                scipy.misc.imsave(out_file, image.data)

        if self.video_out:
            self.video_out.write(image.data)

        assert observation is not None, sensor_data
        return observation, py_measurements


def compute_reward_corl2017(env, prev, current):
    reward = 0.0

    if current["next_command"] == "REACH_GOAL":
        cur_dist = 0.0
    else:
        cur_dist = env.planner.get_shortest_path_distance(
            [current["x"], current["y"], 22],
            [current["x_orient"], current["y_orient"], 22],
            [env.end_pos.location.x, env.end_pos.location.y, 22],
            [env.end_pos.orientation.x, env.end_pos.orientation.y, 22]) / 100

    prev_dist = env.planner.get_shortest_path_distance(
        [prev["x"], prev["y"], 22],
        [prev["x_orient"], prev["y_orient"], 22],
        [env.end_pos.location.x, env.end_pos.location.y, 22],
        [env.end_pos.orientation.x, env.end_pos.orientation.y, 22]) / 100

    if env.config["verbose"]:
        print("Cur dist {}, prev dist {}".format(cur_dist, prev_dist))

    # Distance travelled toward the goal in m
    reward += prev_dist - cur_dist

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

    return reward


# Reward function from intel coach, for comparison
# TODO(ekl) shouldn't this include the distance to destination?
def compute_reward_coach(env, prev, current):
    speed_reward = current["forward_speed"] - 1
    if speed_reward > 30.0:
        speed_reward = 30.0

    return (speed_reward -
            5 * current["intersection_offroad"] -
            5 * current["intersection_otherlane"] -
            100 * bool(
                current["collision_vehicles"] +
                current["collision_pedestrians"] +
                current["collision_other"]) -
            10 * abs(current["control"]["steer"]))


REWARD_FUNCTIONS = {
    "corl2017": compute_reward_corl2017,
    "coach": compute_reward_coach,
}


def compute_reward(env, prev, current):
    return REWARD_FUNCTIONS[env.config["reward_function"]](env, prev, current)


def print_measurements(measurements):
    number_of_agents = len(measurements.non_player_agents)
    player_measurements = measurements.player_measurements
    message = "Vehicle at ({pos_x:.1f}, {pos_y:.1f}), "
    message += "{speed:.2f} km/h, "
    message += "Collision: {{vehicles={col_cars:.0f}, "
    message += "pedestrians={col_ped:.0f}, other={col_other:.0f}}}, "
    message += "{other_lane:.0f}% other lane, {offroad:.0f}% off-road, "
    message += "({agents_num:d} non-player agents in the scene)"
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


def sigmoid(x):
    x = float(x)
    return np.exp(x) / (1 + np.exp(x))


if __name__ == "__main__":
    for _ in range(2):
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
