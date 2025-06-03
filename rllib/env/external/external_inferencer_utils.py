import socket
import tempfile
import time

import gymnasium as gym

from ray.rllib.env.external.external_env_protocol import RLlink as rllink
from ray.rllib.env.external.env_runner_server_for_external_inference import (
    _send_message,
    _get_message,
)


# net_agent.py

import socket
import threading
import time



# agent.py
import random

def get_action(observation):
    return random.randint(0, 1)  # Discrete(2)



class Client:
    def __init__(self):
        self.sock = None
        self.thread = None
        self.should_run = False

    def start(self, port: int):
        self.should_run = True

        def run():
            try:
                self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                self.sock.connect(("localhost", port))

                # Send ping-pong.
                _send_message(self.sock, {"type": rllink.PING.name})
                msg_type, msg_body = _get_message(self.sock)
                assert msg_type == rllink.PONG

                # Request config.
                _send_message(self.sock, {"type": rllink.GET_CONFIG.name})
                msg_type, msg_body = _get_message(self.sock)
                assert msg_type == rllink.SET_CONFIG
                env_steps_per_sample = msg_body["env_steps_per_sample"]
                force_on_policy = msg_body["force_on_policy"]

                # Request model weights.
                _send_message(self.sock, {"type": rllink.GET_STATE.name})
                msg_type, msg_body = _get_message(self.sock)
                assert msg_type == rllink.SET_STATE
                onnx_session, output_names = _set_state(msg_body)

                while self.should_run:
                    #self.sock.sendall(b"tick\n")
                    time.sleep(1)
            except Exception as e:
                print("[Python] Client error:", e)
            finally:
                if self.sock:
                    self.sock.close()

        self.thread = threading.Thread(target=run, daemon=True)
        self.thread.start()

    def shutdown(self):
        self.should_run = False
        if self.sock:
            try:
                self.sock.shutdown(socket.SHUT_RDWR)
            except:
                pass
            self.sock.close()
            self.sock = None
        time.sleep(0.2)

    def do_blocking_read(self):
        if not self.sock:
            raise RuntimeError("Socket not connected")
        return self.sock.recv(1024).decode()


# Global Client instance.
client = Client()


# Functions exposed to C++.

def start_client(port: int = 5556):
    client.start(port=port)


def shutdown_client():
    client.shutdown()


def do_blocking_task(tick):
    print(f"[Python] Blocking read during tick {tick}")
    return client.do_blocking_read()




#def connect_to_server():
#    """`pybind11`-callable function to open a connection and send initial messages."""
#
#    # Episode collection buckets.
#    episodes = []
#    observations = []
#    actions = []
#    action_dist_inputs = []
#    action_logps = []
#    rewards = []
#
#    timesteps = 0
#    episode_return = 0.0
#
#    # Start actual env loop.
#    env = gym.make("CartPole-v1")
#    obs, info = env.reset()
#    observations.append(obs.tolist())
#
#    while True:
#        timesteps += 1
#        # Perform action inference using the ONNX model.
#        logits = onnx_session.run(
#            output_names,
#            {"onnx::Gemm_0": np.array([obs], np.float32)},
#        )[0][
#            0
#        ]  # [0]=first return item, [0]=batch size 1
#
#        # Stochastic sample.
#        action_probs = softmax(logits)
#        action = int(np.random.choice(list(range(env.action_space.n)), p=action_probs))
#        logp = float(np.log(action_probs[action]))
#
#        # Perform the env step.
#        obs, reward, terminated, truncated, info = env.step(action)
#
#        # Collect step data.
#        observations.append(obs.tolist())
#        actions.append(action)
#        action_dist_inputs.append(logits.tolist())
#        action_logps.append(logp)
#        rewards.append(reward)
#        episode_return += reward
#
#        # We have to create a new episode record.
#        if timesteps == env_steps_per_sample or terminated or truncated:
#            episodes.append(
#                {
#                    Columns.OBS: observations,
#                    Columns.ACTIONS: actions,
#                    Columns.ACTION_DIST_INPUTS: action_dist_inputs,
#                    Columns.ACTION_LOGP: action_logps,
#                    Columns.REWARDS: rewards,
#                    "is_terminated": terminated,
#                    "is_truncated": truncated,
#                }
#            )
#            # We collected enough samples -> Send them to server.
#            if timesteps == env_steps_per_sample:
#                # Make sure the amount of data we collected is correct.
#                assert sum(len(e["actions"]) for e in episodes) == env_steps_per_sample
#
#                # Send the data to the server.
#                if force_on_policy:
#                    _send_message(
#                        sock_,
#                        {
#                            "type": rllink.EPISODES_AND_GET_STATE.name,
#                            "episodes": episodes,
#                            "timesteps": timesteps,
#                        },
#                    )
#                    # We are forced to sample on-policy. Have to wait for a response
#                    # with the state (weights) in it.
#                    msg_type, msg_body = _get_message(sock_)
#                    assert msg_type == rllink.SET_STATE
#                    onnx_session, output_names = _set_state(msg_body)
#
#                # Sampling doesn't have to be on-policy -> continue collecting
#                # samples.
#                else:
#                    raise NotImplementedError
#
#                episodes = []
#                timesteps = 0
#
#            # Set new buckets to empty lists (for next episode).
#            observations = [observations[-1]]
#            actions = []
#            action_dist_inputs = []
#            action_logps = []
#            rewards = []
#
#            # The episode is done -> Reset.
#            if terminated or truncated:
#                obs, _ = env.reset()
#                observations = [obs.tolist()]
#                episode_return = 0.0
#