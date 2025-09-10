import pickle
import socket
import time

import gymnasium as gym
import numpy as np

from ray.rllib.core import (
    Columns,
    COMPONENT_RL_MODULE,
)
from ray.rllib.env.external.rllink import (
    get_rllink_message,
    send_rllink_message,
    RLlink,
)
from ray.rllib.env.single_agent_episode import SingleAgentEpisode
from ray.rllib.utils.framework import try_import_torch
from ray.rllib.utils.numpy import softmax

torch, _ = try_import_torch()


def _dummy_external_client(port: int = 5556):
    """A dummy client that runs CartPole and acts as a testing external env."""

    def _set_state(msg_body, rl_module):
        rl_module.set_state(msg_body[COMPONENT_RL_MODULE])
        # return msg_body[WEIGHTS_SEQ_NO]

    # Connect to server.
    while True:
        try:
            print(f"Trying to connect to localhost:{port} ...")
            sock_ = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock_.connect(("localhost", port))
            break
        except ConnectionRefusedError:
            time.sleep(5)

    # Send ping-pong.
    send_rllink_message(sock_, {"type": RLlink.PING.name})
    msg_type, msg_body = get_rllink_message(sock_)
    assert msg_type == RLlink.PONG

    # Request config.
    send_rllink_message(sock_, {"type": RLlink.GET_CONFIG.name})
    msg_type, msg_body = get_rllink_message(sock_)
    assert msg_type == RLlink.SET_CONFIG

    config = pickle.loads(msg_body["config"])
    # Create the RLModule.
    rl_module = config.get_rl_module_spec().build()

    # Request state/weights.
    send_rllink_message(sock_, {"type": RLlink.GET_STATE.name})
    msg_type, msg_body = get_rllink_message(sock_)
    assert msg_type == RLlink.SET_STATE
    _set_state(msg_body["state"], rl_module)

    env_steps_per_sample = config.get_rollout_fragment_length()

    # Start actual env loop.
    env = gym.make("CartPole-v1")
    obs, _ = env.reset()
    episode = SingleAgentEpisode(observations=[obs])
    episodes = [episode]

    while True:
        # Perform action inference using the RLModule.
        logits = rl_module.forward_exploration(
            batch={
                Columns.OBS: torch.tensor(np.array([obs], np.float32)),
            }
        )[Columns.ACTION_DIST_INPUTS][
            0
        ].numpy()  # [0]=batch size 1

        # Stochastic sample.
        action_probs = softmax(logits)
        action = int(np.random.choice(list(range(env.action_space.n)), p=action_probs))
        logp = float(np.log(action_probs[action]))

        # Perform the env step.
        obs, reward, terminated, truncated, _ = env.step(action)

        # Collect step data.
        episode.add_env_step(
            action=action,
            reward=reward,
            observation=obs,
            terminated=terminated,
            truncated=truncated,
            extra_model_outputs={
                Columns.ACTION_DIST_INPUTS: logits,
                Columns.ACTION_LOGP: logp,
            },
        )

        # We collected enough samples -> Send them to server.
        if sum(map(len, episodes)) == env_steps_per_sample:
            # Send the data to the server.
            send_rllink_message(
                sock_,
                {
                    "type": RLlink.EPISODES_AND_GET_STATE.name,
                    "episodes": [e.get_state() for e in episodes],
                    "timesteps": env_steps_per_sample,
                },
            )
            # We are forced to sample on-policy. Have to wait for a response
            # with the state (weights) in it.
            msg_type, msg_body = get_rllink_message(sock_)
            assert msg_type == RLlink.SET_STATE
            _set_state(msg_body["state"], rl_module)

            episodes = []
            if not episode.is_done:
                episode = episode.cut()
                episodes.append(episode)

        # If episode is done, reset env and create a new episode.
        if episode.is_done:
            obs, _ = env.reset()
            episode = SingleAgentEpisode(observations=[obs])
            episodes.append(episode)
