import pickle
import socket
import time
from collections import defaultdict

import gymnasium as gym
import numpy as np
from gymnasium.spaces import Dict
from ray.rllib.algorithms import AlgorithmConfig

from ray.rllib.core import (
    Columns,
    COMPONENT_RL_MODULE,
)
from ray.rllib.env.external.rllink import (
    get_rllink_message,
    send_rllink_message,
    RLlink,
)
from ray.rllib.env.multi_agent_episode import MultiAgentEpisode
from ray.rllib.env.single_agent_episode import SingleAgentEpisode
from ray.rllib.examples.envs.classes.multi_agent import MultiAgentCartPole
from ray.rllib.utils.framework import try_import_torch
from ray.rllib.utils.metrics import WEIGHTS_SEQ_NO
from ray.rllib.utils.numpy import softmax

torch, _ = try_import_torch()


def policy_mapping_fn(agent_id, *_, **__):
    return f"p{agent_id}"


def _dummy_multi_agent_external_client(port: int = 5556):
    """
    A dummy client that runs MultiAgentCartPole and acts as a testing external env.
    """

    def _set_state(msg_body, rl_module):
        rl_module.set_state(msg_body[COMPONENT_RL_MODULE])
        return msg_body[WEIGHTS_SEQ_NO]

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

    config = AlgorithmConfig.from_state(pickle.loads(msg_body["config"]))
    # Create the env.
    env = MultiAgentCartPole(config={"num_agents": len(config.policies)})
    env_to_module = config.build_env_to_module_connector(env=None, spaces=None)
    module_to_env = config.build_module_to_env_connector(env=None, spaces=None)
    # Create the RLModule.
    rl_module = config.get_multi_rl_module_spec(env=env, inference_only=True).build()

    # Request state/weights.
    send_rllink_message(sock_, {"type": RLlink.GET_STATE.name})
    msg_type, msg_body = get_rllink_message(sock_)
    assert msg_type == RLlink.SET_STATE
    weights_seq_no = _set_state(msg_body["state"], rl_module)

    env_steps_per_sample = config.get_rollout_fragment_length()

    # Start actual env loop.
    obs, _ = env.reset()
    episode = MultiAgentEpisode(
        observations=[obs],
        observation_space=Dict(env.observation_spaces),
        action_space=Dict(env.action_spaces),
        agent_module_ids={aid: policy_mapping_fn(aid) for aid in env.possible_agents},
    )
    episodes = [episode]
    shared_data = {}

    while True:
        rl_module_in = env_to_module(
            episodes=[episode],
            batch={},
            rl_module=rl_module,
            explore=True,
            shared_data=shared_data,
        )
        rl_module_out = rl_module.forward_exploration(rl_module_in)
        to_env = module_to_env(
            batch=rl_module_out,
            episodes=[episode],
            rl_module=rl_module,
            explore=True,
            shared_data=shared_data,
        )

        # Perform the env step.
        actions = to_env[Columns.ACTIONS][0]
        obs, reward, terminated, truncated, _ = env.step(actions)

        extra_model_outputs = defaultdict(dict)
        # `to_env` returns a dictionary with column keys and
        # (AgentID, value) tuple values.
        for col, ma_dict_list in to_env.items():
            ma_dict = ma_dict_list[0]
            for agent_id, val in ma_dict.items():
                extra_model_outputs[agent_id][col] = val
                extra_model_outputs[agent_id][
                    WEIGHTS_SEQ_NO
                ] = weights_seq_no
        extra_model_outputs = dict(extra_model_outputs)

        # Collect step data.
        episode.add_env_step(
            actions=actions,
            rewards=reward,
            observations=obs,
            terminateds=terminated,
            truncateds=truncated,
            extra_model_outputs=extra_model_outputs,
        )

        # We collected enough samples -> Send them to server.
        if sum(map(len, episodes)) == env_steps_per_sample:
            # Send the data to the server.
            send_rllink_message(
                sock_,
                {
                    "type": RLlink.EPISODES_AND_GET_STATE.name,
                    "episodes": [
                        e.get_state(exclude_agent_to_module_mapping_fn=True)
                        for e in episodes
                    ],
                    "timesteps": env_steps_per_sample,
                },
            )
            # We are forced to sample on-policy. Have to wait for a response
            # with the state (weights) in it.
            msg_type, msg_body = get_rllink_message(sock_)
            assert msg_type == RLlink.SET_STATE
            weights_seq_no = _set_state(msg_body["state"], rl_module)

            episodes = []
            if not episode.is_done:
                episode = episode.cut()
                episodes.append(episode)

        # If episode is done, reset env and create a new episode.
        if episode.is_done:
            obs, _ = env.reset()
            episode = MultiAgentEpisode(
                observations=[obs],
                observation_space=Dict(env.observation_spaces),
                action_space=Dict(env.action_spaces),
                agent_module_ids={
                    aid: policy_mapping_fn(aid) for aid in env.possible_agents
                },
            )
            episodes.append(episode)
            shared_data = {}
