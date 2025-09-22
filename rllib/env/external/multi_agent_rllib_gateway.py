import logging
import pickle
import socket
import threading
import time
from collections import defaultdict
from pathlib import Path

import gymnasium as gym
import numpy as np
import tree

from ray.rllib.algorithms import AlgorithmConfig, Algorithm
from ray.rllib.connectors.env_to_module import EnvToModulePipeline
from ray.rllib.connectors.module_to_env import ModuleToEnvPipeline
from ray.rllib.core import Columns, COMPONENT_RL_MODULE, COMPONENT_ENV_RUNNER, \
    COMPONENT_ENV_TO_MODULE_CONNECTOR, COMPONENT_LEARNER, \
    COMPONENT_MODULE_TO_ENV_CONNECTOR, COMPONENT_LEARNER_GROUP
from ray.rllib.core.rl_module import MultiRLModule
from ray.rllib.env import INPUT_ENV_SPACES
from ray.rllib.env.external.rllink import (
    get_rllink_message,
    send_rllink_message,
    RLlink,
)
from ray.rllib.env.multi_agent_env_runner import MultiAgentEnvRunner
from ray.rllib.env.multi_agent_episode import MultiAgentEpisode
from ray.rllib.utils.metrics import WEIGHTS_SEQ_NO
from ray.rllib.utils.typing import AgentID, ModuleID
from ray.util.annotations import DeveloperAPI

logger = logging.getLogger("ray.rllib")


@DeveloperAPI
class MultiAgentRLlibGateway:
    # TODO: update docstring
    """Gateway class for external, for ex. non-python, simulators to connect to RLlib.

    As long as there is a path to bind python code into your simulator's language, for
    example C++, you should be able to use the simulator very easily in connection with
    an RLlib experiment.

    You should use the gateway as follows in your C++ code:

    .. code-block:: c++

        #include <pybind11/embed.h>
        #include <pybind11/stl.h>

        namespace py = pybind11;

        int main(int argc, char** argv)
        {
            // Proper interpreter init (RAII-safe).
            py::scoped_interpreter guard{};

            // Import RLlibGateway class.
            py::object rllib_gateway_class = py::module_::import(
                "ray.rllib.env.external.rllib_gateway"
            ).attr("RLlibGateway");
            py::object rllib = rllib_gateway_class();

            // Assuming, you have a CartPole simulator class, create it and reset.
            CartPole env;
            env.reset();
            float total_reward = 0.0;
            int eps = 0;

            // Endless loop through an infinite number of episodes.
            while (true)
            {
                // Send previous reward (result of the previous action taken) and
                // current observation to get_action. If the episode has just been
                // reset, the gateway won't log it (for example, set it to 0.0).
                try
                {
                    py::gil_scoped_acquire gil;
                    py::object action = rllib.attr("get_action")(
                        env.reward,
                        env.observation
                    );
                    // Apply the locally computed action in the simulation.
                    env.step(action.cast<int>());
                }
                catch (const py::error_already_set& e)
                {
                    std::cerr << "[Python error in get_action]\n" << e.what() << std::endl;
                    break;
                }

                // Send last reward and last observation to episode_done().
                if (env.terminated || env.truncated)
                {
                    try {
                        py::gil_scoped_acquire gil;
                        rllib.attr("episode_done")(
                            env.reward,
                            env.observation,
                            env.truncated
                        );
                    }
                    catch (const py::error_already_set& e) {
                        std::cerr << "[Python error in get_action (episode done)]\n"
                                  << e.what() << std::endl;
                        break;
                    }
                    // Reset episode to start a new one.
                    env.reset();
                    // Report episode's total return.
                    std::cout << "Episode " << eps << " return: " << total_reward << "\n";
                    total_reward = 0.0f;
                    eps += 1;
                }
                total_reward += env.reward;
            }
            return 0;
        }

    The gateway automatically tries to connect to the given address and port, where
    an RLlib EnvRunner should be listening as a service.

    Once connected to an RLlib EnvRunner, the gateway receives the RLlib algo config
    and the current state of the EnvRunner (model weights and connector states).
    It then constructs the local RLModule and connector pipelines (env-to-module
    and module-to-env), through which it's enabled to compute actions locally.

    As a user of the gateway, its `get_action` API is the only method you need to call
    from within your simulator's code (for example C++), always passing it the
    previously received reward, the current observation, and whether the episode is
    terminated/truncated. Right after an episode reset, the reward passed in should be
    0.0 and the observation passed in should be the first observation with which the
    episode starts.

    The gateway also takes care of frequently sending batches of recorded episodes
    back to the connected EnvRunner for model updating purposes and then waits for
    the latest model weights and connector states.
    """

    def __init__(
        self,
        address: str = "localhost",
        port: int = 5556,
        socket_timeout: float = 60.0,
        inference_only: bool = False,
        checkpoint_path: str | None = None,
    ):
        """Initializes a RLlibGateway instance.

        Args:
            address: The address under which to connect to the RLlib EnvRunner.
            port: The port to connect to.
            inference_only: If true, the gateway will just perform inference without
                connecting to an EnvRunner and sending the collected data to RLlib
                for training.
            checkpoint_path: Path to checkpoint to load for inference. Only used
                with inference_only=True.
        """
        # RLlib MultiAgentEpisode collection buckets.
        self._episodes = []

        self._prev_action = None
        self._prev_extra_model_outputs = None

        self._is_initialized = False

        self._inference_only = inference_only
        if self._inference_only:
            if checkpoint_path is None:
                raise ValueError(
                    "You need to provide a checkpoint_path to use RLlibGateway "
                    "in inference_only mode!"
                )
            checkpoint_path = Path(checkpoint_path)
            self._env_to_module = EnvToModulePipeline.from_checkpoint(
                checkpoint_path / COMPONENT_ENV_RUNNER /
                COMPONENT_ENV_TO_MODULE_CONNECTOR
            )
            self._rl_module = MultiRLModule.from_checkpoint(
                checkpoint_path / COMPONENT_LEARNER_GROUP /
                COMPONENT_LEARNER / COMPONENT_RL_MODULE
            )
            self._module_to_env = ModuleToEnvPipeline.from_checkpoint(
                checkpoint_path / COMPONENT_ENV_RUNNER /
                COMPONENT_MODULE_TO_ENV_CONNECTOR
            )
            self._config = Algorithm.from_checkpoint(str(checkpoint_path)).config
            self._is_initialized = True
        else:
            # The open socket connection to an RLlib EnvRunner.
            self._sock = None
            # The timesteps sampled thus far.
            self._timesteps = 0
            # The RLlib config from the ray cluster.
            self._config = None
            # EnvToModule connector pipeline.
            self._env_to_module = None
            # ModuleToEnv connector pipeline.
            self._module_to_env = None
            # The RLModule for action computations.
            self._rl_module = None
            self._weights_seq_no = 0
            # The client thread running in the background and communicating
            # with an RLlib EnvRunner.
            self._client_thread = None
            self._socket_timeout = socket_timeout

            threading.Thread(
                target=self._connect_to_server_thread_func,
                args=(address, port),
            ).start()

    @property
    def is_initialized(self):
        """Returns True, if this Gateway has an RLModule and connectors."""
        return self._is_initialized

    def _get_agent_to_module_ids(
        self, episode: MultiAgentEpisode
    ) -> dict[AgentID, ModuleID]:
        assert self._config is not None and self._config.policy_mapping_fn

        return {
            aid: self._config.policy_mapping_fn(aid, episode)
            for aid in self._config.action_space
        }

    def get_action(
        self,
        prev_reward,
        prev_observation,
        terminated,
        truncated,
    ):
        """Computes and returns a new action, given an observation.

        Args:
            prev_reward: The reward received after the previously computed action
                (returned from this method in the previous call).
            prev_observation: The current observation, from which the action should be
                computed. Note that first, `observation`, the previously returned
                action, `prev_reward`, and `terminated/truncated` are logged with the running
                episode through `Episode.add_env_step()`, then the env-to-module
                connector creates the inference forward batch for the RLModule based on
                this running episode.
        """
        # Block until we are initialized (no RLModule and no action space to
        # compute anything).
        while not self.is_initialized:
            time.sleep(0.1)

        # C++ may send observation tensors as std::vector<float> (which get translated
        # into python lists).
        def _ensure_array(obs):
            if isinstance(obs, list):
                return np.array(obs, np.float32)
            else:
                return obs
        prev_observation = tree.map_structure(_ensure_array, prev_observation)

        # Episode logging.
        if len(self._episodes) == 0 or self._episodes[-1].is_done:
            self._episodes.append(
                MultiAgentEpisode(
                    observation_space=self._config.observation_space,
                    action_space=self._config.action_space,
                )
            )
            agent_module_ids = self._get_agent_to_module_ids(self._episodes[-1])
            self._episodes[-1]._agent_to_module_mapping = agent_module_ids
            self._episodes[-1].add_env_reset(observations=prev_observation)
        else:
            # Log timestep to current episode.
            self._episodes[-1].add_env_step(
                observations=prev_observation,
                actions=self._prev_action,
                rewards=prev_reward,
                terminateds=terminated,
                truncateds=truncated,
                extra_model_outputs=self._prev_extra_model_outputs,
            )
            self._timesteps += 1

            # We collected enough samples -> Send them to server.
            if (
                not self._inference_only
                and self._timesteps == self._config.get_rollout_fragment_length()
            ):
                self._send_episode_data_to_server()
                # early out if all episodes were done because action doesn't matter
                if not self._episodes:
                    return self._prev_action

        # Model forward pass.
        shared_data = {}
        to_module = self._env_to_module(
            episodes=[self._episodes[-1]],
            rl_module=self._rl_module,
            explore=True,
            shared_data=shared_data,
        )
        model_outs = self._rl_module.forward_inference(to_module)
        # Add `module_outs` to `batch`.
        to_module.update(model_outs)
        to_env = self._module_to_env(
            episodes=[self._episodes[-1]],
            batch=to_module,
            rl_module=self._rl_module,
            explore=True,
            shared_data=shared_data,
        )
        # Extract the action that should be applied in the env.
        self._prev_action = to_env.pop(Columns.ACTIONS)
        action_for_env = to_env.pop(Columns.ACTIONS_FOR_ENV, self._prev_action)[0]
        self._prev_action = self._prev_action[0]

        extra_model_outputs = defaultdict(dict)
        # `to_env` returns a dictionary with column keys and
        # (AgentID, value) tuple values.
        for col, ma_dict_list in to_env.items():
            ma_dict = ma_dict_list[0]
            for agent_id, val in ma_dict.items():
                extra_model_outputs[agent_id][col] = val
                extra_model_outputs[agent_id][
                    WEIGHTS_SEQ_NO
                ] = self._weights_seq_no
        extra_model_outputs = dict(extra_model_outputs)

        # Store action for next timestep's logging into the episode.
        self._prev_extra_model_outputs = extra_model_outputs

        # And return the action.
        return action_for_env

    def _send_episode_data_to_server(self):
        assert sum(map(len, self._episodes)) == (
            self._config.get_rollout_fragment_length()
        )

        ongoing_episodes_continuations = [
            eps.cut(len_lookback_buffer=self._config.episode_lookback_horizon)
            for eps in self._episodes if not eps.is_done
        ]

        for eps in self._episodes:
            # Just started Episodes do not have to be returned. There is no data
            # in them anyway.
            if eps.env_t == 0:
                continue
            eps.validate()

            MultiAgentEnvRunner._prune_zero_len_sa_episodes(eps)

        # Send the data to the server.
        # On-policy: Block until response received back from server.
        if True:  # force_on_policy:
            episode_states = [
                e.get_state(exclude_agent_to_module_mapping_fn=True)
                for e in self._episodes
            ]
            msg_type, msg_body = self._try_send_receive_rllink_msg(
                {
                    "type": RLlink.EPISODES_AND_GET_STATE.name,
                    "episodes": episode_states,
                    "timesteps": self._timesteps,
                },
            )
            # We are forced to sample on-policy. Have to wait for a response
            # with the state (weights) in it.
            if msg_type != RLlink.SET_STATE:
                logger.warning(
                    "Can't SET_STATE! Connection error to RLlib "
                    f"server. {msg_body}"
                )
            else:
                self._set_state(msg_body["state"])

        # Sampling doesn't have to be on-policy -> continue collecting
        # samples.
        else:
            raise NotImplementedError

        self._timesteps = 0
        # Continue collecting into the cut Episode chunks.
        self._episodes = ongoing_episodes_continuations

    def _connect_to_server_thread_func(self, address, port):
        # Try initializing the Gateway.
        while True:
            # Try connecting to (RLlib) server.
            while True:
                try:
                    logger.info(f"Trying to connect to {address}:{port} ...")
                    self._sock = socket.socket(
                        socket.AF_INET,
                        socket.SOCK_STREAM,
                    )
                    self._sock.settimeout(self._socket_timeout)
                    self._sock.connect((address, port))
                    break
                except ConnectionRefusedError:
                    time.sleep(5)

            logger.info(f"Connected to server at {address}:{port} ...")

            # Send ping-pong.
            msg_type, msg_body = self._try_send_receive_rllink_msg(
                {"type": RLlink.PING.name},
            )
            # Error -> Retry connecting to server.
            if msg_type != RLlink.PONG:
                continue
            logger.info("\tPING/PONG ok ...")

            # Request config.
            msg_type, msg_body = self._try_send_receive_rllink_msg(
                {"type": RLlink.GET_CONFIG.name}
            )
            # Error -> Retry connecting to server.
            if msg_type != RLlink.SET_CONFIG:
                continue
            # TODO (sven): Make AlgorithmConfig msgpack'able by making it a
            #  Checkpointable with a pickle-independent state.
            self._config = AlgorithmConfig.from_state(pickle.loads(msg_body["config"]))
            # Create the RLModule and connector pipelines.
            self._env_to_module = self._config.build_env_to_module_connector()
            rl_module_spec = self._config.get_multi_rl_module_spec(
                spaces=self._get_spaces(), inference_only=True
            )
            self._rl_module = rl_module_spec.build()
            self._module_to_env = self._config.build_module_to_env_connector()
            logger.info("\tGET_CONFIG ok (built connectors and module) ...")

            # Request EnvRunner state (incl. model weights).
            msg_type, msg_body = self._try_send_receive_rllink_msg(
                {"type": RLlink.GET_STATE.name}
            )
            # Error -> Retry connecting to server.
            if msg_type != RLlink.SET_STATE:
                continue
            self._set_state(msg_body["state"])
            logger.info("\tSET_STATE ok ...")

            # Set this Gateway to `initialized` and return from the thread.
            self._is_initialized = True
            return

    def _get_spaces(self) -> dict[str, tuple[gym.Space, gym.Space]]:
        assert self._env_to_module is not None and self._config is not None
        return {
            INPUT_ENV_SPACES: (self._config.observation_space, self._config.action_space),
            **{
                mid: (o, self._env_to_module.action_space[mid])
                for mid, o in
                self._env_to_module.observation_space.spaces.items()
            },
        }

    def _set_state(self, msg_body):
        # TODO (sven): Add once our EnvRunner publishes these (right now, it doesn't
        #  even have its own connectors, for simplicity).
        # self._env_to_module.set_state(msg_body[COMPONENT_ENV_TO_MODULE_CONNECTOR])
        # self._module_to_env.set_state(msg_body[COMPONENT_MODULE_TO_ENV_CONNECTOR])
        self._rl_module.set_state(msg_body[COMPONENT_RL_MODULE])
        self._weights_seq_no = msg_body[WEIGHTS_SEQ_NO]

    def _try_send_receive_rllink_msg(self, msg):
        try:
            send_rllink_message(self._sock, msg)
            msg_type, msg_body = get_rllink_message(self._sock)
        except ConnectionError as e:
            msg_type = e.__class__
            msg_body = str(e)
            # Try closing and invalidating socket.
            try:
                self._sock.close()
                self._sock = None
            except Exception:
                time.sleep(1)
            time.sleep(2)

        return msg_type, msg_body
