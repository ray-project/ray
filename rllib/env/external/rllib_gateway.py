import logging
import pickle
import socket
import threading
import time

import numpy as np

from ray.rllib.core import Columns, COMPONENT_RL_MODULE
from ray.rllib.env.external.rllink import (
    get_rllink_message,
    send_rllink_message,
    RLlink,
)
from ray.rllib.env.single_agent_episode import SingleAgentEpisode
from ray.rllib.utils.metrics import WEIGHTS_SEQ_NO
from ray.util.annotations import DeveloperAPI

logger = logging.getLogger("ray.rllib")


@DeveloperAPI
class RLlibGateway:
    """Gateway class for external, for ex. non-python, simulators to connect to RLlib.

    As long as there is a path to bind python code into your simulator's language, for
    example C++, you should be able to use the simulator very easily in connection with
    an RLlib experiment.

    C++ example:
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
                        env.reward,  // 0.0 if episode just started with a reset-obs.
                        env.observation,
                        false,
                        false
                    );
                    // Apply the locally computed action in the simulation.
                    env.step(action.cast<int>());
                }
                catch (const py::error_already_set& e)
                {
                    std::cerr << "[Python error in get_action]\n" << e.what() << std::endl;
                    break;
                }

                // Send last reward and last observation (with dummy-action request) to
                // get_action.
                if (env.terminated || env.truncated)
                {
                    try {
                        py::gil_scoped_acquire gil;
                        rllib.attr("get_action")(
                            env.reward,
                            env.observation,
                            env.terminated,
                            env.truncated
                        );
                    }
                    catch (const py::error_already_set& e) {
                        std::cerr << "[Python error in get_action (episode done)]\n" << e.what() << std::endl;
                        break;
                    }
                    // Episode is done, reset it to start a new one.
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

    def __init__(self, address: str = "localhost", port: int = 5556):
        """Initializes a RLlibGateway instance.

        Args:
            address: The address under which to connect to the RLlib EnvRunner.
            port: The post to connect to.
        """
        # RLlib SingleAgentEpisode collection buckets.
        self._episodes = []
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
        # The client thread running in the background and communicating with an RLlib
        # EnvRunner.
        self._client_thread = None

        self._prev_action = None
        self._prev_extra_model_outputs = None

        threading.Thread(
            target=self._connect_to_server_thread_func,
            args=(address, port),
        ).start()

    def get_action(self, prev_reward, next_observation, terminated, truncated):
        """Computes and returns a new action, given an observation.

        Args:
            prev_reward: The reward received after the previously computed action
                (returned from this method in the previous call).
            next_observation: The current observation, from which the action should be
                computed. Note that first, `observation`, the previously returned
                action, `prev_reward`, and `terminated/truncated` are logged with the running
                episdode through `Episode.add_env_step()`, then the env-to-module
                connector creates the inference forward batch for the RLModule based on
                this running episode.
            terminated: Whether the episode is terminated. If True, `next_observation`
                is the terminal observation of the episode and `prev_reward` is the last
                reward that the agent receives in the episode.
            truncated: Whether the episode is truncated (done). If True,
                `next_observation` is the observation right before the truncation point
                and `prev_reward` is the last reward that the agent receives in the
                episode. A truncated episode's last observation should still be used to
                compute value function estimates at the truncation point.
        """
        # TODO (sven): Block until we have created our model.
        while self._module_to_env is None:
            time.sleep(0.01)

        # C++ may send observation tensors as std::vector<float> (which get translated
        # into python lists).
        if isinstance(next_observation, list):
            next_observation = np.array(next_observation, np.float32)

        # Episode logging.
        if len(self._episodes) == 0 or self._episodes[-1].is_done:
            self._episodes.append(SingleAgentEpisode())
            self._episodes[-1].add_env_reset(observation=next_observation)
        else:
            # Log timestep to current episode.
            self._episodes[-1].add_env_step(
                observation=next_observation,
                action=self._prev_action,
                reward=prev_reward,
                terminated=terminated,
                truncated=truncated,
                extra_model_outputs=self._prev_extra_model_outputs,
            )
            self._timesteps += 1

            # TODO (sven): If enough timesteps have been collected, send out episodes
            #  through socket to RLlib server for training.
            # We collected enough samples -> Send them to server.
            if self._timesteps == self._config.get_rollout_fragment_length():
                assert sum(map(len, self._episodes)) == (
                    self._config.get_rollout_fragment_length()
                )

                # Send the data to the server.
                # On-policy: Block until response received back from server. Note that
                # this may halt the simulation calling this function (`get_action`) for
                # a while.
                if True:  # force_on_policy:
                    send_rllink_message(
                        self._sock,
                        {
                            "type": RLlink.EPISODES_AND_GET_STATE.name,
                            "episodes": [e.get_state() for e in self._episodes],
                            "timesteps": self._timesteps,
                        },
                    )
                    # We are forced to sample on-policy. Have to wait for a response
                    # with the state (weights) in it.
                    msg_type, msg_body = get_rllink_message(self._sock)
                    assert msg_type == RLlink.SET_STATE
                    self._set_state(msg_body["state"])

                # Sampling doesn't have to be on-policy -> continue collecting
                # samples.
                else:
                    raise NotImplementedError

                self._timesteps = 0
                self._episodes = [
                    eps.cut(len_lookback_buffer=1)
                    for eps in self._episodes
                    if not eps.is_done
                ]

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

        extra_model_output = {k: v[0] for k, v in to_env.items()}
        extra_model_output[WEIGHTS_SEQ_NO] = self._weights_seq_no

        # Store action for next timestep's logging into the episode.
        self._prev_extra_model_outputs = extra_model_output

        # And return the action.
        return action_for_env

    def _set_state(self, msg_body):
        # TODO (sven): Add once our EnvRunner publishes these (right now, it doesn't
        #  even have its own connectors, for simplicity).
        # self._env_to_module.set_state(msg_body[COMPONENT_ENV_TO_MODULE_CONNECTOR])
        # self._module_to_env.set_state(msg_body[COMPONENT_MODULE_TO_ENV_CONNECTOR])
        self._rl_module.set_state(msg_body[COMPONENT_RL_MODULE])
        self._weights_seq_no = msg_body[WEIGHTS_SEQ_NO]

    def _connect_to_server_thread_func(self, address, port):
        # Try connecting to server.
        while True:
            try:
                logger.info(f"Trying to connect to {address}:{port} ...")
                self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                # self._sock.settimeout(120.0)
                self._sock.connect((address, port))
                break
            except ConnectionRefusedError:
                time.sleep(5)

        logger.info(f"Connected to server at {address}:{port} ...")

        # Send ping-pong.
        send_rllink_message(self._sock, {"type": RLlink.PING.name})
        msg_type, msg_body = get_rllink_message(self._sock)
        assert msg_type == RLlink.PONG

        logger.info("\tPING/PONG ok ...")

        # Request config.
        send_rllink_message(self._sock, {"type": RLlink.GET_CONFIG.name})
        msg_type, msg_body = get_rllink_message(self._sock)
        assert msg_type == RLlink.SET_CONFIG
        # TODO (sven): Make AlgorithmConfig msgpack'able by making it a
        #  Checkpointable with a pickle-independent state.
        self._config = pickle.loads(msg_body["config"])
        # Create the RLModule and connector pipelines.
        self._env_to_module = self._config.build_env_to_module_connector()
        rl_module_spec = self._config.get_rl_module_spec()
        self._rl_module = rl_module_spec.build()
        self._module_to_env = self._config.build_module_to_env_connector()

        logger.info("\tGET_CONFIG ok (built connectors and module) ...")

        # Request EnvRunner state (incl. model weights).
        send_rllink_message(self._sock, {"type": RLlink.GET_STATE.name})
        msg_type, msg_body = get_rllink_message(self._sock)
        assert msg_type == RLlink.SET_STATE
        self._set_state(msg_body["state"])

        logger.info("\tSET_STATE ok ...")

