from collections import defaultdict
import threading
from typing import Dict, List

import numpy as np

import ray
from ray.rllib.core import Columns, DEFAULT_MODULE_ID
from ray.rllib.core.rl_module.rl_module import RLModuleSpec
from ray.rllib.env import INPUT_ENV_SPACES
from ray.rllib.env.single_agent_episode import SingleAgentEpisode
from ray.rllib.utils.annotations import override


class SEEDInference(threading.Thread):
    """SEED: ZMQ communication pattern PoC"""

    def __init__(
        self,
        *,
        config,
        router_channel,
        env,
        metrics,
    ):
        super().__init__()

        self.config = config
        self.metrics = metrics
        self._router_channel = router_channel
        self.env = env
        self.num_envs = self.env.num_envs

        self._env_to_module = config.build_env_to_module_connector(self.env)
        self.module = None
        self.make_module()
        self._module_to_env = config.build_module_to_env_connector(self.env)

        # Dict mapping EnvRunner ActorID to lists (env vector indices) of ongoing episodes.
        self._episodes: Dict[ray.ActorID, List[SingleAgentEpisode]] = {}
        self._inference_batch_count = 0

        # Dicts mapping EnvRunner ActorID to lists (env vector indices) of previously
        # computed actions and model outputs.
        # These buffered values need to be passed into the `.add_env_step()` method
        # call of the episodes.
        self._buffered_actions = defaultdict(list)
        self._buffered_extra_model_outputs = defaultdict(list)

        self._episodes_for_next_inference_batch = []
        self._env_runners_for_actions = []

        self._inference_batch_size = max(
            self.config.inference_batch_size,
            self.config.num_env_runners * self.num_envs,
        )

    @override(threading.Thread)
    def run(self):
        while True:
            self._forward_inference()

    def make_module(self):
        try:
            module_spec: RLModuleSpec = self.config.get_rl_module_spec(
                env=self.env.unwrapped, spaces=self.get_spaces(), inference_only=True
            )
            # Build the module from its spec.
            self.module = module_spec.build()

            # TODO (sven): Move the RLModule to our device.
            # TODO (sven): In order to make this framework-agnostic, we should maybe
            #  make the RLModule.build() method accept a device OR create an additional
            #  `RLModule.to()` override.
            # self.module.to(self._device)

        # If `AlgorithmConfig.get_rl_module_spec()` is not implemented, this env runner
        # will not have an RLModule, but might still be usable with random actions.
        except NotImplementedError:
            self.module = None

    # TODO (sven): In case we need to go back to an actor/distributed design, use this
    #  method to (remote) set the dealer channel attribute.
    # def start_zmq(self, dealer_channel):
    #    self._dealer_channel = dealer_channel

    def get_spaces(self):
        return {
            INPUT_ENV_SPACES: (self.env.observation_space, self.env.action_space),
            DEFAULT_MODULE_ID: (
                self._env_to_module.observation_space,
                self.env.single_action_space,
            ),
        }

    def _forward_inference(self):
        # receive the message from the RouterChannel
        (
            observations,
            rewards,
            terminateds,
            truncateds,
        ), env_runner = self._router_channel.read()
        # TODO (sven): Support arbitrary obs spaces.
        observations = np.frombuffer(observations, dtype=np.float32).reshape(
            self.env.observation_space.shape
        )
        num_observations = len(observations)
        rewards = np.frombuffer(rewards, dtype=np.float64)
        terminateds = np.frombuffer(terminateds, dtype=bool)
        truncateds = np.frombuffer(truncateds, dtype=bool)

        # Add observations to the running episodes.
        if env_runner._actor_id not in self._episodes:
            self._episodes[env_runner._actor_id] = [
                SingleAgentEpisode(
                    observation_space=self.env.single_observation_space,
                    action_space=self.env.single_action_space,
                )
                for _ in range(self.num_envs)
            ]
        episodes_for_inference_batch = self._episodes[env_runner._actor_id][:]

        for vec_idx, observation in enumerate(observations):
            episode = episodes_for_inference_batch[vec_idx]
            if not episode.is_reset:
                episode.add_env_reset(observation=observation)
            else:
                self._episodes[env_runner._actor_id][vec_idx].add_env_step(
                    observation=observation,
                    action=self._buffered_actions[env_runner._actor_id][vec_idx],
                    reward=float(rewards[vec_idx]),
                    terminated=bool(terminateds[vec_idx]),
                    truncated=bool(truncateds[vec_idx]),
                    extra_model_outputs=self._buffered_extra_model_outputs[
                        env_runner._actor_id
                    ][vec_idx],
                )

                # Start a new episode, if this one has been terminated.
                if terminateds[vec_idx] or truncateds[vec_idx]:
                    self._episodes[env_runner._actor_id][vec_idx] = SingleAgentEpisode(
                        observation_space=self.env.single_observation_space,
                        action_space=self.env.single_action_space,
                    )

        # TODO (sven): After adding a step to the episodes, send the latest steps
        #  also to the aggregator actors, so they can themselves build episodes
        #  and use these to build train batches.

        # Increase the inference batch counter.
        self._inference_batch_count += num_observations
        self._episodes_for_next_inference_batch.extend(episodes_for_inference_batch)
        self._env_runners_for_actions.append(env_runner)

        # If we have enough samples for an inference batch, create it and perform
        # a forward pass.
        if self._inference_batch_count >= self._inference_batch_size:
            # Env-to-module connector.
            shared_data = {}
            batch = self._env_to_module(
                episodes=self._episodes_for_next_inference_batch,
                explore=self.config.explore,
                rl_module=self.module,
                shared_data=shared_data,
                metrics=self.metrics,
            )
            # Compute actions.
            module_output = self.module.forward_exploration(batch)

            # Module-to-env connector.
            to_env = self._module_to_env(
                batch=module_output,
                episodes=self._episodes_for_next_inference_batch,
                explore=self.config.explore,
                rl_module=self.module,
                shared_data=shared_data,
                metrics=self.metrics,
            )
            all_actions = to_env.pop(Columns.ACTIONS)
            all_actions_for_env = to_env.pop(Columns.ACTIONS_FOR_ENV, all_actions)

            for idx, env_runner_for_action in enumerate(self._env_runners_for_actions):
                actions = all_actions_for_env[
                    idx * self.num_envs : (idx + 1) * self.num_envs
                ]
                # Send all computed actions back to their respective EnvRunners.
                self._router_channel.write(
                    actor=env_runner_for_action,
                    message=actions.tobytes(),
                )
                self._buffered_actions[env_runner_for_action._actor_id] = actions

                extra_model_output = [
                    {k: v[idx * self.num_envs + vec_idx] for k, v in to_env.items()}
                    for vec_idx in range(self.num_envs)
                ]
                # extra_model_output[WEIGHTS_SEQ_NO] = self._weights_seq_no
                self._buffered_extra_model_outputs[
                    env_runner_for_action._actor_id
                ] = extra_model_output

            self._episodes_for_next_inference_batch = []
            self._inference_batch_count = 0
            self._env_runners_for_actions = []
