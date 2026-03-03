import logging
from typing import Any, Dict, Optional

import gymnasium as gym

from ray.rllib.connectors.env_to_module import (
    NumpyToTensor,
)
from ray.rllib.connectors.module_to_env import (
    GetActions,
    ModuleToEnvPipeline,
    TensorToNumpy,
)
from ray.rllib.core import (
    DEFAULT_MODULE_ID,
)
from ray.rllib.core.rl_module.rl_module import RLModule, RLModuleSpec
from ray.rllib.env import INPUT_ENV_SINGLE_SPACES, INPUT_ENV_SPACES
from ray.rllib.env.single_agent_episode import SingleAgentEpisode
from ray.rllib.utils.error import ERR_MSG_INVALID_ENV_DESCRIPTOR, EnvError
from ray.rllib.utils.framework import get_device
from ray.tune.registry import ENV_CREATOR, _global_registry
from ray.util.annotations import PublicAPI

logger = logging.getLogger("ray.rllib")


@PublicAPI(stability="alpha")
class InferenceActor:
    def __init__(self, *, config, **kwargs):
        self.config = config
        self.explore = self.config.explore

        self._device = get_device(
            config=self.config,
            num_gpus_requested=self.config.inference_num_gpus_per_actor,
        )

        self.env: Optional[gym.Env] = None
        self.make_env()

        spaces = self.get_spaces()
        self.spaces: Optional[Dict] = spaces

        self.module: Optional[RLModule] = None
        self.make_module()

        self.numpy_to_tensor = NumpyToTensor(device=self._device)
        self.gpu_to_cpu_pipeline = self._build_gpu_to_cpu_pipeline()

    def make_env(self) -> None:
        """Creates a vectorized gymnasium env and stores it in `self.env`.

        Note that users can change the EnvRunner's config (e.g. change
        `self.config.env_config`) and then call this method to create new environments
        with the updated configuration.
        """
        # If an env already exists, try closing it first
        # to allow it to properly clean up.
        if self.env is not None:
            try:
                self.env.close()
            except Exception as e:
                logger.warning(
                    "Tried closing the existing env, but failed with error: "
                    f"{e.args[0]}"
                )

        env_config = self.config.env_config

        # No env provided -> Error.
        if not self.config.env:
            raise ValueError(
                "`config.env` is not provided! "
                "You should provide a valid environment to your config through "
                "`config.environment([env descriptor e.g. 'CartPole-v1'])`."
            )
        # Register env for the local context.
        # Note, `gym.register` has to be called on each worker.
        elif isinstance(self.config.env, str) and _global_registry.contains(
            ENV_CREATOR, self.config.env
        ):
            env_name = "rllib-single-agent-env-inference-v0"
            entry_point = _global_registry.get(ENV_CREATOR, self.config.env)
            gym.register(
                id=env_name,
                entry_point=lambda: entry_point(env_config),
            )
            env_config = {}
        elif callable(self.config.env):
            env_name = "rllib-single-agent-env-inference-v0"
            gym.register(
                id=env_name,
                entry_point=lambda: self.config.env(env_config),
            )
            env_config = {}
        else:
            env_name = self.config.env

        try:
            self.env = gym.make(
                id=env_name,
                **env_config,
            )
        except gym.error.Error as e:
            raise EnvError(
                ERR_MSG_INVALID_ENV_DESCRIPTOR.format(self.config.env)
            ) from e

    def make_module(self):
        env = self.env.unwrapped if self.env is not None else None
        try:
            module_spec: RLModuleSpec = self.config.get_rl_module_spec(
                env=env, spaces=self.get_spaces(), inference_only=True
            )

            self.module = module_spec.build()
            self.module.to(self._device)

        # If `AlgorithmConfig.get_rl_module_spec()` is not implemented, env runner
        # will not have an RLModule, but might still be usable with random actions.
        except NotImplementedError:
            self.module = None

    def get_spaces(self):
        if self.env is None:
            return self.spaces
        return {
            INPUT_ENV_SPACES: (self.env.observation_space, self.env.action_space),
            INPUT_ENV_SINGLE_SPACES: (
                self.env.observation_space,
                self.env.action_space,
            ),
            DEFAULT_MODULE_ID: (
                self.env.observation_space,
                self.env.action_space,
            ),
        }

    def forward_exploration(self, batch: Dict[str, Any], **kwargs) -> Dict[str, Any]:
        """Pass batch to the RLModule.

        Handle parts of the EnvToModule and ModuleToEnv pipelines that can be run on GPUs
        """

        batch = self.numpy_to_tensor(
            rl_module=self.module,
            batch=batch,
            episodes=None,
        )

        to_env = self.module._forward_exploration(batch, **kwargs)
        to_env = self.gpu_to_cpu_pipeline(
            rl_module=self.module,
            batch=to_env,
            episodes=[SingleAgentEpisode(),], # hack
            explore=self.explore,
        )

        return to_env

    def forward_inference(self, batch: Dict[str, Any], **kwargs) -> Dict[str, Any]:
        """Pass batch to the RLModule."""
        return self.module._forward_inference(batch, **kwargs)

    def _build_gpu_to_cpu_pipeline(self):
        env = self.env
        spaces = self.spaces

        if env is not None:
            obs_space = getattr(env, "single_observation_space", env.observation_space)
        elif spaces is not None and INPUT_ENV_SINGLE_SPACES in spaces:
            obs_space = spaces[INPUT_ENV_SINGLE_SPACES][0]
        else:
            obs_space = self.config.observation_space
        if obs_space is None and self.config.is_multi_agent:
            obs_space = gym.spaces.Dict(
                {
                    aid: env.envs[0].unwrapped.get_observation_space(aid)
                    for aid in env.envs[0].unwrapped.possible_agents
                }
            )
        if env is not None:
            act_space = getattr(env, "single_action_space", env.action_space)
        elif spaces is not None and INPUT_ENV_SINGLE_SPACES in spaces:
            act_space = spaces[INPUT_ENV_SINGLE_SPACES][1]
        else:
            act_space = self.config.action_space
        if act_space is None and self.config.is_multi_agent:
            act_space = gym.spaces.Dict(
                {
                    aid: env.envs[0].unwrapped.get_action_space(aid)
                    for aid in env.envs[0].unwrapped.possible_agents
                }
            )
        pipeline = ModuleToEnvPipeline(
            input_observation_space=obs_space,
            input_action_space=act_space,
            connectors=[],
        )
        pipeline.append(GetActions())
        pipeline.append(TensorToNumpy())

        return pipeline
