import logging
from typing import Any, Dict, Optional

import gymnasium as gym

from ray.rllib.core import (
    DEFAULT_MODULE_ID,
)
from ray.rllib.core.rl_module.rl_module import RLModule, RLModuleSpec
from ray.rllib.env import INPUT_ENV_SINGLE_SPACES, INPUT_ENV_SPACES
from ray.rllib.utils.error import ERR_MSG_INVALID_ENV_DESCRIPTOR, EnvError
from ray.rllib.utils.framework import get_device
from ray.tune.registry import ENV_CREATOR, _global_registry
from ray.util.annotations import PublicAPI

logger = logging.getLogger("ray.rllib")


@PublicAPI(stability="alpha")
class InferenceActor:
    def __init__(self, *, config, **kwargs):
        self.config = config

        self._device = get_device(
            config=self.config,
            num_gpus_requested=self.config.inference_num_gpus_per_actor,
        )

        self.env: Optional[gym.Env] = None
        self.make_env()
        self.spaces: Optional[Dict] = self.get_spaces()

        self.module: Optional[RLModule] = None
        self.make_module()

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
        """Pass batch to the RLModule."""
        return self.module._forward_exploration(batch, **kwargs)

    def forward_inference(self, batch: Dict[str, Any], **kwargs) -> Dict[str, Any]:
        """Pass batch to the RLModule."""
        return self.module._forward_inference(batch, **kwargs)
