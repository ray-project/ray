import logging
from typing import Optional

import numpy as np

from ray.rllib.algorithms.algorithm import Algorithm
from ray.rllib.algorithms.algorithm_config import AlgorithmConfig, NotProvided
from ray.rllib.algorithms.appo.torch.appo_torch_learner import APPOTorchLearner
from ray.rllib.algorithms.seed.utils.seed_inference import SEEDInference
from ray.rllib.algorithms.seed.utils.seed_env_runner import SEEDEnvRunner
from ray.rllib.core.rl_module.rl_module import RLModuleSpec
from ray.rllib.utils.annotations import override
from ray.util.anyscale.zmq_channel import RouterChannel

logger = logging.getLogger(__name__)


class SEEDConfig(AlgorithmConfig):
    """The config class for SEED."""

    def __init__(self, algo_class=None):
        """Initializes a SEEDConfig instance."""
        super().__init__(algo_class=algo_class or SEED)

        self.inference_batch_size = 2

        # Override env_runner_cls to use the SEED-specific EnvRunner.
        self.env_runner_cls = SEEDEnvRunner

        # Override some of AlgorithmConfig's default values
        self.min_time_s_per_iteration = 10

        # TODO (sven): APPO Learner.
        self.circular_buffer_num_batches = 1
        self.circular_buffer_iterations_per_batch = 1

        # ZMQ-based Router-Dealer communication pattern
        self._zmq_asyncio = False
        self._router_channel_max_num_actors = 1_000
        self._max_outbound_messages = 1_000_000
        self._max_inbound_messages = 1_000_000

    @override(AlgorithmConfig)
    def get_default_rl_module_spec(self) -> RLModuleSpec:
        if self.framework_str == "torch":
            from ray.rllib.algorithms.appo.torch.appo_torch_rl_module import (
                APPOTorchRLModule as RLModule,
            )
        else:
            raise ValueError(
                f"The framework {self.framework_str} is not supported. "
                "Use either 'torch' or 'tf2'."
            )

        return RLModuleSpec(module_class=RLModule)

    @override(AlgorithmConfig)
    def get_default_learner_class(self):
        if self.framework_str == "torch":
            return APPOTorchLearner
        elif self.framework_str in ["tf2", "tf"]:
            raise ValueError(
                "TensorFlow is no longer supported on the new API stack! "
                "Use `framework='torch'`."
            )
        else:
            raise ValueError(
                f"The framework {self.framework_str} is not supported. "
                "Use `framework='torch'`."
            )

    def inference(
        self,
        *,
        inference_batch_size: Optional[int] = NotProvided,
    ) -> "SEEDConfig":
        """Sets the inference related configuration."""
        if inference_batch_size is not NotProvided:
            self.inference_batch_size = inference_batch_size

        return self

    @override(AlgorithmConfig)
    def training(
        self,
        **kwargs,
    ) -> "SEEDConfig":
        """Sets the training related configuration."""
        super().training(**kwargs)
        return self

    @override(AlgorithmConfig)
    def validate(self) -> None:
        # Call the super class' validation method first.
        super().validate()


class SEED(Algorithm):
    """SEED Algorithm class."""

    @classmethod
    @override(Algorithm)
    def get_default_config(cls) -> AlgorithmConfig:
        return SEEDConfig()

    @override(Algorithm)
    def setup(self, config: SEEDConfig):
        super().setup(config)

        # Create RouterChannel
        self.router_channel = RouterChannel(
            _asyncio=self.config._zmq_asyncio,
            max_num_actors=self.config._router_channel_max_num_actors,
            max_outbound_messages=self.config._max_outbound_messages,
            max_inbound_messages=self.config._max_inbound_messages,
        )

        _dealer_channels = {
            aid: self.router_channel.create_dealer(
                actor=actor,
                _asyncio=self.config._zmq_asyncio,
            )
            for aid, actor in self.env_runner_group._worker_manager.actors().items()
        }
        self.env_runner_group.foreach_env_runner(
            [
                lambda er, _ch=dealer_ch: er.start_zmq(dealer_channel=_ch)
                for dealer_ch in _dealer_channels.values()
            ],
            remote_worker_ids=list(_dealer_channels.keys()),
            local_env_runner=False,
        )
        self.env_runner_group.foreach_env_runner(
            "sample", timeout_seconds=0.0, local_env_runner=False
        )

        for aid, actor in self.env_runner_group._worker_manager.actors().items():
            print(f"  EnvRunners" f" - ID: {aid}" f" - ActorHandle: {actor}")
            _action = np.random.rand(5).astype(np.float32)
            self.router_channel.write(
                actor=actor,
                message=_action.tobytes(),
            )

        # Create and start the inference thread.
        self.inference_thread = SEEDInference(
            config=self.config,
            router_channel=self.router_channel,
            env=self.env_runner.env,
            metrics=self.metrics,
        )
        self.inference_thread.start()

    @override(Algorithm)
    def training_step(self):
        pass
