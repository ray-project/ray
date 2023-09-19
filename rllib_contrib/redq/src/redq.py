import logging
from typing import Optional, Type

from redq_torch_policy import REDQTorchPolicy

from ray.rllib.algorithms.algorithm_config import AlgorithmConfig, NotProvided
from ray.rllib.algorithms.dqn.dqn import DQN
from ray.rllib.algorithms.sac import SACConfig
from ray.rllib.policy.policy import Policy
from ray.rllib.utils.annotations import override
from ray.rllib.utils.deprecation import Deprecated
from ray.rllib.utils.framework import try_import_tf, try_import_tfp

tf1, tf, tfv = try_import_tf()
tfp = try_import_tfp()

logger = logging.getLogger(__name__)


class REDQConfig(SACConfig):
    """Defines a configuration class from which a REDQ Algorithm can be built.
       Subclass of SACConfig

    Example:
        >>> config = REDQConfig().training(gamma=0.9, lr=0.01)  # doctest: +SKIP
        >>> config = config.resources(num_gpus=0)  # doctest: +SKIP
        >>> config = config.rollouts(num_rollout_workers=4)  # doctest: +SKIP
        >>> print(config.to_dict())  # doctest: +SKIP
        >>> # Build a Algorithm object from the config and run 1 training iteration.
        >>> algo = config.build(env="CartPole-v1")  # doctest: +SKIP
        >>> algo.train()  # doctest: +SKIP
    """

    def __init__(self, algo_class=None):
        super().__init__(algo_class=algo_class or REDQ)
        self.ensemble_size = 2
        self.num_critics = 2
        self.target_q_fcn_aggregator = "min"
        self.q_fcn_aggregator = "mean"  # to get SAC set to 'min'

    @override(SACConfig)
    def training(
        self,
        *,
        ensemble_size: Optional[int] = NotProvided,
        num_critics: Optional[int] = NotProvided,
        target_q_fcn_aggregator: Optional[str] = NotProvided,
        q_fcn_aggregator: Optional[str] = NotProvided,
        **kwargs,
    ) -> "REDQConfig":
        """Sets the training related configuration.

        Args:
            ensemble_size (Optional[int]): ensemble size for the target q functions
                and the q functions (default value: 2)

            num_critics (Optional[int]): the number of critics used for building
                target labels (default value: 2)

            target_q_fcn_aggregator (Optional[str]): a string ('min' or 'mean')
                determining an aggregator function for target Q functions
                in target labels computations

            q_fcn_aggregator (Optional[str]): a string ('min' or 'mean') determining
                an aggregator function for Q functions in policy loss computation

        Returns:
            This updated AlgorithmConfig object.
        """
        super().training(**kwargs)
        if ensemble_size is not NotProvided:
            self.ensemble_size = ensemble_size
        if num_critics is not NotProvided:
            self.num_critics = num_critics
        if target_q_fcn_aggregator is not NotProvided:
            self.target_q_fcn_aggregator = target_q_fcn_aggregator
        if q_fcn_aggregator is not NotProvided:
            self.q_fcn_aggregator = q_fcn_aggregator
        return self

    @override(SACConfig)
    def validate(self) -> None:
        # Call super's validation method.
        super().validate()
        assert (
            self.ensemble_size >= self.num_critics
        ), "Number of critics should be smaller or equal to ensemble size"
        assert (
            type(self.q_fcn_aggregator) is str
        ), "Target predicion is a string: 'min' or 'mean'"
        assert (
            type(self.target_q_fcn_aggregator) is str
        ), "Target predicion is a string: 'min' or 'mean'"


class REDQ(DQN):
    "A REDQ implementation based on SAC."

    def __init__(self, *args, **kwargs):
        self._allow_unknown_subkeys += ["policy_model_config", "q_model_config"]
        super().__init__(*args, **kwargs)

    @classmethod
    @override(DQN)
    def get_default_config(cls) -> AlgorithmConfig:
        return REDQConfig()

    @classmethod
    @override(DQN)
    def get_default_policy_class(
        cls, config: AlgorithmConfig
    ) -> Optional[Type[Policy]]:
        if config["framework"] == "torch":
            return REDQTorchPolicy
        else:
            raise NotImplementedError


class _deprecated_default_config(dict):
    def __init__(self):
        super().__init__(REDQConfig().to_dict())

    @Deprecated(
        old="redq::DEFAULT_CONFIG",
        new="redq::REDQConfig(...)",
        error=True,
    )
    def __getitem__(self, item):
        return super().__getitem__(item)


DEFAULT_CONFIG = _deprecated_default_config()
