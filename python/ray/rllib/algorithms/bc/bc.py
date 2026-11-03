from ray.rllib.algorithms.algorithm_config import AlgorithmConfig
from ray.rllib.algorithms.marwil.marwil import MARWIL, MARWILConfig
from ray.rllib.core.rl_module.rl_module import RLModuleSpec
from ray.rllib.utils.annotations import override
from ray.rllib.utils.typing import RLModuleSpecType


class BCConfig(MARWILConfig):
    """Defines a configuration class from which a new BC Algorithm can be built

    Examples:
        >>> from pathlib import Path
        >>> import gymnasium as gym
        >>> from ray.rllib.algorithms.bc import BCConfig
        >>> # Get the path to the CartPole data in the rllib folder.
        >>> base_path = Path(__file__).parents[2]
        >>> data_path = base_path / "offline/tests/data/cartpole/cartpole-v1_large"
        >>> config = BCConfig().training(lr=0.00001, gamma=0.99)
        >>> # Set the observation and action spaces of the data.
        >>> env = gym.make("CartPole-v1")
        >>> config = config.environment(
        ...     observation_space=env.observation_space,
        ...     action_space=env.action_space,
        ... )
        >>> config = config.offline_data(
        ...     input_=[data_path.as_posix()],
        ...     # Run exactly one update per training iteration.
        ...     dataset_num_iters_per_learner=1,
        ... )
        >>> # Build an Algorithm object from the config and run 1 training iteration.
        >>> algo = config.build()
        >>> _ = algo.train()
        >>> from ray.rllib.algorithms.bc import BCConfig
        >>> from ray import tune
        >>> config = BCConfig()
        >>> # Print out some default values.
        >>> print(config.beta)
        0.0
        >>> # Update the config object.
        >>> _ = config.training(lr=tune.grid_search([0.001, 0.0001]))
        >>> # Set the config object's data path.
        >>> _ = config.offline_data(
        ...     input_=[data_path.as_posix()],
        ...     dataset_num_iters_per_learner=1,
        ... )
        >>> # Set the observation and action spaces of the data.
        >>> _ = config.environment(
        ...     observation_space=env.observation_space,
        ...     action_space=env.action_space,
        ... )
        >>> # Use to_dict() to get the old-style python config dict
        >>> # when running with tune.
        >>> print("[ignore]"); results = tune.Tuner(
        ...     "BC",
        ...     param_space=config.to_dict(),
        ...     run_config=tune.RunConfig(stop={"training_iteration": 1}),
        ... ).fit()  # doctest: +ELLIPSIS
        [ignore]...
        >>> results.num_errors
        0
    """

    def __init__(self, algo_class=None):
        """Initializes a BCConfig instance."""
        super().__init__(algo_class=algo_class or BC)

        # fmt: off
        # __sphinx_doc_begin__
        # No need to calculate advantages (or do anything else with the rewards).
        self.beta = 0.0
        # Advantages (calculated during postprocessing)
        # not important for behavioral cloning.
        self.postprocess_inputs = False

        # Materialize only the mapped data. This is optimal as long
        # as no connector in the connector pipeline holds a state.
        self.materialize_data = False
        self.materialize_mapped_data = True
        # __sphinx_doc_end__
        # fmt: on

    @override(AlgorithmConfig)
    def get_default_rl_module_spec(self) -> RLModuleSpecType:
        if self.framework_str == "torch":
            from ray.rllib.algorithms.bc.torch.default_bc_torch_rl_module import (
                DefaultBCTorchRLModule,
            )

            return RLModuleSpec(module_class=DefaultBCTorchRLModule)
        else:
            raise ValueError(
                f"The framework {self.framework_str} is not supported. "
                "Use `torch` instead."
            )

    @override(AlgorithmConfig)
    def build_learner_connector(
        self,
        input_observation_space,
        input_action_space,
        device=None,
    ):
        pipeline = super().build_learner_connector(
            input_observation_space=input_observation_space,
            input_action_space=input_action_space,
            device=device,
        )

        # Remove unneeded connectors from the MARWIL connector pipeline.
        pipeline.remove("AddOneTsToEpisodesAndTruncate")
        pipeline.remove("GeneralAdvantageEstimation")

        return pipeline

    @override(MARWILConfig)
    def validate(self) -> None:
        # Call super's validation method.
        super().validate()

        if self.beta != 0.0:
            self._value_error("For behavioral cloning, `beta` parameter must be 0.0!")


class BC(MARWIL):
    """Behavioral Cloning (derived from MARWIL).

    Uses MARWIL with beta force-set to 0.0.
    """

    @classmethod
    @override(MARWIL)
    def get_default_config(cls) -> BCConfig:
        return BCConfig()
