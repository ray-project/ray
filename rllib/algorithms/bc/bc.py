from ray.rllib.algorithms.algorithm_config import AlgorithmConfig
from ray.rllib.algorithms.marwil.marwil import MARWIL, MARWILConfig
from ray.rllib.core.rl_module.rl_module import RLModuleSpec
from ray.rllib.utils.annotations import override
from ray.rllib.utils.typing import RLModuleSpecType


class BCConfig(MARWILConfig):
    """Defines a configuration class from which a new BC Algorithm can be built

    .. testcode::
        :skipif: True

        from ray.rllib.algorithms.bc import BCConfig
        # Run this from the ray directory root.
        config = BCConfig().training(lr=0.00001, gamma=0.99)
        config = config.offline_data(
            input_="./rllib/tests/data/cartpole/large.json")

        # Build an Algorithm object from the config and run 1 training iteration.
        algo = config.build()
        algo.train()

    .. testcode::
        :skipif: True

        from ray.rllib.algorithms.bc import BCConfig
        from ray import tune
        config = BCConfig()
        # Print out some default values.
        print(config.beta)
        # Update the config object.
        config.training(
            lr=tune.grid_search([0.001, 0.0001]), beta=0.75
        )
        # Set the config object's data path.
        # Run this from the ray directory root.
        config.offline_data(
            input_="./rllib/tests/data/cartpole/large.json"
        )
        # Set the config object's env, used for evaluation.
        config.environment(env="CartPole-v1")
        # Use to_dict() to get the old-style python config dict
        # when running with tune.
        tune.Tuner(
            "BC",
            param_space=config.to_dict(),
        ).fit()
    """

    def __init__(self, algo_class=None):
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
    def get_default_config(cls) -> AlgorithmConfig:
        return BCConfig()
