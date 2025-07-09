import abc

from typing import Any, Dict, List, Tuple, Union

from ray.rllib.algorithms.algorithm_config import AlgorithmConfig
from ray.rllib.core import DEFAULT_MODULE_ID
from ray.rllib.core.learner.learner_group import LearnerGroup
from ray.rllib.core.learner.training_data import TrainingData
from ray.rllib.core.rl_module.multi_rl_module import MultiRLModuleSpec
from ray.rllib.env import INPUT_ENV_SPACES
from ray.rllib.utils.typing import ParamDict, ResultDict


class LearnerGroupAPI(abc.ABC):
    """Base API for learning functionalities.

    This base api defines a `LearnerGroup` and multiple related
    methods to update `RLModule`s.

    Note, only concrete apis can be inherited from. Furthermore,
    the "apis-to-the-left"-rule must be applied, i.e. any api
    needs to be left of `RLAlgorithm`. Finally, any derived method
    should always call `super()`.
    """

    # A learner group for managing the `RLModule`. Note, this
    # group could be customized.
    _learner_group: LearnerGroup = None

    def __init__(self, config: AlgorithmConfig, **kwargs):
        """Initializes a LearnerGroupAPI."""
        # Call the super's method.
        super().__init__(config=config, **kwargs)

    abc.abstractmethod

    def _setup(self, config: AlgorithmConfig):
        """Abstract method to setup the specific `LearnerGroup`(s)."""

    abc.abstractmethod

    def update(
        self, training_data: Union[Dict[str, Any], TrainingData], **kwargs
    ) -> Tuple[ParamDict, Union[ResultDict, List[ResultDict]]]:
        """Updates the RLModule(s)."""
        pass

    abc.abstractmethod

    def _provide_sync_state(self, state: Any, **kwargs):
        """Abstract method to receive a state from the Learner(s)."""
        return state

    abc.abstractmethod

    def cleanup(self):
        """Pulls down the `LearnerGroup`(s)."""
        self._learner_group = None

    def get_multi_rl_module_spec(self, config: AlgorithmConfig) -> MultiRLModuleSpec:
        """Defines the MultiRLModuleSpec to be used in the Learner(s)."""
        spaces = {
            INPUT_ENV_SPACES: (
                # TODO (sven, simon): This needs the observation and action
                #   spaces to be defined in the config. Otherwise spaces could
                #   be defined centrally in the config or base algorithm.
                config.observation_space,
                config.action_space,
            )
        }

        spaces.update(
            {
                # TODO (sven, simon): This does not include any preprocessing
                #   like in EnvToModule.
                DEFAULT_MODULE_ID: (
                    config.observation_space,
                    config.action_space,
                ),
            }
        )

        return config.get_multi_rl_module_spec(
            spaces=spaces,
            inference_only=False,
        )


class SimpleLearnerGroupAPI(LearnerGroupAPI):
    """Concrete API for learning functionalities."""

    def __init__(self, config: AlgorithmConfig, **kwargs):
        # Important here to call super.
        super().__init__(config=config, **kwargs)

    def _setup(self, config: AlgorithmConfig):
        self.logger.info(f"Setup SimpleLearnerGroupAPI ...")
        module_spec = self.get_multi_rl_module_spec(config=config)
        self._learner_group = config.build_learner_group(rl_module_spec=module_spec)
        # `super`` must be called here, to ensure all other mixins
        # following in MRO are set up.
        super()._setup(config=config)

    def update(
        self, training_data: TrainingData, **kwargs
    ) -> Tuple[ParamDict, Union[ResultDict, List[ResultDict]]]:
        # Call update via the `LearnerGroup`.
        # TODO (simon): Maybe deprecate with v1 also any other data
        #   but `TrainingData`.
        return self._learner_group.update(
            training_data=training_data,
            timesteps=None,
            **kwargs,
        )

    def _provide_sync_state(self, state: Any, **kwargs):
        # Note, state must have an `update` method defined, too.
        state.update(self._learner_group.get_state())

        # Call super's update such that all apis can contribute to
        # the state.
        return super()._provide_sync_state(state, **kwargs)
