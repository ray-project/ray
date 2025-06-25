import abc

from typing import Any, Dict, List, Tuple, Union

from ray.rllib.algorithms.algorithm_config import AlgorithmConfig
from ray.rllib.core import DEFAULT_MODULE_ID
from ray.rllib.core.learner.learner_group import LearnerGroup
from ray.rllib.core.learner.training_data import TrainingData
from ray.rllib.core.rl_module.multi_rl_module import MultiRLModuleSpec
from ray.rllib.env import INPUT_ENV_SPACES
from ray.rllib.utils.typing import ParamDict, ResultDict


class LearnerMixin(abc.ABC):

    _learner_group: LearnerGroup = None

    def __init__(self, config: AlgorithmConfig, **kwargs):
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

    def _provide_sync_state(self, state, **kwargs):
        return super()._provide_sync_state(state, **kwargs)

    abc.abstractmethod

    def cleanup(self):
        """Pulls down the `LearnerGroup`(s)."""
        self._learner_group = None

    def get_multi_rl_module_spec(self, config: AlgorithmConfig) -> MultiRLModuleSpec:
        spaces = {
            INPUT_ENV_SPACES: (
                config.observation_space,
                config.action_space,
            )
        }

        spaces.update(
            {
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


class LearnerConcreteMixin(LearnerMixin):
    def __init__(self, config: AlgorithmConfig, **kwargs):
        super().__init__(config=config, **kwargs)

    def _setup(self, config: AlgorithmConfig):
        print("Setup LearnerConcreteMixin ...")
        module_spec = self.get_multi_rl_module_spec(config=config)
        self._learner_group = config.build_learner_group(rl_module_spec=module_spec)
        super()._setup(config=config)

    def update(
        self, training_data: TrainingData, **kwargs
    ) -> Tuple[ParamDict, Union[ResultDict, List[ResultDict]]]:

        return self._learner_group.update(
            training_data=training_data,
            timesteps=None,
            **kwargs,
        )

    def _provide_sync_state(self, state: Any, **kwargs):

        state.update(self._learner_group.get_state())

        return super()._provide_sync_state(state, **kwargs)
