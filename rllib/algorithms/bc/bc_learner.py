from ray.rllib.core.learner.learner import Learner
from ray.rllib.connectors.common.add_observations_from_episodes_to_batch import (
    AddObservationsFromEpisodesToBatch,
)
from ray.rllib.connectors.learner.add_next_observations_from_episodes_to_train_batch import (  # noqa
    AddNextObservationsFromEpisodesToTrainBatch,
)
from ray.rllib.utils.annotations import (
    override,
    OverrideToImplementCustomLogic_CallToSuperRecommended,
)


class BCLearner(Learner):
    @OverrideToImplementCustomLogic_CallToSuperRecommended
    @override(Learner)
    def build(self) -> None:
        super().build()
        # Prepend a NEXT_OBS from episodes to train batch connector piece (right
        # after the observation default piece).
        if (
            self.config.add_default_connectors_to_learner_pipeline
            and self.config.enable_env_runner_and_connector_v2
        ):
            self._learner_connector.insert_after(
                AddObservationsFromEpisodesToBatch,
                AddNextObservationsFromEpisodesToTrainBatch(),
            )
