from functools import partial

from ray.rllib.connectors.common.add_infos_from_episodes_to_batch import (
    AddInfosFromEpisodesToBatch,
)


# Enable backward compatibility.
AddInfosFromEpisodesToTrainBatch = partial(
    AddInfosFromEpisodesToBatch,
    as_learner_connector=True,
)
