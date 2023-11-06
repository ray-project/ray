from typing import Any

import numpy as np

from ray.rllib.connectors.connector_v2 import ConnectorV2
from ray.rllib.connectors.connector_context_v2 import ConnectorContextV2
from ray.rllib.policy.sample_batch import SampleBatch


class DefaultLearnerConnector(ConnectorV2):
    """Connector added by default by RLlib to the end of the learner connector pipeline.

    Makes sure that the final train batch going into the RLModule for updating
    (`forward_train()` call) contains at least:
    - Observations under the SampleBatch.OBS key.
    - Actions, rewards, terminal/truncation flags under the respective keys.
    - All data inside the episodes' `extra_model_outs` property, e.g. value function
      predictions, advantages, etc..
    - States:
    TODO (sven): Figure out how to handle states automatically in here.
      Should we only provide one state per max_seq_len (if yes, these would have to be
      stored in the episodes)?
    """
    def __call__(self, input_: Any, episodes, ctx: ConnectorContextV2, **kwargs):
        # If episodes are provided, extract the essential data from them, but only if
        # this data is not present yet in `input_`.
        if episodes:
            for essential_key in [
                SampleBatch.EPS_ID,
                SampleBatch.OBS,
                SampleBatch.INFOS,
                SampleBatch.ACTIONS,
                SampleBatch.REWARDS,
                SampleBatch.TERMINATEDS,
                SampleBatch.TRUNCATEDS,
                SampleBatch.T,
                *episodes[0].extra_model_outputs.keys()
            ]:
                if essential_key not in input_:
                    episode_data = []
                    for episode in episodes:
                        episode_data.append(episode.get_data(essential_key))
                    # Concatenate everything together.
                    input_[essential_key] = np.concatenate(episode_data, axis=0)

        return input_