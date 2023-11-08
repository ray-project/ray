from typing import Any

import numpy as np
import tree

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
        if not episodes:
            return input_

        # Simple 1D data.
        for key in [
            SampleBatch.EPS_ID,
            SampleBatch.REWARDS,
            SampleBatch.TERMINATEDS,
            SampleBatch.TRUNCATEDS,
            SampleBatch.T,
        ]:
            if key not in input_:
                episode_data = []
                for episode in episodes:
                    episode_data.append(episode.get_data(key))
                # Concatenate everything together.
                input_[key] = np.concatenate(episode_data)

        # Possibly nested data.
        for key in [
            SampleBatch.OBS,
            SampleBatch.ACTIONS,
            *episodes[0].extra_model_outputs.keys()
        ]:
            if key not in input_:
                episode_data = []
                for episode in episodes:
                    episode_data.append(episode.get_data(key))
                # Concatenate everything together.
                input_[key] = tree.map_structure(
                    lambda *s: np.concatenate(s, axis=0),
                    *episode_data,
                )

        # Infos (always as lists).
        if SampleBatch.INFOS not in input_:
            episode_data = []
            for episode in episodes:
                episode_data.extend(episode.get_data(SampleBatch.INFOS))
            input_[SampleBatch.INFOS] = episode_data

        return input_