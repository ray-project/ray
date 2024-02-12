from functools import partial

from ray.rllib.connectors.common.prev_action_prev_reward import (
    _PrevRewardPrevActionConnector
)


PrevRewardPrevActionEnvToModule = partial(
    _PrevRewardPrevActionConnector, as_learner_connector=False
)
