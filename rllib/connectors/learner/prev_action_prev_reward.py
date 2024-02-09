from functools import partial

from ray.rllib.connectors.common.prev_action_prev_reward import (
    _PrevRewardPrevActionConnector,
)


PrevRewardPrevActionLearner = partial(
    _PrevRewardPrevActionConnector, as_learner_connector=True
)
