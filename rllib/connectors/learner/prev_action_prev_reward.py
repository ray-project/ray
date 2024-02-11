from functools import partial

from ray.rllib.connectors.env_to_module.prev_action_prev_reward import (
    _PrevRewardPrevActionConnector,
)


PrevRewardPrevActionLearner = partial(
    _PrevRewardPrevActionConnector, as_learner_connector=True
)
