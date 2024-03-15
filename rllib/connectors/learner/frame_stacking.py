from functools import partial

from ray.rllib.connectors.common.frame_stacking import _FrameStacking


FrameStackingLearner = partial(_FrameStacking, as_learner_connector=True)
