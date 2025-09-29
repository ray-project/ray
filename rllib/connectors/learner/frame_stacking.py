from functools import partial

from ray.rllib.connectors.common.frame_stacking import FrameStacking


FrameStackingLearner = partial(FrameStacking, as_learner_connector=True)
