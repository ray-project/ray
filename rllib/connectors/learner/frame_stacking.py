from functools import partial

from ray.rllib.connectors.common.frame_stacking import _FrameStackingConnector


FrameStackingLearner = partial(_FrameStackingConnector, as_learner_connector=True)
