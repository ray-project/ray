from functools import partial

from ray.rllib.connectors.common.frame_stacking import _FrameStacking


FrameStackingEnvToModule = partial(_FrameStacking, as_learner_connector=False)
