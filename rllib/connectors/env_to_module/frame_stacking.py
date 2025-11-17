from functools import partial

from ray.rllib.connectors.common.frame_stacking import FrameStacking

FrameStackingEnvToModule = partial(FrameStacking, as_learner_connector=False)
