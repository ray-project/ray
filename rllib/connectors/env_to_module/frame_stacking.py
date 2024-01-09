from functools import partial

from ray.rllib.connectors.common.frame_stacking import _FrameStackingConnector


FrameStackingEnvToModule = partial(_FrameStackingConnector, as_learner_connector=False)
