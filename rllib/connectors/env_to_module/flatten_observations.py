from functools import partial

from ray.rllib.connectors.common.flatten_observations import FlattenObservations

FrameStackingEnvToModule = partial(FlattenObservations, as_learner_connector=False)
