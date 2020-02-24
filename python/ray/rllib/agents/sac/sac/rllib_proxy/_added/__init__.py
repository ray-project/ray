from sac.dev_utils import using_ray_8
from sac.rllib_proxy._added._utils import *

if using_ray_8():
    from ray.rllib.evaluation import worker_set
    WorkerSet = worker_set.WorkerSet
    from ray.rllib.env import normalize_actions
    NormalizeActionWrapper = normalize_actions.NormalizeActionWrapper
else:
    WorkerSet = None
    NormalizeActionWrapper = None
