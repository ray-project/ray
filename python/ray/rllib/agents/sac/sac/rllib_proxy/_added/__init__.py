from ray.rllib.agents.sac.sac.dev_utils import using_ray_8
from ray.rllib.agents.sac.sac.rllib_proxy._added._utils import *

if using_ray_8():
    from ray.rllib.evaluation import worker_set
    WorkerSet = worker_set.WorkerSet
else:
    WorkerSet = None

# The following code was copied from ray v 0.8.1, so it's fine to include it for both
# versions
from ray.rllib.agents.sac.sac.rllib_proxy._added._envs import NormalizeActionWrapper
