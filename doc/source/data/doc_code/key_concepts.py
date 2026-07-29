# flake8: noqa

# fmt: off
# __resource_allocation_1_begin__
import ray
from ray import tune

# This workload will use spare cluster resources for execution.
def objective(*args):
    ray.data.range(10).show()

# Create a cluster with 4 CPU slots available.
ray.init(num_cpus=4)

# By setting `max_concurrent_trials=3`, this ensures the cluster will always
# have a sparse CPU for Dataset. Try setting `max_concurrent_trials=4` here,
# and notice that the experiment will appear to hang.
tuner = tune.Tuner(
    tune.with_resources(objective, {"cpu": 1}),
    tune_config=tune.TuneConfig(
        num_samples=1,
        max_concurrent_trials=3
    )
)
tuner.fit()
# __resource_allocation_1_end__
# fmt: on
