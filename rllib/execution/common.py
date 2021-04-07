from ray.util.iter import LocalIterator
from ray.rllib.policy.sample_batch import SampleBatch, MultiAgentBatch
from ray.rllib.utils.typing import Dict, SampleBatchType
from ray.util.iter_metrics import MetricsContext

# Counters for training progress (keys for metrics.counters).
STEPS_SAMPLED_COUNTER = "num_steps_sampled"
AGENT_STEPS_SAMPLED_COUNTER = "num_agent_steps_sampled"
STEPS_TRAINED_COUNTER = "num_steps_trained"
AGENT_STEPS_TRAINED_COUNTER = "num_agent_steps_trained"

# Counters to track target network updates.
LAST_TARGET_UPDATE_TS = "last_target_update_ts"
NUM_TARGET_UPDATES = "num_target_updates"

# Performance timers (keys for metrics.timers).
APPLY_GRADS_TIMER = "apply_grad"
COMPUTE_GRADS_TIMER = "compute_grads"
WORKER_UPDATE_TIMER = "update"
GRAD_WAIT_TIMER = "grad_wait"
SAMPLE_TIMER = "sample"
LEARN_ON_BATCH_TIMER = "learn"
LOAD_BATCH_TIMER = "load"

# Instant metrics (keys for metrics.info).
LEARNER_INFO = "learner"


# Asserts that an object is a type of SampleBatch.
def _check_sample_batch_type(batch: SampleBatchType) -> None:
    if not isinstance(batch, (SampleBatch, MultiAgentBatch)):
        raise ValueError("Expected either SampleBatch or MultiAgentBatch, "
                         "got {}: {}".format(type(batch), batch))


# Returns pipeline global vars that should be periodically sent to each worker.
def _get_global_vars() -> Dict:
    metrics = LocalIterator.get_metrics()
    return {"timestep": metrics.counters[STEPS_SAMPLED_COUNTER]}


def _get_shared_metrics() -> MetricsContext:
    """Return shared metrics for the training workflow.

    This only applies if this trainer has an execution plan."""
    return LocalIterator.get_metrics()
