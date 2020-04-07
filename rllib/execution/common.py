import logging
from typing import List, Any, Tuple, Union
import numpy as np
import queue
import random
import time

import ray
from ray.util.iter import from_actors, LocalIterator, _NextValueNotReady
from ray.util.iter_metrics import SharedMetrics
from ray.rllib.optimizers.replay_buffer import PrioritizedReplayBuffer, \
    ReplayBuffer
from ray.rllib.evaluation.metrics import collect_episodes, \
    summarize_episodes, get_learner_stats
from ray.rllib.evaluation.rollout_worker import get_global_worker
from ray.rllib.policy.sample_batch import SampleBatch, MultiAgentBatch, \
    DEFAULT_POLICY_ID
from ray.rllib.utils.compression import pack_if_needed

# Counters for training progress (keys for metrics.counters).
STEPS_SAMPLED_COUNTER = "num_steps_sampled"
STEPS_TRAINED_COUNTER = "num_steps_trained"

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

# Instant metrics (keys for metrics.info).
LEARNER_INFO = "learner"

# Type aliases.
GradientType = dict
SampleBatchType = Union[SampleBatch, MultiAgentBatch]


# Asserts that an object is a type of SampleBatch.
def _check_sample_batch_type(batch):
    if not isinstance(batch, SampleBatchType.__args__):
        raise ValueError("Expected either SampleBatch or MultiAgentBatch, "
                         "got {}: {}".format(type(batch), batch))


# Returns pipeline global vars that should be periodically sent to each worker.
def _get_global_vars():
    metrics = LocalIterator.get_metrics()
    return {"timestep": metrics.counters[STEPS_SAMPLED_COUNTER]}
