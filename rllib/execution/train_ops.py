import logging
import numpy as np
import math
from typing import Dict

from ray.rllib.execution.common import (
    LEARN_ON_BATCH_TIMER,
    LOAD_BATCH_TIMER,
)
from ray.rllib.policy.sample_batch import DEFAULT_POLICY_ID
from ray.rllib.utils.annotations import DeveloperAPI
from ray.rllib.utils.framework import try_import_tf
from ray.rllib.utils.deprecation import deprecation_warning
from ray.rllib.utils.metrics import (
    NUM_ENV_STEPS_TRAINED,
    NUM_AGENT_STEPS_TRAINED,
)
from ray.rllib.utils.metrics.learner_info import LearnerInfoBuilder
from ray.rllib.utils.sgd import do_minibatch_sgd
from ray.util import log_once

tf1, tf, tfv = try_import_tf()

logger = logging.getLogger(__name__)


@DeveloperAPI
def train_one_step(algorithm, train_batch, policies_to_train=None) -> Dict:
    """Function that improves the all policies in `train_batch` on the local worker.

    Examples:
        >>> from ray.rllib.execution.rollout_ops import synchronous_parallel_sample
        >>> algo = [...] # doctest: +SKIP
        >>> train_batch = synchronous_parallel_sample(algo.workers) # doctest: +SKIP
        >>> # This trains the policy on one batch.
        >>> results = train_one_step(algo, train_batch)) # doctest: +SKIP
        {"default_policy": ...}

    Updates the NUM_ENV_STEPS_TRAINED and NUM_AGENT_STEPS_TRAINED counters as well as
    the LEARN_ON_BATCH_TIMER timer of the `algorithm` object.
    """
    if log_once("train_one_step_deprecation_warning"):
        deprecation_warning(old="ray.rllib.execution.train_ops.train_one_step")

    config = algorithm.config
    workers = algorithm.workers
    local_worker = workers.local_worker()
    num_sgd_iter = config.get("num_sgd_iter", 1)
    sgd_minibatch_size = config.get("sgd_minibatch_size", 0)

    learn_timer = algorithm._timers[LEARN_ON_BATCH_TIMER]
    with learn_timer:
        # Subsample minibatches (size=`sgd_minibatch_size`) from the
        # train batch and loop through train batch `num_sgd_iter` times.
        if num_sgd_iter > 1 or sgd_minibatch_size > 0:
            info = do_minibatch_sgd(
                train_batch,
                {
                    pid: local_worker.get_policy(pid)
                    for pid in policies_to_train
                    or local_worker.get_policies_to_train(train_batch)
                },
                local_worker,
                num_sgd_iter,
                sgd_minibatch_size,
                [],
            )
        # Single update step using train batch.
        else:
            info = local_worker.learn_on_batch(train_batch)

    learn_timer.push_units_processed(train_batch.count)
    algorithm._counters[NUM_ENV_STEPS_TRAINED] += train_batch.count
    algorithm._counters[NUM_AGENT_STEPS_TRAINED] += train_batch.agent_steps()

    if algorithm.reward_estimators:
        info[DEFAULT_POLICY_ID]["off_policy_estimation"] = {}
        for name, estimator in algorithm.reward_estimators.items():
            info[DEFAULT_POLICY_ID]["off_policy_estimation"][name] = estimator.train(
                train_batch
            )
    return info


@DeveloperAPI
def multi_gpu_train_one_step(algorithm, train_batch) -> Dict:
    """Multi-GPU version of train_one_step.

    Uses the policies' `load_batch_into_buffer` and `learn_on_loaded_batch` methods
    to be more efficient wrt CPU/GPU data transfers. For example, when doing multiple
    passes through a train batch (e.g. for PPO) using `config.num_sgd_iter`, the
    actual train batch is only split once and loaded once into the GPU(s).

    Examples:
        >>> from ray.rllib.execution.rollout_ops import synchronous_parallel_sample
        >>> algo = [...] # doctest: +SKIP
        >>> train_batch = synchronous_parallel_sample(algo.workers) # doctest: +SKIP
        >>> # This trains the policy on one batch.
        >>> results = multi_gpu_train_one_step(algo, train_batch)) # doctest: +SKIP
        {"default_policy": ...}

    Updates the NUM_ENV_STEPS_TRAINED and NUM_AGENT_STEPS_TRAINED counters as well as
    the LOAD_BATCH_TIMER and LEARN_ON_BATCH_TIMER timers of the Algorithm instance.
    """
    if log_once("mulit_gpu_train_one_step_deprecation_warning"):
        deprecation_warning(
            old=("ray.rllib.execution.train_ops." "multi_gpu_train_one_step")
        )
    config = algorithm.config
    workers = algorithm.workers
    local_worker = workers.local_worker()
    num_sgd_iter = config.get("num_sgd_iter", 1)
    sgd_minibatch_size = config.get("sgd_minibatch_size", config["train_batch_size"])

    # Determine the number of devices (GPUs or 1 CPU) we use.
    num_devices = int(math.ceil(config["num_gpus"] or 1))

    # Make sure total batch size is dividable by the number of devices.
    # Batch size per tower.
    per_device_batch_size = sgd_minibatch_size // num_devices
    # Total batch size.
    batch_size = per_device_batch_size * num_devices
    assert batch_size % num_devices == 0
    assert batch_size >= num_devices, "Batch size too small!"

    # Handle everything as if multi-agent.
    train_batch = train_batch.as_multi_agent()

    # Load data into GPUs.
    load_timer = algorithm._timers[LOAD_BATCH_TIMER]
    with load_timer:
        num_loaded_samples = {}
        for policy_id, batch in train_batch.policy_batches.items():
            # Not a policy-to-train.
            if (
                local_worker.is_policy_to_train is not None
                and not local_worker.is_policy_to_train(policy_id, train_batch)
            ):
                continue

            # Decompress SampleBatch, in case some columns are compressed.
            batch.decompress_if_needed()

            # Load the entire train batch into the Policy's only buffer
            # (idx=0). Policies only have >1 buffers, if we are training
            # asynchronously.
            num_loaded_samples[policy_id] = local_worker.policy_map[
                policy_id
            ].load_batch_into_buffer(batch, buffer_index=0)

    # Execute minibatch SGD on loaded data.
    learn_timer = algorithm._timers[LEARN_ON_BATCH_TIMER]
    with learn_timer:
        # Use LearnerInfoBuilder as a unified way to build the final
        # results dict from `learn_on_loaded_batch` call(s).
        # This makes sure results dicts always have the same structure
        # no matter the setup (multi-GPU, multi-agent, minibatch SGD,
        # tf vs torch).
        learner_info_builder = LearnerInfoBuilder(num_devices=num_devices)

        for policy_id, samples_per_device in num_loaded_samples.items():
            policy = local_worker.policy_map[policy_id]
            num_batches = max(1, int(samples_per_device) // int(per_device_batch_size))
            logger.debug("== sgd epochs for {} ==".format(policy_id))
            for _ in range(num_sgd_iter):
                permutation = np.random.permutation(num_batches)
                for batch_index in range(num_batches):
                    # Learn on the pre-loaded data in the buffer.
                    # Note: For minibatch SGD, the data is an offset into
                    # the pre-loaded entire train batch.
                    results = policy.learn_on_loaded_batch(
                        permutation[batch_index] * per_device_batch_size, buffer_index=0
                    )

                    learner_info_builder.add_learn_on_batch_results(results, policy_id)

        # Tower reduce and finalize results.
        learner_info = learner_info_builder.finalize()

    load_timer.push_units_processed(train_batch.count)
    learn_timer.push_units_processed(train_batch.count)

    # TODO: Move this into Algorithm's `training_step` method for
    #  better transparency.
    algorithm._counters[NUM_ENV_STEPS_TRAINED] += train_batch.count
    algorithm._counters[NUM_AGENT_STEPS_TRAINED] += train_batch.agent_steps()

    if algorithm.reward_estimators:
        learner_info[DEFAULT_POLICY_ID]["off_policy_estimation"] = {}
        for name, estimator in algorithm.reward_estimators.items():
            learner_info[DEFAULT_POLICY_ID]["off_policy_estimation"][
                name
            ] = estimator.train(train_batch)

    return learner_info
